//! Inactivity limit for the streams of a sync exchange.

use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use n0_future::time::{sleep, Duration, Instant, Sleep};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// The error a stream fails with when it made no progress for the limit.
#[derive(Debug)]
struct IdleTimedOut(Duration);

impl fmt::Display for IdleTimedOut {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "no progress for {:?}", self.0)
    }
}

impl std::error::Error for IdleTimedOut {}

/// The limit, if `error` is (or wraps) an [`IdleTimeout`] expiry.
pub(super) fn is_idle_timeout(error: &anyhow::Error) -> Option<Duration> {
    error.chain().find_map(|cause| {
        cause
            .downcast_ref::<io::Error>()
            .and_then(io::Error::get_ref)
            .and_then(|inner| inner.downcast_ref::<IdleTimedOut>())
            .map(|timed_out| timed_out.0)
    })
}

/// Fails reads and writes that stay pending for `limit` without making progress.
///
/// The clock runs only while an operation is pending and restarts on every poll that completes,
/// so a transfer that keeps moving is never cut off, however long it takes, while a peer that
/// stops answering is detected after `limit`. Time the owner spends between operations (for
/// example in the local replica actor) does not count.
pub(super) struct IdleTimeout<S> {
    inner: S,
    limit: Duration,
    timer: Pin<Box<Sleep>>,
    /// Whether `timer` holds the deadline of the operation currently pending.
    armed: bool,
}

impl<S> IdleTimeout<S> {
    pub(super) fn new(inner: S, limit: Duration) -> Self {
        Self {
            inner,
            limit,
            timer: Box::pin(sleep(limit)),
            armed: false,
        }
    }

    fn guard<T>(&mut self, cx: &mut Context<'_>, poll: Poll<io::Result<T>>) -> Poll<io::Result<T>> {
        if poll.is_ready() {
            self.armed = false;
            return poll;
        }
        if !self.armed {
            self.timer.as_mut().reset(Instant::now() + self.limit);
            self.armed = true;
        }
        match self.timer.as_mut().poll(cx) {
            Poll::Ready(()) => {
                self.armed = false;
                Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    IdleTimedOut(self.limit),
                )))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for IdleTimeout<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_read(cx, buf);
        this.guard(cx, poll)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for IdleTimeout<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_write(cx, buf);
        this.guard(cx, poll)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_flush(cx);
        this.guard(cx, poll)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let poll = Pin::new(&mut this.inner).poll_shutdown(cx);
        this.guard(cx, poll)
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

    use super::*;

    const LIMIT: Duration = Duration::from_secs(10);

    #[tokio::test(start_paused = true)]
    async fn silent_read_fails_after_the_limit() {
        let (local, _remote) = duplex(64);
        let mut local = IdleTimeout::new(local, LIMIT);
        let started = Instant::now();
        let err = local.read_u8().await.unwrap_err();
        assert_eq!(started.elapsed(), LIMIT);
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert_eq!(is_idle_timeout(&err.into()), Some(LIMIT));
    }

    #[tokio::test(start_paused = true)]
    async fn blocked_write_fails_after_the_limit() {
        // Nothing reads the far end, so the second write fills the pipe and stays pending.
        let (local, _remote) = duplex(4);
        let mut local = IdleTimeout::new(local, LIMIT);
        local.write_all(&[0; 4]).await.unwrap();
        let err = local.write_all(&[0; 4]).await.unwrap_err();
        assert_eq!(is_idle_timeout(&err.into()), Some(LIMIT));
    }

    #[tokio::test(start_paused = true)]
    async fn a_transfer_that_keeps_moving_outlives_the_limit() {
        let (local, mut remote) = duplex(64);
        let mut local = IdleTimeout::new(local, LIMIT);
        let started = Instant::now();
        let sender = tokio::spawn(async move {
            // Every gap is shorter than the limit; the whole takes five times as long.
            for byte in 0..5u8 {
                tokio::time::sleep(LIMIT - Duration::from_secs(1)).await;
                remote.write_u8(byte).await.unwrap();
            }
        });
        for expected in 0..5u8 {
            assert_eq!(local.read_u8().await.unwrap(), expected);
        }
        sender.await.unwrap();
        assert!(started.elapsed() > LIMIT);
    }

    #[tokio::test(start_paused = true)]
    async fn time_between_operations_does_not_count() {
        let (local, mut remote) = duplex(64);
        let mut local = IdleTimeout::new(local, LIMIT);
        remote.write_u8(1).await.unwrap();
        assert_eq!(local.read_u8().await.unwrap(), 1);
        // The owner is busy (for example waiting on the local replica) for longer than the limit.
        tokio::time::sleep(LIMIT * 3).await;
        remote.write_u8(2).await.unwrap();
        assert_eq!(local.read_u8().await.unwrap(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn other_errors_are_not_timeouts() {
        let err = anyhow::Error::from(io::Error::from(io::ErrorKind::TimedOut));
        assert_eq!(is_idle_timeout(&err), None);
    }
}
