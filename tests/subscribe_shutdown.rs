//! A document subscription ends when its node shuts down.
//!
//! Consumers fail closed on end-of-stream: a stream that stays open after the
//! node is gone leaves them serving an answer nobody maintains.

use std::time::Duration;

use iroh_docs::store::Query;
use n0_error::Result;
use n0_future::StreamExt;

mod util;

const END_DEADLINE: Duration = Duration::from_secs(5);

async fn assert_ends_after_shutdown(query_during_shutdown: bool) -> Result<()> {
    let node = util::Node::memory(util::empty_endpoint().await?)
        .spawn()
        .await
        .unwrap();
    let author = node.docs().author_create().await?;
    let doc = node.docs().create().await?;
    let mut events = doc.subscribe().await?;

    for i in 0..8 {
        doc.set_bytes(author, format!("k-{i}"), format!("v-{i}"))
            .await?;
        // Reads between writes, as a consumer refreshing on each event does.
        let _ = doc.get_many(Query::all()).await?.collect::<Vec<_>>().await;
        let _ = doc.get_one(Query::key_exact(format!("k-{i}"))).await?;
    }

    let reader = query_during_shutdown.then(|| {
        let doc = doc.clone();
        tokio::spawn(async move {
            loop {
                // Errors arrive either from the call or as a stream item.
                let Ok(stream) = doc.get_many(Query::all()).await else {
                    break;
                };
                let items = stream.collect::<Vec<_>>().await;
                if items.iter().any(|item| item.is_err()) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
    });

    node.shutdown().await?;

    let ended = tokio::time::timeout(END_DEADLINE, async {
        while events.next().await.is_some() {}
    })
    .await;
    assert!(
        ended.is_ok(),
        "the subscription is still open {END_DEADLINE:?} after the node shut down"
    );
    if let Some(reader) = reader {
        tokio::time::timeout(END_DEADLINE, reader)
            .await
            .expect("reads fail once the node is gone")
            .unwrap();
    }
    Ok(())
}

#[tokio::test]
async fn a_subscription_ends_when_the_node_shuts_down() -> Result<()> {
    assert_ends_after_shutdown(false).await
}

#[tokio::test]
async fn a_subscription_ends_when_the_node_shuts_down_under_reads() -> Result<()> {
    assert_ends_after_shutdown(true).await
}
