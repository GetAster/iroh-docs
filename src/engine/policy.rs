//! A per-peer sync policy, consulted before each reconciliation exchange.
//!
//! The namespace id is the read capability in iroh-docs: a peer that does not
//! know it cannot ask for it. An application that hands namespace ids around as
//! identifiers can install a policy to decide, per peer, which namespaces it
//! syncs. Without one, every peer that names a namespace this node syncs is
//! accepted, as before.

use std::sync::{Arc, RwLock};

use iroh::PublicKey;

use crate::NamespaceId;

/// Which side of a sync exchange a policy decision is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SyncDirection {
    /// The peer opened the exchange and named the namespace.
    Accept,
    /// This node is about to dial the peer to sync the namespace.
    Connect,
}

/// A sync policy: `true` lets the exchange proceed.
///
/// It runs on the live actor's loop for every accept and every dial, so it must
/// answer from memory without blocking.
pub type SyncPolicyFn = dyn Fn(NamespaceId, PublicKey, SyncDirection) -> bool + Send + Sync;

/// Shared slot for the installed policy, read by the live actor.
#[derive(Clone, Default)]
pub(crate) struct SyncPolicy(Arc<RwLock<Option<Arc<SyncPolicyFn>>>>);

impl std::fmt::Debug for SyncPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let installed = self.0.read().map(|slot| slot.is_some()).unwrap_or(false);
        f.debug_struct("SyncPolicy")
            .field("installed", &installed)
            .finish()
    }
}

impl SyncPolicy {
    pub(crate) fn set(&self, policy: Option<Arc<SyncPolicyFn>>) {
        *self
            .0
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = policy;
    }

    /// Whether the exchange may proceed. A panicking policy poisons nothing:
    /// the slot is only written by [`SyncPolicy::set`].
    pub(crate) fn allows(
        &self,
        namespace: NamespaceId,
        peer: PublicKey,
        direction: SyncDirection,
    ) -> bool {
        let policy = self
            .0
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        match policy {
            None => true,
            Some(policy) => policy(namespace, peer, direction),
        }
    }
}
