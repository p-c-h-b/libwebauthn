//! Caller-driven teardown of hybrid connections, and the registry that tracks
//! connections left lingering for a late linking update.
//!
//! After a QR-initiated ceremony the authenticator may send its linking
//! information a while after the CTAP response. Capturing it needs the
//! connection to stay open after the caller is done with the channel. A
//! caller opts in by carrying a [`CableLingerConfig`] on its
//! [`ChannelSettings`](crate::transport::ChannelSettings) and calling
//! [`CableChannel::linger`](super::channel::CableChannel::linger) once the
//! ceremony has completed. The same [`CableLingerRegistry`] instance must be
//! threaded through every hybrid `channel()` call of one logical client: a
//! new connection evicts any connection still lingering, and the registry is
//! the only handle left once the channel has been dropped.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use tokio::sync::watch;

/// Default linger window. The spec asks for at least two minutes after Shutdown.
pub const DEFAULT_LINGER: Duration = Duration::from_secs(120);
/// Absolute ceiling on a linger, whatever the configured window. Matches Chromium.
pub const HARD_CAP: Duration = Duration::from_secs(180);
/// Concurrent detached lingerers per registry. The oldest is evicted on overflow.
pub(crate) const MAX_LINGERING: usize = 8;

/// Caller to task teardown intent. One watch per connection, distinct from
/// [`ConnectionState`](super::channel::ConnectionState), which is the task to
/// caller phase. Cancel always wins and no intent is ever downgraded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Teardown {
    /// Running normally: connecting, or in the active loop.
    Active,
    /// Graceful: send Shutdown, then terminate.
    Close,
    /// Graceful: send Shutdown, then linger for a late linking update if eligible.
    Linger,
    /// Hard: no Shutdown, terminate now.
    Cancel,
}

/// Opt-in to lingering, carried on
/// [`ChannelSettings::cable_linger`](crate::transport::ChannelSettings::cable_linger).
#[derive(Debug, Clone)]
pub struct CableLingerConfig {
    /// Tracks lingering connections across ceremonies. Share one instance per client.
    pub registry: CableLingerRegistry,
    /// How long to keep receiving after Shutdown. Clamped to [`HARD_CAP`].
    pub linger_duration: Duration,
}

impl CableLingerConfig {
    pub fn new(registry: CableLingerRegistry) -> Self {
        Self {
            registry,
            linger_duration: DEFAULT_LINGER,
        }
    }
}

#[derive(Default)]
struct RegistryInner {
    next_id: u64,
    entries: BTreeMap<u64, Arc<watch::Sender<Teardown>>>,
}

impl RegistryInner {
    fn is_lingering(tx: &watch::Sender<Teardown>) -> bool {
        *tx.borrow() == Teardown::Linger
    }
}

impl Drop for RegistryInner {
    fn drop(&mut self) {
        for tx in self.entries.values() {
            tx.send_replace(Teardown::Cancel);
        }
    }
}

/// Tracks hybrid connections from creation so that a lingering one can be
/// evicted after its channel is gone. Cheap to clone. The caller holds the
/// only strong reference: dropping the last clone cancels every lingerer.
#[derive(Clone, Default)]
pub struct CableLingerRegistry {
    inner: Arc<Mutex<RegistryInner>>,
}

impl CableLingerRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, RegistryInner> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Connections currently lingering. Eventually consistent with the
    /// natural expiry of a linger window.
    pub fn lingering_count(&self) -> usize {
        self.lock()
            .entries
            .values()
            .filter(|tx| RegistryInner::is_lingering(tx))
            .count()
    }

    /// Cancels every lingering connection. Returns how many were signalled.
    /// Connections still connecting or in use are left alone.
    pub fn close_lingering(&self) -> usize {
        let inner = self.lock();
        let lingering: Vec<_> = inner
            .entries
            .values()
            .filter(|tx| RegistryInner::is_lingering(tx))
            .collect();
        for tx in &lingering {
            tx.send_replace(Teardown::Cancel);
        }
        lingering.len()
    }

    /// Registers a connection at creation time. Over [`MAX_LINGERING`], the
    /// oldest lingering connection is cancelled to make room.
    pub(crate) fn register(&self, tx: Arc<watch::Sender<Teardown>>) -> RegistryGuard {
        let mut inner = self.lock();
        if inner.entries.len() >= MAX_LINGERING {
            let oldest = inner
                .entries
                .iter()
                .find(|(_, tx)| RegistryInner::is_lingering(tx))
                .map(|(id, _)| *id);
            if let Some(id) = oldest {
                if let Some(evicted) = inner.entries.remove(&id) {
                    evicted.send_replace(Teardown::Cancel);
                }
            }
        }
        let id = inner.next_id;
        inner.next_id += 1;
        inner.entries.insert(id, tx);
        RegistryGuard {
            inner: Arc::downgrade(&self.inner),
            id,
        }
    }
}

impl fmt::Debug for CableLingerRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CableLingerRegistry")
            .field("lingering_count", &self.lingering_count())
            .finish()
    }
}

/// Removes the connection from its registry when the connection task ends,
/// however it ends. Holds a weak reference so it never keeps the registry alive.
pub(crate) struct RegistryGuard {
    inner: Weak<Mutex<RegistryInner>>,
    id: u64,
}

impl Drop for RegistryGuard {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.upgrade() {
            inner
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .entries
                .remove(&self.id);
        }
    }
}

/// What the connection task needs to linger. Present only for connections
/// that are eligible and tracked by a registry.
pub(crate) struct LingerParams {
    pub linger_duration: Duration,
    #[allow(dead_code)]
    pub guard: RegistryGuard,
}

impl LingerParams {
    /// Builds the linger parameters for a connection, registering it. `None`
    /// when the caller did not opt in or the connection is not eligible.
    pub(crate) fn new(
        config: Option<&CableLingerConfig>,
        eligible: bool,
        tx: &Arc<watch::Sender<Teardown>>,
    ) -> Option<Self> {
        let config = config?;
        if !eligible {
            return None;
        }
        Some(Self {
            linger_duration: config.linger_duration.min(HARD_CAP),
            guard: config.registry.register(tx.clone()),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(registry: &CableLingerRegistry) -> (Arc<watch::Sender<Teardown>>, RegistryGuard) {
        let (tx, _rx) = watch::channel(Teardown::Active);
        let tx = Arc::new(tx);
        let guard = registry.register(tx.clone());
        (tx, guard)
    }

    #[test]
    fn close_lingering_cancels_only_lingerers() {
        let registry = CableLingerRegistry::new();
        let (lingering, _g1) = entry(&registry);
        let (active, _g2) = entry(&registry);
        lingering.send_replace(Teardown::Linger);

        assert_eq!(registry.lingering_count(), 1);
        assert_eq!(registry.close_lingering(), 1);
        assert_eq!(*lingering.borrow(), Teardown::Cancel);
        assert_eq!(*active.borrow(), Teardown::Active);
        assert_eq!(registry.lingering_count(), 0);
    }

    #[test]
    fn guard_drop_deregisters() {
        let registry = CableLingerRegistry::new();
        let (tx, guard) = entry(&registry);
        tx.send_replace(Teardown::Linger);
        assert_eq!(registry.lingering_count(), 1);
        drop(guard);
        assert_eq!(registry.lingering_count(), 0);
        assert_eq!(registry.close_lingering(), 0);
    }

    #[test]
    fn overflow_evicts_the_oldest_lingerer() {
        let registry = CableLingerRegistry::new();
        let mut entries = Vec::new();
        for _ in 0..MAX_LINGERING {
            let (tx, guard) = entry(&registry);
            tx.send_replace(Teardown::Linger);
            entries.push((tx, guard));
        }
        let (newest, _guard) = entry(&registry);

        assert_eq!(*entries[0].0.borrow(), Teardown::Cancel);
        assert_eq!(*entries[1].0.borrow(), Teardown::Linger);
        assert_eq!(*newest.borrow(), Teardown::Active);
        assert_eq!(registry.lingering_count(), MAX_LINGERING - 1);
    }

    #[test]
    fn overflow_never_evicts_an_active_connection() {
        let registry = CableLingerRegistry::new();
        let mut entries = Vec::new();
        for _ in 0..MAX_LINGERING {
            entries.push(entry(&registry));
        }
        let _newest = entry(&registry);
        assert!(entries
            .iter()
            .all(|(tx, _)| *tx.borrow() == Teardown::Active));
    }

    #[test]
    fn dropping_the_registry_cancels_everything() {
        let registry = CableLingerRegistry::new();
        let (lingering, _g1) = entry(&registry);
        let (active, _g2) = entry(&registry);
        lingering.send_replace(Teardown::Linger);

        let clone = registry.clone();
        drop(registry);
        assert_eq!(
            *lingering.borrow(),
            Teardown::Linger,
            "a clone keeps it alive"
        );
        drop(clone);
        assert_eq!(*lingering.borrow(), Teardown::Cancel);
        assert_eq!(*active.borrow(), Teardown::Cancel);
    }

    #[test]
    fn linger_params_require_opt_in_and_eligibility() {
        let registry = CableLingerRegistry::new();
        let config = CableLingerConfig::new(registry.clone());
        let (tx, _rx) = watch::channel(Teardown::Active);
        let tx = Arc::new(tx);

        assert!(LingerParams::new(None, true, &tx).is_none());
        assert!(LingerParams::new(Some(&config), false, &tx).is_none());
        let params = LingerParams::new(Some(&config), true, &tx).expect("eligible");
        assert_eq!(params.linger_duration, DEFAULT_LINGER);
        tx.send_replace(Teardown::Linger);
        assert_eq!(registry.lingering_count(), 1);
    }

    #[test]
    fn linger_duration_is_clamped_to_the_hard_cap() {
        let mut config = CableLingerConfig::new(CableLingerRegistry::new());
        config.linger_duration = HARD_CAP * 2;
        let (tx, _rx) = watch::channel(Teardown::Active);
        let params = LingerParams::new(Some(&config), true, &Arc::new(tx)).expect("eligible");
        assert_eq!(params.linger_duration, HARD_CAP);
    }
}
