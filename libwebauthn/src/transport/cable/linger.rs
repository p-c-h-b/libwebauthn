//! Caller-driven teardown of hybrid connections.

/// Caller to task teardown intent. One watch per connection, distinct from
/// [`ConnectionState`](super::channel::ConnectionState), which is the task to
/// caller phase. Cancel always wins and no intent is ever downgraded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Teardown {
    /// Running normally: connecting, or in the active loop.
    Active,
    /// Graceful: send Shutdown, then terminate.
    Close,
    /// Hard: no Shutdown, terminate now.
    Cancel,
}
