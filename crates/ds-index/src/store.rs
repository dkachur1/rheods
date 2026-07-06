//! Where index runs live once built. Two implementations: an in-memory one
//! for tests, and the trait a real backend needs to satisfy.
//!
//! This crate deliberately does NOT depend on the vendored
//! `durable-streams` source (it's a binary crate, not a library — see
//! `docs/integration-points.md`), so it can't reuse its `TierConfig`/
//! `object_store` wiring directly today. Phase 2 (see `docs/plan.md`) is
//! where the patched server calls into this trait using its own already-
//! configured object-store client; until then, `RunStore` is the seam a real
//! backend plugs into.

use crate::run::IndexRun;
use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum RunStoreError {
    #[error("run store backend error: {0}")]
    Backend(String),
}

#[async_trait]
pub trait RunStore: Send + Sync {
    /// Publish a sealed, immutable run. Takes ownership — a run is built
    /// once and handed off, never mutated after. Object key convention
    /// should mirror Prisma's `streams/<hash>/index/<run-id>.idx` shape so a
    /// real object-store backend can reuse the existing `tier` cold-storage
    /// bucket layout, just under an `index/` prefix instead of `segments/`.
    async fn publish(&self, stream_id: &str, run: IndexRun) -> Result<(), RunStoreError>;

    /// Runs whose `[start_segment, end_segment)` span could contain
    /// `segment_hint` (or all runs, if the backend can't cheaply filter —
    /// callers still get correct results, just less pruning). Returns owned
    /// clones rather than references: a real backend fetches run bytes over
    /// the network anyway (no borrow to hand back), so the in-memory
    /// implementation matches that shape instead of leaking a lock guard's
    /// lifetime through the trait.
    async fn runs_covering(
        &self,
        stream_id: &str,
        segment_hint: u64,
    ) -> Result<Vec<IndexRun>, RunStoreError>;
}

/// In-memory `RunStore` for tests and for exercising the read/write path
/// before a real object-store backend exists.
#[derive(Default)]
pub struct InMemoryRunStore {
    runs: tokio::sync::RwLock<std::collections::HashMap<String, Vec<IndexRun>>>,
}

#[async_trait]
impl RunStore for InMemoryRunStore {
    async fn publish(&self, stream_id: &str, run: IndexRun) -> Result<(), RunStoreError> {
        self.runs
            .write()
            .await
            .entry(stream_id.to_string())
            .or_default()
            .push(run);
        Ok(())
    }

    async fn runs_covering(
        &self,
        stream_id: &str,
        segment_hint: u64,
    ) -> Result<Vec<IndexRun>, RunStoreError> {
        Ok(self
            .runs
            .read()
            .await
            .get(stream_id)
            .map(|runs| {
                runs.iter()
                    .filter(|r| r.covers(segment_hint))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default())
    }
}
