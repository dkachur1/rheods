//! Routing-key index for durable-streams. See `../../../docs/plan.md` for
//! what's implemented vs. stubbed, and `../../../docs/integration-points.md`
//! for exactly where this hooks into the vendored server.

pub mod accumulator;
pub mod fingerprint;
pub mod run;
pub mod store;

pub use accumulator::SegmentAccumulator;
pub use fingerprint::FingerprintKey;
pub use run::{IndexRun, RunBuilder, DEFAULT_RUN_SPAN};
pub use store::{InMemoryRunStore, RunStore, RunStoreError};

#[cfg(test)]
mod integration_test {
    use super::*;

    #[tokio::test]
    async fn publish_then_lookup_round_trip() {
        let key = FingerprintKey::from_bytes([5u8; 16]);
        let mut builder = RunBuilder::new(key, 0);
        builder.observe(b"conv:xyz", 2);
        let run = builder.build().expect("non-empty");

        let store = InMemoryRunStore::default();
        store.publish("stream-1", run).await.expect("publish ok");

        let runs = store
            .runs_covering("stream-1", 2)
            .await
            .expect("lookup ok");
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].candidates(&key, b"conv:xyz"), Some(vec![2]));
    }
}
