//! The replicated command language.
//!
//! A mutating cluster request serializes into one [`RaftCommand`], which
//! becomes one log entry (`D = String` — the JSON encoding below). One
//! entry applies atomically via a single [`WriteBatch`] on every node,
//! which is exactly the atomicity the single-node engine already
//! guarantees: a whole SQL transaction COMMIT replicates as ONE entry.
//!
//! The legacy wire form `"SET <key> <value>"` (used by the pre-cluster
//! tests) still applies through the same path.

use crate::WriteBatch;

/// One set operation inside a replicated command.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SetOp {
    pub key: String,
    pub value: String,
    /// Absolute unix-expiry timestamp, exactly as [`WriteBatch`]
    /// stores it (`set_with_ttl` computes `now + ttl_secs`); 0 = never.
    pub ttl: u64,
}

/// A batch of mutations replicated and applied as one atomic unit.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RaftCommand {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sets: Vec<SetOp>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dels: Vec<String>,
    /// The SSI commit record for a transaction COMMIT, so that every
    /// node's apply path can record the transaction in its OWN committed
    /// history. `None` for plain writes (and for every entry written by a
    /// node that predates this field — `serde(default)` decodes those as
    /// `None`, so the log stays backward-compatible). See
    /// [`crate::transaction::SsiCommitRecord`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssi: Option<crate::transaction::SsiCommitRecord>,
}

impl RaftCommand {
    /// Serializes to the log-entry form (the `D` of a raft entry).
    pub fn encode(&self) -> String {
        serde_json::to_string(self).expect("raft command serializes")
    }

    /// Parses a log entry back into a command. `None` for entries in an
    /// unrecognized format (legacy non-JSON text): apply treats those
    /// through the legacy parser instead.
    pub fn decode(entry: &str) -> Option<Self> {
        serde_json::from_str(entry).ok()
    }

    /// Builds a command from a staged write batch. Keys must be UTF-8 —
    /// every write surface in this codebase (REST, SQL, KV grammar,
    /// TCP) is string-keyed, so this holds for anything a client can
    /// produce.
    pub fn from_batch(batch: &WriteBatch) -> Self {
        let mut cmd = Self::default();
        Self::fill_from_batch(&mut cmd, batch);
        cmd
    }

    /// Builds a transaction-COMMIT command: the batch plus the SSI record
    /// that every node's apply path records into its own committed
    /// history. Without the record riding along, the history exists only
    /// on the node that ran the COMMIT — the leader-local hole tracked as
    /// issue #124.
    pub fn from_batch_with_ssi(
        batch: &WriteBatch,
        ssi: crate::transaction::SsiCommitRecord,
    ) -> Self {
        let mut cmd = Self::default();
        Self::fill_from_batch(&mut cmd, batch);
        cmd.ssi = Some(ssi);
        cmd
    }

    fn fill_from_batch(cmd: &mut Self, batch: &WriteBatch) {
        // Writes and deletes are disjoint per batch construction (a
        // later op of the same key replaces the earlier one), so a
        // single pass preserves the batch's semantics.
        for (key, value, expiry) in &batch.buffered_writes {
            cmd.sets.push(SetOp {
                key: String::from_utf8_lossy(key).into_owned(),
                value: value.clone(),
                ttl: *expiry,
            });
        }
        for key in &batch.buffered_deletes {
            cmd.dels.push(String::from_utf8_lossy(key).into_owned());
        }
    }

    /// Whether the command carries any mutation. An SSI record is not
    /// counted: it describes writes, and a record with no writes to
    /// describe has no commit marker to stamp it at (the marker comes from
    /// the batch the apply commits). `commit_ssi_blocking` short-circuits
    /// an empty batch before a record-carrying command is ever built, so a
    /// command that reaches the log always has both.
    pub fn is_empty(&self) -> bool {
        self.sets.is_empty() && self.dels.is_empty()
    }

    /// Rejects writes into the raft's own bookkeeping keys — the
    /// storage adapter forbids them at apply time, so proposals are
    /// rejected up front with a clear error instead of an "ERR" apply
    /// result the client can't distinguish from success.
    pub fn protects_system_keys(&self) -> Result<(), String> {
        const SYS: &str = "__sys__/raft/";
        for op in &self.sets {
            if op.key.starts_with(SYS) {
                return Err(format!("key prefix '{SYS}' is reserved"));
            }
        }
        for key in &self.dels {
            if key.starts_with(SYS) {
                return Err(format!("key prefix '{SYS}' is reserved"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_round_trips_through_log_entry_form() {
        let cmd = RaftCommand {
            sets: vec![SetOp {
                key: "k1".into(),
                value: "v1".into(),
                ttl: 120,
            }],
            dels: vec!["k2".into()],
            ssi: None,
        };
        let encoded = cmd.encode();
        let decoded = RaftCommand::decode(&encoded).expect("decodes");
        assert_eq!(decoded.sets[0].key, "k1");
        assert_eq!(decoded.sets[0].ttl, 120);
        assert_eq!(decoded.dels, vec!["k2".to_string()]);
    }

    #[test]
    fn ssi_record_round_trips_and_old_entries_decode_without_it() {
        // A COMMIT command: the record must survive the log-entry form.
        let cmd = RaftCommand::from_batch_with_ssi(
            &{
                let mut b = WriteBatch::new();
                b.set("k", "v".into()).unwrap();
                b
            },
            crate::transaction::SsiCommitRecord {
                txn_id: 7,
                write_keys: vec!["k".into()],
                read_keys: vec!["r".into()],
                read_ranges: vec![("a".into(), "z".into())],
            },
        );
        let decoded = RaftCommand::decode(&cmd.encode()).expect("decodes");
        let ssi = decoded.ssi.expect("ssi record survived the round trip");
        assert_eq!(ssi.txn_id, 7);
        assert_eq!(ssi.write_keys, vec!["k"]);
        assert_eq!(ssi.read_keys, vec!["r"]);
        assert_eq!(ssi.read_ranges, vec![("a".into(), "z".into())]);

        // A pre-field log entry (plain JSON, no `ssi` key) still decodes,
        // and its SSI slot is None — the log is backward-compatible.
        let legacy = RaftCommand::decode(r#"{"sets":[{"key":"k","value":"v","ttl":0}]}"#)
            .expect("legacy entry decodes");
        assert!(legacy.ssi.is_none());
    }

    #[test]
    fn system_keys_are_rejected_at_proposal_time() {
        let mut cmd = RaftCommand::default();
        cmd.sets.push(SetOp {
            key: "__sys__/raft/meta".into(),
            value: "{}".into(),
            ttl: 0,
        });
        assert!(cmd.protects_system_keys().is_err());
    }

    #[test]
    fn empty_sets_are_omitted_from_the_entry() {
        let cmd = RaftCommand::default();
        assert_eq!(cmd.encode(), "{}");
    }
}
