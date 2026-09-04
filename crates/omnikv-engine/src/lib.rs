#![expect(
    dead_code,
    unused_imports,
    unused_variables,
    reason = "Legacy modules still expose staged database features and compatibility shims; issue #64 makes this debt explicit instead of hiding it behind broad allow attributes."
)]
#![expect(
    clippy::assigning_clones,
    clippy::bool_to_int_with_if,
    clippy::branches_sharing_code,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::collapsible_else_if,
    clippy::collection_is_never_read,
    clippy::derive_partial_eq_without_eq,
    clippy::doc_markdown,
    clippy::explicit_iter_loop,
    clippy::format_collect,
    clippy::format_push_string,
    clippy::if_not_else,
    clippy::items_after_statements,
    clippy::manual_let_else,
    clippy::manual_string_new,
    clippy::map_unwrap_or,
    clippy::match_same_arms,
    clippy::match_wildcard_for_single_variants,
    clippy::missing_const_for_fn,
    clippy::missing_errors_doc,
    clippy::missing_panics_doc,
    clippy::must_use_candidate,
    clippy::needless_collect,
    clippy::needless_continue,
    clippy::needless_pass_by_value,
    clippy::non_std_lazy_statics,
    clippy::option_if_let_else,
    clippy::or_fun_call,
    clippy::redundant_clone,
    clippy::redundant_closure_for_method_calls,
    clippy::redundant_else,
    clippy::self_only_used_in_recursion,
    clippy::semicolon_if_nothing_returned,
    clippy::significant_drop_tightening,
    clippy::single_char_pattern,
    clippy::single_match_else,
    clippy::suboptimal_flops,
    clippy::too_long_first_doc_paragraph,
    clippy::too_many_lines,
    clippy::uninlined_format_args,
    clippy::unnecessary_wraps,
    clippy::unreadable_literal,
    clippy::unused_self,
    clippy::use_self,
    clippy::used_underscore_binding,
    reason = "Strict clippy::pedantic and clippy::nursery are now enabled. These legacy findings are documented debt to burn down in focused follow-up PRs while preventing new undocumented lint categories."
)]

pub mod storage {
    pub mod core;
}

pub use storage::core::*;

#[path = "storage/backup.rs"]
pub mod backup;
#[path = "storage/crypto.rs"]
pub mod crypto;
#[path = "storage/transaction.rs"]
pub mod transaction;
#[path = "storage/wal.rs"]
pub mod wal;

pub mod embedded;
pub use embedded::{
    EmbeddedBatch, EmbeddedConfig, EmbeddedError, EmbeddedOmniKv, EmbeddedSnapshot,
    EmbeddedSqlResult, EmbeddedStats, EmbeddedWrite, KeyValue,
};

#[path = "query/catalog.rs"]
pub mod catalog;
#[path = "query/optimizer.rs"]
pub mod optimizer;
#[path = "query/pgwire.rs"]
pub mod pgwire;
#[path = "query/plan_exec.rs"]
pub mod plan_exec;
#[path = "query/prepared.rs"]
pub mod prepared;
#[path = "query/query.rs"]
pub mod query;
#[path = "query/schema.rs"]
pub mod schema;
#[path = "query/secondary_index.rs"]
pub mod secondary_index;
#[path = "query/sql.rs"]
pub mod sql;
#[path = "query/sql_exec.rs"]
pub mod sql_exec;
#[path = "query/volcano.rs"]
pub mod volcano;

#[path = "raft/raft_impl.rs"]
pub mod raft_impl;
#[path = "raft/raft_init.rs"]
pub mod raft_init;
#[path = "raft/raft_network.rs"]
pub mod raft_network;
#[path = "raft/raft_storage.rs"]
pub mod raft_storage;

#[path = "runtime/chaos.rs"]
pub mod chaos;
#[path = "runtime/config.rs"]
pub mod config;
#[path = "runtime/dist_txn.rs"]
pub mod dist_txn;
#[path = "runtime/failpoints.rs"]
pub mod failpoints;
#[path = "runtime/generator.rs"]
pub mod generator;
#[path = "runtime/hardening.rs"]
pub mod hardening;
#[path = "runtime/metrics_prometheus.rs"]
pub mod metrics_prometheus;
#[path = "runtime/ops.rs"]
pub mod ops;
