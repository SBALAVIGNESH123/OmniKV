//! Prepared Statement & Query Plan Cache Engine
//!
//! Provides parameterized query support and an LRU-based plan cache to avoid
//! re-parsing identical queries. This is how production databases like
//! PostgreSQL and MySQL eliminate redundant parsing overhead.
//!
//! ## Features
//!
//! - **Parameterized queries**: `SELECT * WHERE key >= $1 AND key <= $2`
//! - **Named parameters**: `SELECT * WHERE key = :user_id`
//! - **Plan caching**: LRU cache with configurable capacity
//! - **Prepared statement handles**: Prepare once, execute many times
//! - **Execution engine**: Binds parameters and runs against OmniKV
//! - **Cache statistics**: Hit/miss counters for monitoring
//!
//! ## Usage
//!
//! ```rust,ignore
//! let qe = QueryEngine::new(db, 1000); // 1000-entry plan cache
//!
//! // Prepare once
//! let stmt = qe.prepare("SELECT * WHERE key >= $1 AND key <= $2")?;
//!
//! // Execute many times with different params
//! let results = qe.execute(&stmt, &["100", "200"])?;
//! let results2 = qe.execute(&stmt, &["500", "600"])?;
//! ```

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{OmniKV, WriteBatch, OmniError};
use crate::query::{self, Action, Condition, Operator, Query};

/// Unique handle for a prepared statement.
pub type StmtId = u64;

/// A parameter placeholder in a query template.
#[derive(Debug, Clone, PartialEq)]
pub enum ParamRef {
    /// Positional: $1, $2, ...
    Positional(usize),
    /// Named: :name, :user_id, ...
    Named(String),
    /// Literal value (no parameter substitution needed).
    Literal(String),
}

/// A prepared query template — parsed once, executed many times.
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// Unique statement ID.
    pub id: StmtId,
    /// The original query string (with placeholders).
    pub query_template: String,
    /// The parsed action type.
    pub action: PreparedAction,
    /// Parameterized conditions.
    pub conditions: Vec<PreparedCondition>,
    /// LIMIT clause.
    pub limit: Option<usize>,
    /// ORDER BY DESC flag.
    pub order_desc: bool,
    /// Number of positional parameters ($1, $2, ...).
    pub param_count: usize,
    /// Named parameter names.
    pub named_params: Vec<String>,
}

/// Prepared action — like Action but with parameter references for INSERT/UPDATE.
#[derive(Debug, Clone, PartialEq)]
pub enum PreparedAction {
    SelectAll,
    SelectCount,
    Delete,
    Insert(ParamRef, ParamRef),  // key, value
    Update(ParamRef, ParamRef),  // value, key (SET value WHERE key)
}

/// A condition with parameter references instead of literal values.
#[derive(Debug, Clone)]
pub struct PreparedCondition {
    pub operator: Operator,
    pub value: ParamRef,
}

impl PartialEq for PreparedCondition {
    fn eq(&self, other: &Self) -> bool {
        self.operator == other.operator && self.value == other.value
    }
}

/// LRU-ish plan cache entry.
#[derive(Clone)]
struct CacheEntry {
    stmt: PreparedStatement,
    access_count: u64,
}

/// Query Plan Cache — avoids re-parsing identical query templates.
struct PlanCache {
    /// Template string → cached prepared statement.
    entries: HashMap<String, CacheEntry>,
    /// Maximum number of cached plans.
    capacity: usize,
    /// Statistics.
    hits: u64,
    misses: u64,
}

impl PlanCache {
    fn new(capacity: usize) -> Self {
        Self {
            entries: HashMap::new(),
            capacity,
            hits: 0,
            misses: 0,
        }
    }

    fn get(&mut self, template: &str) -> Option<PreparedStatement> {
        if let Some(entry) = self.entries.get_mut(template) {
            entry.access_count += 1;
            self.hits += 1;
            Some(entry.stmt.clone())
        } else {
            self.misses += 1;
            None
        }
    }

    fn insert(&mut self, template: String, stmt: PreparedStatement) {
        // Evict least-accessed entry if at capacity
        if self.entries.len() >= self.capacity {
            let min_key = self.entries.iter()
                .min_by_key(|(_, e)| e.access_count)
                .map(|(k, _)| k.clone());
            if let Some(key) = min_key {
                self.entries.remove(&key);
            }
        }
        self.entries.insert(template, CacheEntry {
            stmt,
            access_count: 1,
        });
    }

    fn stats(&self) -> (u64, u64, usize) {
        (self.hits, self.misses, self.entries.len())
    }

    fn clear(&mut self) {
        self.entries.clear();
        self.hits = 0;
        self.misses = 0;
    }
}

/// Result of executing a prepared statement.
#[derive(Debug, Clone)]
pub enum QueryResult {
    /// Rows returned by SELECT *.
    Rows(Vec<(String, String)>),
    /// Count returned by SELECT COUNT.
    Count(usize),
    /// Rows affected by INSERT/UPDATE/DELETE.
    Affected(usize),
}

/// The Query Engine — the main interface for prepared statements and execution.
pub struct QueryEngine {
    db: Arc<OmniKV>,
    cache: Mutex<PlanCache>,
    next_stmt_id: AtomicU64,
}

impl QueryEngine {
    /// Creates a new QueryEngine with the given plan cache capacity.
    pub fn new(db: Arc<OmniKV>, cache_capacity: usize) -> Self {
        Self {
            db,
            cache: Mutex::new(PlanCache::new(cache_capacity)),
            next_stmt_id: AtomicU64::new(1),
        }
    }

    /// PREPARE — parses a query template and returns a reusable statement handle.
    ///
    /// Supports positional (`$1`, `$2`) and named (`:name`) parameter placeholders.
    /// The parsed plan is cached so identical templates skip parsing on subsequent calls.
    pub fn prepare(&self, query_template: &str) -> Result<PreparedStatement, OmniError> {
        // Check plan cache first
        let mut cache = self.cache.lock()
            .map_err(|_| OmniError::LockPoisoned("plan cache".into()))?;
        
        if let Some(mut cached) = cache.get(query_template) {
            // Re-assign a fresh ID for this handle
            cached.id = self.next_stmt_id.fetch_add(1, Ordering::SeqCst);
            return Ok(cached);
        }
        drop(cache); // Release lock during parsing

        // Parse the template
        let stmt = self.parse_template(query_template)?;

        // Cache the parsed plan
        let mut cache = self.cache.lock()
            .map_err(|_| OmniError::LockPoisoned("plan cache".into()))?;
        cache.insert(query_template.to_string(), stmt.clone());

        Ok(stmt)
    }

    /// EXECUTE — runs a prepared statement with bound parameter values.
    ///
    /// Positional params: `execute(&stmt, &["value1", "value2"])`
    pub fn execute(&self, stmt: &PreparedStatement, params: &[&str]) -> Result<QueryResult, OmniError> {
        self.execute_with_params(stmt, params, &HashMap::new())
    }

    /// EXECUTE with named params — binds named parameters by name.
    ///
    /// Named params: `execute_named(&stmt, &[("user_id", "42")])`
    pub fn execute_named(
        &self,
        stmt: &PreparedStatement,
        named_params: &[(&str, &str)],
    ) -> Result<QueryResult, OmniError> {
        let map: HashMap<String, String> = named_params.iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        self.execute_with_params(stmt, &[], &map)
    }

    /// EXECUTE_QUERY — one-shot parse + execute (no prepare step).
    /// Uses the plan cache internally for repeated queries.
    pub fn execute_query(&self, query_str: &str) -> Result<QueryResult, OmniError> {
        let stmt = self.prepare(query_str)?;
        self.execute(&stmt, &[])
    }

    /// Returns cache statistics: (hits, misses, cached_plans).
    pub fn cache_stats(&self) -> (u64, u64, usize) {
        let cache = self.cache.lock().expect("plan cache");
        cache.stats()
    }

    /// Clears the plan cache.
    pub fn clear_cache(&self) {
        let mut cache = self.cache.lock().expect("plan cache");
        cache.clear();
    }

    // ═══════════════════════════════════════════════════════════════
    // Internal: Template Parsing
    // ═══════════════════════════════════════════════════════════════

    fn parse_template(&self, template: &str) -> Result<PreparedStatement, OmniError> {
        let id = self.next_stmt_id.fetch_add(1, Ordering::SeqCst);
        let tokens: Vec<&str> = template.split_whitespace().collect();
        
        if tokens.is_empty() {
            return Err(OmniError::IoError("Empty query".into()));
        }

        let mut param_count = 0;
        let mut named_params = Vec::new();

        // Count parameters
        for token in &tokens {
            if token.starts_with('$') {
                if let Ok(n) = token[1..].parse::<usize>() {
                    if n > param_count { param_count = n; }
                }
            } else if token.starts_with(':') && token.len() > 1 {
                let name = token[1..].to_string();
                if !named_params.contains(&name) {
                    named_params.push(name);
                }
            }
        }

        match tokens[0].to_uppercase().as_str() {
            "SELECT" => self.parse_select_template(id, template, &tokens, param_count, named_params),
            "DELETE" => self.parse_delete_template(id, template, &tokens, param_count, named_params),
            "INSERT" => self.parse_insert_template(id, template, &tokens, param_count, named_params),
            "UPDATE" => self.parse_update_template(id, template, &tokens, param_count, named_params),
            _ => Err(OmniError::IoError(format!("Unsupported command: {}", tokens[0]))),
        }
    }

    fn parse_param_ref(token: &str) -> ParamRef {
        if token.starts_with('$') {
            if let Ok(n) = token[1..].parse::<usize>() {
                return ParamRef::Positional(n);
            }
        }
        if token.starts_with(':') && token.len() > 1 {
            return ParamRef::Named(token[1..].to_string());
        }
        ParamRef::Literal(token.to_string())
    }

    fn parse_select_template(
        &self, id: StmtId, template: &str, tokens: &[&str],
        param_count: usize, named_params: Vec<String>,
    ) -> Result<PreparedStatement, OmniError> {
        if tokens.len() < 2 {
            return Err(OmniError::IoError("Expected * or COUNT after SELECT".into()));
        }

        let action = if tokens[1] == "*" {
            PreparedAction::SelectAll
        } else if tokens[1].to_uppercase() == "COUNT" {
            PreparedAction::SelectCount
        } else {
            return Err(OmniError::IoError("Expected * or COUNT after SELECT".into()));
        };

        let mut conditions = Vec::new();
        let mut limit = None;
        let mut order_desc = false;
        let mut i = 2;

        // Parse WHERE
        if i < tokens.len() && tokens[i].to_uppercase() == "WHERE" {
            i += 1;
            while i < tokens.len() {
                let upper = tokens[i].to_uppercase();
                if upper == "AND" { i += 1; continue; }
                if upper == "LIMIT" || upper == "ORDER" { break; }
                if tokens[i].to_lowercase() != "key" {
                    return Err(OmniError::IoError("Can only filter on 'key'".into()));
                }
                if i + 2 >= tokens.len() {
                    return Err(OmniError::IoError("Incomplete condition".into()));
                }

                let op = match tokens[i+1] {
                    "=" => Operator::Eq,
                    ">=" => Operator::Gte,
                    "<=" => Operator::Lte,
                    other => return Err(OmniError::IoError(format!("Unsupported operator: {}", other))),
                };
                let value = Self::parse_param_ref(tokens[i+2]);
                conditions.push(PreparedCondition { operator: op, value });
                i += 3;
            }
        }

        // Parse ORDER BY
        if i < tokens.len() && tokens[i].to_uppercase() == "ORDER" {
            if i + 1 < tokens.len() && tokens[i+1].to_uppercase() == "BY" {
                i += 2;
                if i < tokens.len() && tokens[i].to_uppercase() == "DESC" {
                    order_desc = true;
                    i += 1;
                } else if i < tokens.len() && tokens[i].to_uppercase() == "ASC" {
                    i += 1;
                }
            }
        }

        // Parse LIMIT
        if i < tokens.len() && tokens[i].to_uppercase() == "LIMIT" {
            i += 1;
            if i < tokens.len() {
                let param = Self::parse_param_ref(tokens[i]);
                match &param {
                    ParamRef::Literal(v) => {
                        limit = Some(v.parse::<usize>()
                            .map_err(|_| OmniError::IoError(format!("Invalid LIMIT: {}", v)))?);
                    }
                    _ => {
                        // LIMIT with parameter — resolved at execution time
                        // For now, we don't support parameterized LIMIT
                        return Err(OmniError::IoError("Parameterized LIMIT not yet supported".into()));
                    }
                }
            }
        }

        Ok(PreparedStatement {
            id, query_template: template.to_string(),
            action, conditions, limit, order_desc,
            param_count, named_params,
        })
    }

    fn parse_delete_template(
        &self, id: StmtId, template: &str, tokens: &[&str],
        param_count: usize, named_params: Vec<String>,
    ) -> Result<PreparedStatement, OmniError> {
        let mut conditions = Vec::new();
        let mut i = 1;

        if i >= tokens.len() || tokens[i].to_uppercase() != "WHERE" {
            return Err(OmniError::IoError("DELETE requires WHERE clause".into()));
        }
        i += 1;

        while i < tokens.len() {
            let upper = tokens[i].to_uppercase();
            if upper == "AND" { i += 1; continue; }
            if tokens[i].to_lowercase() != "key" {
                return Err(OmniError::IoError("Can only filter on 'key'".into()));
            }
            if i + 2 >= tokens.len() {
                return Err(OmniError::IoError("Incomplete condition".into()));
            }

            let op = match tokens[i+1] {
                "=" => Operator::Eq,
                ">=" => Operator::Gte,
                "<=" => Operator::Lte,
                other => return Err(OmniError::IoError(format!("Unsupported operator: {}", other))),
            };
            let value = Self::parse_param_ref(tokens[i+2]);
            conditions.push(PreparedCondition { operator: op, value });
            i += 3;
        }

        if conditions.is_empty() {
            return Err(OmniError::IoError("DELETE WHERE requires conditions".into()));
        }

        Ok(PreparedStatement {
            id, query_template: template.to_string(),
            action: PreparedAction::Delete, conditions, limit: None, order_desc: false,
            param_count, named_params,
        })
    }

    fn parse_insert_template(
        &self, id: StmtId, template: &str, tokens: &[&str],
        param_count: usize, named_params: Vec<String>,
    ) -> Result<PreparedStatement, OmniError> {
        if tokens.len() < 3 {
            return Err(OmniError::IoError("INSERT requires <key> <value>".into()));
        }
        let key_ref = Self::parse_param_ref(tokens[1]);
        let val_ref = Self::parse_param_ref(tokens[2]);

        Ok(PreparedStatement {
            id, query_template: template.to_string(),
            action: PreparedAction::Insert(key_ref, val_ref),
            conditions: Vec::new(), limit: None, order_desc: false,
            param_count, named_params,
        })
    }

    fn parse_update_template(
        &self, id: StmtId, template: &str, tokens: &[&str],
        param_count: usize, named_params: Vec<String>,
    ) -> Result<PreparedStatement, OmniError> {
        // UPDATE SET value = $1 WHERE key = $2
        if tokens.len() < 8 {
            return Err(OmniError::IoError("UPDATE requires SET value = <val> WHERE key = <key>".into()));
        }
        if tokens[1].to_uppercase() != "SET" || tokens[2].to_uppercase() != "VALUE" || tokens[3] != "=" {
            return Err(OmniError::IoError("Expected SET value =".into()));
        }

        let mut i = 4;
        let mut value_tokens = Vec::new();
        while i < tokens.len() && tokens[i].to_uppercase() != "WHERE" {
            value_tokens.push(tokens[i]);
            i += 1;
        }
        if value_tokens.is_empty() {
            return Err(OmniError::IoError("Missing value in UPDATE".into()));
        }

        // If single token and it's a param, use param ref; otherwise literal
        let val_ref = if value_tokens.len() == 1 {
            Self::parse_param_ref(value_tokens[0])
        } else {
            ParamRef::Literal(value_tokens.join(" "))
        };

        if i >= tokens.len() || tokens[i].to_uppercase() != "WHERE" {
            return Err(OmniError::IoError("UPDATE requires WHERE clause".into()));
        }
        i += 1;
        if i + 2 >= tokens.len() || tokens[i].to_lowercase() != "key" || tokens[i+1] != "=" {
            return Err(OmniError::IoError("WHERE must be: WHERE key = <key>".into()));
        }
        let key_ref = Self::parse_param_ref(tokens[i+2]);

        Ok(PreparedStatement {
            id, query_template: template.to_string(),
            action: PreparedAction::Update(val_ref, key_ref),
            conditions: Vec::new(), limit: None, order_desc: false,
            param_count, named_params,
        })
    }

    // ═══════════════════════════════════════════════════════════════
    // Internal: Execution
    // ═══════════════════════════════════════════════════════════════

    fn execute_with_params(
        &self,
        stmt: &PreparedStatement,
        positional: &[&str],
        named: &HashMap<String, String>,
    ) -> Result<QueryResult, OmniError> {
        let resolve = |p: &ParamRef| -> Result<String, OmniError> {
            match p {
                ParamRef::Positional(n) => {
                    positional.get(*n - 1)
                        .map(|s| s.to_string())
                        .ok_or_else(|| OmniError::IoError(format!("Missing parameter ${}", n)))
                }
                ParamRef::Named(name) => {
                    named.get(name)
                        .cloned()
                        .ok_or_else(|| OmniError::IoError(format!("Missing parameter :{}", name)))
                }
                ParamRef::Literal(v) => Ok(v.clone()),
            }
        };

        match &stmt.action {
            PreparedAction::SelectAll | PreparedAction::SelectCount => {
                // Resolve conditions
                let mut start_key = String::new();
                let mut end_key = String::from("~");
                let mut exact_key = None;

                for cond in &stmt.conditions {
                    let val = resolve(&cond.value)?;
                    match cond.operator {
                        Operator::Eq => exact_key = Some(val),
                        Operator::Gte => start_key = val,
                        Operator::Lte => end_key = val,
                    }
                }

                let seq = self.db.get_seq();

                if let Some(key) = exact_key {
                    // Point lookup
                    let val = self.db.find(&key, seq)?;
                    match &stmt.action {
                        PreparedAction::SelectCount => {
                            Ok(QueryResult::Count(if val.is_some() { 1 } else { 0 }))
                        }
                        _ => {
                            match val {
                                Some(v) => Ok(QueryResult::Rows(vec![(key, v)])),
                                None => Ok(QueryResult::Rows(vec![])),
                            }
                        }
                    }
                } else {
                    // Range scan
                    let mut results: Vec<(String, String)> = self.db.scan_iter(&start_key, &end_key, seq)?
                        .collect();

                    if stmt.order_desc {
                        results.reverse();
                    }
                    if let Some(limit) = stmt.limit {
                        results.truncate(limit);
                    }

                    match &stmt.action {
                        PreparedAction::SelectCount => Ok(QueryResult::Count(results.len())),
                        _ => Ok(QueryResult::Rows(results)),
                    }
                }
            }

            PreparedAction::Insert(key_ref, val_ref) => {
                let key = resolve(key_ref)?;
                let value = resolve(val_ref)?;
                let mut batch = WriteBatch::new();
                batch.set(&key, value)?;
                self.db.commit_batch(&batch)?;
                Ok(QueryResult::Affected(1))
            }

            PreparedAction::Update(val_ref, key_ref) => {
                let key = resolve(key_ref)?;
                let value = resolve(val_ref)?;
                let mut batch = WriteBatch::new();
                batch.set(&key, value)?;
                self.db.commit_batch(&batch)?;
                Ok(QueryResult::Affected(1))
            }

            PreparedAction::Delete => {
                let mut affected = 0;
                let seq = self.db.get_seq();

                for cond in &stmt.conditions {
                    let val = resolve(&cond.value)?;
                    if cond.operator == Operator::Eq {
                        let mut batch = WriteBatch::new();
                        batch.delete(&val)?;
                        self.db.commit_batch(&batch)?;
                        affected += 1;
                    }
                }

                // Range delete
                let mut start = None;
                let mut end = None;
                for cond in &stmt.conditions {
                    let val = resolve(&cond.value)?;
                    match cond.operator {
                        Operator::Gte => start = Some(val),
                        Operator::Lte => end = Some(val),
                        _ => {}
                    }
                }
                if let (Some(s), Some(e)) = (start, end) {
                    let keys: Vec<String> = self.db.scan_iter(&s, &e, seq)?
                        .map(|(k, _)| k)
                        .collect();
                    let mut batch = WriteBatch::new();
                    for k in &keys {
                        batch.delete(k)?;
                    }
                    self.db.commit_batch(&batch)?;
                    affected += keys.len();
                }

                Ok(QueryResult::Affected(affected))
            }
        }
    }
}
