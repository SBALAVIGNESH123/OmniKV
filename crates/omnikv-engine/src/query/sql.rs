//! SQL Parser v2 — Full SQL support
//!
//! Supports: CREATE TABLE, DROP TABLE, INSERT INTO, SELECT with JOIN,
//! WHERE with AND/OR, GROUP BY, ORDER BY, LIMIT, aggregates.

use crate::catalog::ColumnType;

#[derive(Debug, Clone, PartialEq)]
#[expect(
    clippy::large_enum_variant,
    reason = "SQL statement variants intentionally keep owned AST payloads for parser simplicity; boxing the largest variant needs a separate API-impact review."
)]
pub enum SqlStatement {
    CreateTable {
        name: String,
        columns: Vec<SqlColumnDef>,
        if_not_exists: bool,
    },
    DropTable {
        name: String,
        if_exists: bool,
    },
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<SqlValue>>,
    },
    Select {
        columns: Vec<SelectColumn>,
        from: FromClause,
        where_clause: Option<WhereExpr>,
        group_by: Vec<String>,
        having: Option<WhereExpr>,
        order_by: Vec<OrderByItem>,
        limit: Option<usize>,
        offset: Option<usize>,
    },
    Update {
        table: String,
        assignments: Vec<(String, SqlValue)>,
        where_clause: Option<WhereExpr>,
    },
    Delete {
        table: String,
        where_clause: Option<WhereExpr>,
    },
    ShowTables,
    Explain(Box<SqlStatement>),
    ExplainAnalyze(Box<SqlStatement>),
    /// UNION / INTERSECT / EXCEPT
    SetOp {
        op: SetOpType,
        left: Box<SqlStatement>,
        right: Box<SqlStatement>,
        all: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetOpType {
    Union,
    Intersect,
    Except,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SqlColumnDef {
    pub name: String,
    pub col_type: String,
    pub primary_key: bool,
    pub nullable: bool,
    pub default: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SelectColumn {
    Star,
    Named(String),
    Qualified(String, String), // table.column
    Aggregate(AggFunc, String),
    /// Window function: ROW_NUMBER() / RANK() / DENSE_RANK() OVER (ORDER BY col [ASC|DESC])
    WindowFunc {
        func: WindowFuncType,
        order_by: String,
        desc: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowFuncType {
    RowNumber,
    Rank,
    DenseRank,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FromClause {
    Table(String),
    Join {
        left: String,
        right: String,
        join_type: JoinType,
        on_left: String,
        on_right: String,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WhereExpr {
    Comparison {
        column: String,
        op: CmpOp,
        value: SqlValue,
    },
    And(Box<WhereExpr>, Box<WhereExpr>),
    Or(Box<WhereExpr>, Box<WhereExpr>),
    Not(Box<WhereExpr>),
    IsNull(String),
    IsNotNull(String),
    In(String, Vec<SqlValue>),
    /// Subquery: WHERE column IN (SELECT ...)
    InSubquery(String, Box<SqlStatement>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CmpOp {
    Eq,
    Ne,
    Gt,
    Lt,
    Gte,
    Lte,
    Like,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Text(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}

impl SqlValue {
    pub fn as_string(&self) -> String {
        match self {
            Self::Text(s) => s.clone(),
            Self::Integer(i) => i.to_string(),
            Self::Float(f) => f.to_string(),
            Self::Boolean(b) => b.to_string(),
            Self::Null => "NULL".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByItem {
    pub column: String,
    pub desc: bool,
}

/// Parse a SQL string into a structured statement.
pub fn parse_sql(input: &str) -> Result<SqlStatement, String> {
    let trimmed = input.trim().trim_end_matches(';');
    let tokens = tokenize(trimmed);
    if tokens.is_empty() {
        return Err("Empty query".into());
    }

    match tokens[0].to_uppercase().as_str() {
        "CREATE" => parse_create_table(&tokens),
        "DROP" => parse_drop_table(&tokens),
        "INSERT" => parse_insert_sql(&tokens),
        "SELECT" => {
            // Check for UNION / INTERSECT / EXCEPT at top level
            let mut depth = 0;
            let mut set_op_pos = None;
            for (idx, tok) in tokens.iter().enumerate() {
                if tok == "(" {
                    depth += 1;
                }
                if tok == ")" {
                    depth -= 1;
                }
                if depth == 0 {
                    let upper = tok.to_uppercase();
                    if upper == "UNION" || upper == "INTERSECT" || upper == "EXCEPT" {
                        set_op_pos = Some(idx);
                        break;
                    }
                }
            }

            if let Some(pos) = set_op_pos {
                let op_token = tokens[pos].to_uppercase();
                let mut right_start = pos + 1;
                let all =
                    if right_start < tokens.len() && tokens[right_start].to_uppercase() == "ALL" {
                        right_start += 1;
                        true
                    } else {
                        false
                    };

                let left_sql = tokens[..pos].join(" ");
                let right_sql = tokens[right_start..].join(" ");

                let left = parse_sql(&left_sql)?;
                let right = parse_sql(&right_sql)?;

                let op = match op_token.as_str() {
                    "UNION" => SetOpType::Union,
                    "INTERSECT" => SetOpType::Intersect,
                    "EXCEPT" => SetOpType::Except,
                    _ => unreachable!(),
                };

                Ok(SqlStatement::SetOp {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                    all,
                })
            } else {
                parse_select_sql(&tokens)
            }
        }
        "UPDATE" => parse_update_sql(&tokens),
        "DELETE" => parse_delete_sql(&tokens),
        "SHOW" => Ok(SqlStatement::ShowTables),
        "EXPLAIN" => {
            if tokens.get(1).map(|t| t.to_uppercase()) == Some("ANALYZE".into()) {
                let inner = parse_sql(&tokens[2..].join(" "))?;
                Ok(SqlStatement::ExplainAnalyze(Box::new(inner)))
            } else {
                let inner = parse_sql(&tokens[1..].join(" "))?;
                Ok(SqlStatement::Explain(Box::new(inner)))
            }
        }
        _ => Err(format!("Unsupported: {}", tokens[0])),
    }
}

fn tokenize(input: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.next() {
        if in_string {
            if ch == '\'' {
                tokens.push(format!("'{}'", current));
                current.clear();
                in_string = false;
            } else {
                current.push(ch);
            }
        } else if ch == '\'' {
            if !current.is_empty() {
                tokens.push(current.clone());
                current.clear();
            }
            in_string = true;
        } else if ch == '(' || ch == ')' || ch == ',' || ch == '*' {
            if !current.is_empty() {
                tokens.push(current.clone());
                current.clear();
            }
            tokens.push(ch.to_string());
        } else if ch == '!' && chars.peek() == Some(&'=') {
            if !current.is_empty() {
                tokens.push(current.clone());
                current.clear();
            }
            chars.next();
            tokens.push("!=".to_string());
        } else if ch == '<' || ch == '>' {
            if !current.is_empty() {
                tokens.push(current.clone());
                current.clear();
            }
            if chars.peek() == Some(&'=') {
                chars.next();
                tokens.push(format!("{}=", ch));
            } else {
                tokens.push(ch.to_string());
            }
        } else if ch == '=' {
            if !current.is_empty() {
                tokens.push(current.clone());
                current.clear();
            }
            tokens.push("=".to_string());
        } else if ch.is_whitespace() {
            if !current.is_empty() {
                tokens.push(current.clone());
                current.clear();
            }
        } else {
            current.push(ch);
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

fn parse_value(token: &str) -> SqlValue {
    if token.starts_with('\'') && token.ends_with('\'') {
        return SqlValue::Text(token[1..token.len() - 1].to_string());
    }
    let upper = token.to_uppercase();
    if upper == "NULL" {
        return SqlValue::Null;
    }
    if upper == "TRUE" {
        return SqlValue::Boolean(true);
    }
    if upper == "FALSE" {
        return SqlValue::Boolean(false);
    }
    if let Ok(i) = token.parse::<i64>() {
        return SqlValue::Integer(i);
    }
    if let Ok(f) = token.parse::<f64>() {
        return SqlValue::Float(f);
    }
    SqlValue::Text(token.to_string())
}

fn parse_create_table(tokens: &[String]) -> Result<SqlStatement, String> {
    let mut i = 1;
    if tokens.get(i).map(|t| t.to_uppercase()) != Some("TABLE".into()) {
        return Err("Expected TABLE after CREATE".into());
    }
    i += 1;

    let if_not_exists = if i + 2 < tokens.len()
        && tokens[i].to_uppercase() == "IF"
        && tokens[i + 1].to_uppercase() == "NOT"
        && tokens[i + 2].to_uppercase() == "EXISTS"
    {
        i += 3;
        true
    } else {
        false
    };

    let name = tokens.get(i).ok_or("Missing table name")?.clone();
    i += 1;

    if tokens.get(i).map(|t| t.as_str()) != Some("(") {
        return Err("Expected ( after table name".into());
    }
    i += 1;

    let mut columns = Vec::new();
    while i < tokens.len() && tokens[i] != ")" {
        if tokens[i] == "," {
            i += 1;
            continue;
        }
        let col_name = tokens[i].clone();
        i += 1;
        let col_type = tokens.get(i).ok_or("Missing column type")?.clone();
        i += 1;

        let mut pk = false;
        let mut nullable = true;
        let mut default = None;

        while i < tokens.len() && tokens[i] != "," && tokens[i] != ")" {
            let upper = tokens[i].to_uppercase();
            if upper == "PRIMARY" {
                i += 1; // skip KEY
                if tokens.get(i).map(|t| t.to_uppercase()) == Some("KEY".into()) {
                    i += 1;
                }
                pk = true;
                nullable = false;
            } else if upper == "NOT" {
                i += 1; // skip NULL
                if tokens.get(i).map(|t| t.to_uppercase()) == Some("NULL".into()) {
                    i += 1;
                }
                nullable = false;
            } else if upper == "DEFAULT" {
                i += 1;
                default = tokens.get(i).cloned();
                i += 1;
            } else {
                i += 1;
            }
        }

        columns.push(SqlColumnDef {
            name: col_name,
            col_type,
            primary_key: pk,
            nullable,
            default,
        });
    }

    Ok(SqlStatement::CreateTable {
        name,
        columns,
        if_not_exists,
    })
}

fn parse_drop_table(tokens: &[String]) -> Result<SqlStatement, String> {
    let mut i = 1;
    if tokens.get(i).map(|t| t.to_uppercase()) != Some("TABLE".into()) {
        return Err("Expected TABLE after DROP".into());
    }
    i += 1;
    let if_exists = if tokens.get(i).map(|t| t.to_uppercase()) == Some("IF".into())
        && tokens.get(i + 1).map(|t| t.to_uppercase()) == Some("EXISTS".into())
    {
        i += 2;
        true
    } else {
        false
    };

    let name = tokens.get(i).ok_or("Missing table name")?.clone();
    Ok(SqlStatement::DropTable { name, if_exists })
}

fn parse_insert_sql(tokens: &[String]) -> Result<SqlStatement, String> {
    // INSERT INTO table (cols) VALUES (vals), (vals)
    let mut i = 1;
    if tokens.get(i).map(|t| t.to_uppercase()) != Some("INTO".into()) {
        return Err("Expected INTO after INSERT".into());
    }
    i += 1;
    let table = tokens.get(i).ok_or("Missing table name")?.clone();
    i += 1;

    let mut columns = Vec::new();
    if tokens.get(i).map(|t| t.as_str()) == Some("(") {
        i += 1;
        while i < tokens.len() && tokens[i] != ")" {
            if tokens[i] != "," {
                columns.push(tokens[i].clone());
            }
            i += 1;
        }
        i += 1; // skip )
    }

    if tokens.get(i).map(|t| t.to_uppercase()) != Some("VALUES".into()) {
        return Err("Expected VALUES".into());
    }
    i += 1;

    let mut all_values = Vec::new();
    while i < tokens.len() {
        if tokens[i] == "(" {
            i += 1;
            let mut row = Vec::new();
            while i < tokens.len() && tokens[i] != ")" {
                if tokens[i] != "," {
                    row.push(parse_value(&tokens[i]));
                }
                i += 1;
            }
            i += 1; // skip )
            all_values.push(row);
        } else if tokens[i] == "," {
            i += 1;
        } else {
            break;
        }
    }

    Ok(SqlStatement::Insert {
        table,
        columns,
        values: all_values,
    })
}

fn parse_select_sql(tokens: &[String]) -> Result<SqlStatement, String> {
    let mut i = 1;
    let mut columns = Vec::new();

    // Parse select columns
    while i < tokens.len() {
        let upper = tokens[i].to_uppercase();
        if upper == "FROM" {
            break;
        }
        if tokens[i] == "," {
            i += 1;
            continue;
        }
        if tokens[i] == "*" {
            columns.push(SelectColumn::Star);
        } else if upper == "ROW_NUMBER" || upper == "RANK" || upper == "DENSE_RANK" {
            // Window function: ROW_NUMBER() OVER (ORDER BY col [DESC])
            let func = match upper.as_str() {
                "ROW_NUMBER" => WindowFuncType::RowNumber,
                "RANK" => WindowFuncType::Rank,
                "DENSE_RANK" => WindowFuncType::DenseRank,
                _ => unreachable!(),
            };
            i += 1;
            // Skip ( )
            if tokens.get(i).map(|t| t.as_str()) == Some("(") {
                i += 1;
            }
            if tokens.get(i).map(|t| t.as_str()) == Some(")") {
                i += 1;
            }
            // OVER
            if tokens.get(i).map(|t| t.to_uppercase()) == Some("OVER".into()) {
                i += 1;
            }
            // (
            if tokens.get(i).map(|t| t.as_str()) == Some("(") {
                i += 1;
            }
            // ORDER BY
            if tokens.get(i).map(|t| t.to_uppercase()) == Some("ORDER".into()) {
                i += 1;
            }
            if tokens.get(i).map(|t| t.to_uppercase()) == Some("BY".into()) {
                i += 1;
            }
            let order_col = tokens.get(i).cloned().unwrap_or_default();
            i += 1;
            let desc = if tokens.get(i).map(|t| t.to_uppercase()) == Some("DESC".into()) {
                i += 1;
                true
            } else {
                if tokens.get(i).map(|t| t.to_uppercase()) == Some("ASC".into()) {
                    i += 1;
                }
                false
            };
            // )
            if tokens.get(i).map(|t| t.as_str()) == Some(")") {
                i += 1;
            }
            columns.push(SelectColumn::WindowFunc {
                func,
                order_by: order_col,
                desc,
            });
            continue;
        } else if upper == "COUNT"
            || upper == "SUM"
            || upper == "AVG"
            || upper == "MIN"
            || upper == "MAX"
        {
            let func = match upper.as_str() {
                "COUNT" => AggFunc::Count,
                "SUM" => AggFunc::Sum,
                "AVG" => AggFunc::Avg,
                "MIN" => AggFunc::Min,
                "MAX" => AggFunc::Max,
                _ => unreachable!(),
            };
            i += 1; // (
            if tokens.get(i).map(|t| t.as_str()) == Some("(") {
                i += 1;
            }
            let col = tokens.get(i).cloned().unwrap_or_else(|| "*".to_string());
            i += 1;
            if tokens.get(i).map(|t| t.as_str()) == Some(")") {
                i += 1;
            }
            columns.push(SelectColumn::Aggregate(func, col));
            continue;
        } else if tokens[i].contains('.') {
            let parts: Vec<&str> = tokens[i].split('.').collect();
            columns.push(SelectColumn::Qualified(parts[0].into(), parts[1].into()));
        } else {
            columns.push(SelectColumn::Named(tokens[i].clone()));
        }
        i += 1;
    }

    // FROM
    if tokens.get(i).map(|t| t.to_uppercase()) != Some("FROM".into()) {
        return Err("Expected FROM".into());
    }
    i += 1;
    let left_table = tokens.get(i).ok_or("Missing table name")?.clone();
    i += 1;

    // Check for JOIN
    let from = if i < tokens.len() {
        let upper = tokens.get(i).map(|t| t.to_uppercase()).unwrap_or_default();
        if upper == "JOIN" || upper == "INNER" || upper == "LEFT" || upper == "RIGHT" {
            let join_type = match upper.as_str() {
                "LEFT" => {
                    i += 1;
                    JoinType::Left
                }
                "RIGHT" => {
                    i += 1;
                    JoinType::Right
                }
                "INNER" => {
                    i += 1;
                    JoinType::Inner
                }
                _ => JoinType::Inner,
            };
            if tokens.get(i).map(|t| t.to_uppercase()) == Some("JOIN".into()) {
                i += 1;
            }
            let right = tokens.get(i).ok_or("Missing right table")?.clone();
            i += 1;
            if tokens.get(i).map(|t| t.to_uppercase()) != Some("ON".into()) {
                return Err("Expected ON after JOIN table".into());
            }
            i += 1;
            let on_left_full = tokens.get(i).ok_or("Missing join condition")?.clone();
            i += 1; // skip =
            i += 1;
            let on_right_full = tokens.get(i).ok_or("Missing join right col")?.clone();
            i += 1;

            let on_left = on_left_full
                .split('.')
                .next_back()
                .unwrap_or(&on_left_full)
                .to_string();
            let on_right = on_right_full
                .split('.')
                .next_back()
                .unwrap_or(&on_right_full)
                .to_string();

            FromClause::Join {
                left: left_table,
                right,
                join_type,
                on_left,
                on_right,
            }
        } else {
            FromClause::Table(left_table)
        }
    } else {
        FromClause::Table(left_table)
    };

    // WHERE
    let where_clause = if i < tokens.len() && tokens[i].to_uppercase() == "WHERE" {
        i += 1;
        let (expr, new_i) = parse_where_expr(tokens, i)?;
        i = new_i;
        Some(expr)
    } else {
        None
    };

    // GROUP BY
    let mut group_by = Vec::new();
    if i < tokens.len() && tokens[i].to_uppercase() == "GROUP" {
        i += 1;
        if tokens.get(i).map(|t| t.to_uppercase()) == Some("BY".into()) {
            i += 1;
        }
        while i < tokens.len() {
            let upper = tokens[i].to_uppercase();
            if upper == "ORDER" || upper == "LIMIT" || upper == "HAVING" {
                break;
            }
            if tokens[i] != "," {
                group_by.push(tokens[i].clone());
            }
            i += 1;
        }
    }

    // HAVING (post-aggregate filter)
    let having = if i < tokens.len() && tokens[i].to_uppercase() == "HAVING" {
        i += 1;
        let (expr, new_i) = parse_where_expr(tokens, i)?;
        i = new_i;
        Some(expr)
    } else {
        None
    };

    // ORDER BY
    let mut order_by = Vec::new();
    if i < tokens.len() && tokens[i].to_uppercase() == "ORDER" {
        i += 1;
        if tokens.get(i).map(|t| t.to_uppercase()) == Some("BY".into()) {
            i += 1;
        }
        while i < tokens.len() {
            let upper = tokens[i].to_uppercase();
            if upper == "LIMIT" {
                break;
            }
            if tokens[i] == "," {
                i += 1;
                continue;
            }
            let col = tokens[i].clone();
            i += 1;
            let desc = if i < tokens.len() && tokens[i].to_uppercase() == "DESC" {
                i += 1;
                true
            } else {
                if i < tokens.len() && tokens[i].to_uppercase() == "ASC" {
                    i += 1;
                }
                false
            };
            order_by.push(OrderByItem { column: col, desc });
        }
    }

    // LIMIT
    let limit = if i < tokens.len() && tokens[i].to_uppercase() == "LIMIT" {
        i += 1;
        Some(
            tokens
                .get(i)
                .ok_or("Missing LIMIT value")?
                .parse::<usize>()
                .map_err(|_| "Invalid LIMIT")?,
        )
    } else {
        None
    };
    if limit.is_some() {
        i += 1;
    }

    // OFFSET
    let offset = if i < tokens.len() && tokens[i].to_uppercase() == "OFFSET" {
        i += 1;
        Some(
            tokens
                .get(i)
                .ok_or("Missing OFFSET value")?
                .parse::<usize>()
                .map_err(|_| "Invalid OFFSET")?,
        )
    } else {
        None
    };
    let _ = i; // suppress unused warning

    Ok(SqlStatement::Select {
        columns,
        from,
        where_clause,
        group_by,
        having,
        order_by,
        limit,
        offset,
    })
}

fn parse_where_expr(tokens: &[String], start: usize) -> Result<(WhereExpr, usize), String> {
    // OR has LOWEST precedence — this is the top-level entry point.
    // SQL standard: AND binds tighter than OR.
    // `a AND b OR c` = `(a AND b) OR c`, NOT `a AND (b OR c)`
    let (mut left, mut i) = parse_where_and(tokens, start)?;

    while i < tokens.len() {
        let upper = tokens[i].to_uppercase();
        if upper == "OR" {
            i += 1;
            let (right, ni) = parse_where_and(tokens, i)?;
            i = ni;
            left = WhereExpr::Or(Box::new(left), Box::new(right));
        } else {
            break;
        }
    }
    Ok((left, i))
}

fn parse_where_and(tokens: &[String], start: usize) -> Result<(WhereExpr, usize), String> {
    // AND has HIGHER precedence than OR.
    let (mut left, mut i) = parse_where_atom(tokens, start)?;

    while i < tokens.len() {
        let upper = tokens[i].to_uppercase();
        if upper == "AND" {
            i += 1;
            let (right, ni) = parse_where_atom(tokens, i)?;
            i = ni;
            left = WhereExpr::And(Box::new(left), Box::new(right));
        } else {
            break;
        }
    }
    Ok((left, i))
}

fn parse_where_atom(tokens: &[String], start: usize) -> Result<(WhereExpr, usize), String> {
    if start >= tokens.len() {
        return Err("Unexpected end in WHERE".into());
    }

    let mut i = start;
    if tokens[i].to_uppercase() == "NOT" {
        i += 1;
        let (inner, ni) = parse_where_atom(tokens, i)?;
        return Ok((WhereExpr::Not(Box::new(inner)), ni));
    }

    if tokens[i] == "(" {
        i += 1;
        let (expr, ni) = parse_where_expr(tokens, i)?;
        i = ni;
        if i < tokens.len() && tokens[i] == ")" {
            i += 1;
        }
        return Ok((expr, i));
    }

    // Handle aggregate function references in HAVING: COUNT(*), SUM(col), etc.
    let agg_funcs = ["COUNT", "SUM", "AVG", "MIN", "MAX"];
    let col_upper = tokens[i].to_uppercase();
    let (col_name, mut i) = if agg_funcs.contains(&col_upper.as_str())
        && i + 1 < tokens.len()
        && tokens[i + 1] == "("
    {
        // Consume: FUNC ( arg )
        let func = tokens[i].to_lowercase();
        let mut j = i + 2; // skip past '('
        let mut arg = String::from("*");
        if j < tokens.len() && tokens[j] != ")" {
            arg = tokens[j].clone();
            j += 1;
        }
        if j < tokens.len() && tokens[j] == ")" {
            j += 1;
        }
        (format!("{}({})", func, arg), j)
    } else {
        let col = tokens[i].clone();
        let name = col.split('.').next_back().unwrap_or(&col).to_string();
        (name, i + 1)
    };

    if i < tokens.len() && tokens[i].to_uppercase() == "IS" {
        i += 1;
        if i < tokens.len() && tokens[i].to_uppercase() == "NOT" {
            i += 1;
            i += 1; // skip NULL
            return Ok((WhereExpr::IsNotNull(col_name), i));
        } else {
            i += 1; // skip NULL
            return Ok((WhereExpr::IsNull(col_name), i));
        }
    }

    if i < tokens.len() && tokens[i].to_uppercase() == "IN" {
        i += 1;
        if i < tokens.len() && tokens[i] == "(" {
            i += 1;
        }
        // Check if this is a subquery: IN (SELECT ...)
        if i < tokens.len() && tokens[i].to_uppercase() == "SELECT" {
            // Collect all tokens until matching )
            let mut depth = 1;
            let sub_start = i;
            let mut sub_end = i;
            while sub_end < tokens.len() {
                if tokens[sub_end] == "(" {
                    depth += 1;
                }
                if tokens[sub_end] == ")" {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                sub_end += 1;
            }
            let sub_sql = tokens[sub_start..sub_end].join(" ");
            let sub_stmt = parse_sql(&sub_sql)?;
            i = sub_end;
            if i < tokens.len() && tokens[i] == ")" {
                i += 1;
            }
            return Ok((WhereExpr::InSubquery(col_name, Box::new(sub_stmt)), i));
        }
        // Regular IN (val1, val2, ...)
        let mut vals = Vec::new();
        while i < tokens.len() && tokens[i] != ")" {
            if tokens[i] != "," {
                vals.push(parse_value(&tokens[i]));
            }
            i += 1;
        }
        if i < tokens.len() {
            i += 1;
        } // skip )
        return Ok((WhereExpr::In(col_name, vals), i));
    }

    let op = if i >= tokens.len() {
        return Err("Missing operator".into());
    } else {
        match tokens[i].as_str() {
            "=" => CmpOp::Eq,
            "!=" => CmpOp::Ne,
            ">" => CmpOp::Gt,
            "<" => CmpOp::Lt,
            ">=" => CmpOp::Gte,
            "<=" => CmpOp::Lte,
            "LIKE" | "like" => CmpOp::Like,
            other => return Err(format!("Unknown operator: {}", other)),
        }
    };
    i += 1;

    let value = if i >= tokens.len() {
        return Err("Missing value".into());
    } else {
        parse_value(&tokens[i])
    };
    i += 1;

    Ok((
        WhereExpr::Comparison {
            column: col_name,
            op,
            value,
        },
        i,
    ))
}

fn parse_update_sql(tokens: &[String]) -> Result<SqlStatement, String> {
    let mut i = 1;
    let table = tokens.get(i).ok_or("Missing table name")?.clone();
    i += 1;
    if tokens.get(i).map(|t| t.to_uppercase()) != Some("SET".into()) {
        return Err("Expected SET after table name".into());
    }
    i += 1;

    let mut assignments = Vec::new();
    while i < tokens.len() {
        let upper = tokens[i].to_uppercase();
        if upper == "WHERE" {
            break;
        }
        if tokens[i] == "," {
            i += 1;
            continue;
        }
        let col = tokens[i].clone();
        i += 1; // skip =
        i += 1;
        let val = parse_value(tokens.get(i).ok_or("Missing value")?);
        i += 1;
        assignments.push((col, val));
    }

    let where_clause = if i < tokens.len() && tokens[i].to_uppercase() == "WHERE" {
        i += 1;
        let (expr, _) = parse_where_expr(tokens, i)?;
        Some(expr)
    } else {
        None
    };

    Ok(SqlStatement::Update {
        table,
        assignments,
        where_clause,
    })
}

fn parse_delete_sql(tokens: &[String]) -> Result<SqlStatement, String> {
    let mut i = 1;
    if tokens.get(i).map(|t| t.to_uppercase()) != Some("FROM".into()) {
        return Err("Expected FROM after DELETE".into());
    }
    i += 1;
    let table = tokens.get(i).ok_or("Missing table name")?.clone();
    i += 1;

    let where_clause = if i < tokens.len() && tokens[i].to_uppercase() == "WHERE" {
        i += 1;
        let (expr, _) = parse_where_expr(tokens, i)?;
        Some(expr)
    } else {
        None
    };

    Ok(SqlStatement::Delete {
        table,
        where_clause,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table() {
        let stmt = parse_sql(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT)",
        )
        .unwrap();
        match stmt {
            SqlStatement::CreateTable { name, columns, .. } => {
                assert_eq!(name, "users");
                assert_eq!(columns.len(), 3);
                assert!(columns[0].primary_key);
                assert!(!columns[1].nullable);
            }
            _ => panic!("Expected CreateTable"),
        }
    }

    #[test]
    fn test_insert() {
        let stmt = parse_sql("INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
        match stmt {
            SqlStatement::Insert {
                table,
                columns,
                values,
            } => {
                assert_eq!(table, "users");
                assert_eq!(columns, vec!["id", "name"]);
                assert_eq!(values[0][0], SqlValue::Integer(1));
            }
            _ => panic!("Expected Insert"),
        }
    }

    #[test]
    fn test_select_join() {
        let stmt = parse_sql("SELECT * FROM users JOIN orders ON users.id = orders.user_id WHERE users.name = 'Alice'").unwrap();
        match stmt {
            SqlStatement::Select {
                from: FromClause::Join { left, right, .. },
                ..
            } => {
                assert_eq!(left, "users");
                assert_eq!(right, "orders");
            }
            _ => panic!("Expected Select with Join"),
        }
    }

    #[test]
    fn test_where_or() {
        let stmt = parse_sql("SELECT * FROM users WHERE name = 'Alice' OR name = 'Bob'").unwrap();
        match stmt {
            SqlStatement::Select {
                where_clause: Some(WhereExpr::Or(..)),
                ..
            } => {}
            _ => panic!("Expected OR clause"),
        }
    }

    #[test]
    fn test_aggregates() {
        let stmt = parse_sql("SELECT COUNT(*), SUM(amount) FROM orders GROUP BY user_id").unwrap();
        match stmt {
            SqlStatement::Select {
                columns, group_by, ..
            } => {
                assert_eq!(columns.len(), 2);
                assert_eq!(group_by, vec!["user_id"]);
            }
            _ => panic!("Expected Select"),
        }
    }
}
