#[derive(Debug, PartialEq, Clone)]
pub enum Operator {
    Eq,
    Gte,
    Lte,
}

#[derive(Debug, PartialEq)]
pub enum Condition {
    Key(Operator, String),
}

#[derive(Debug, PartialEq)]
pub enum Action {
    SelectAll,
    SelectCount,
    Delete,
    Insert(String, String),
    Update(String, String),
}

#[derive(Debug)]
pub struct Query {
    pub action: Action,
    pub conditions: Vec<Condition>,
    pub limit: Option<usize>,
    pub order_desc: bool,
}

pub fn parse_query(q: &str) -> Result<Query, String> {
    let tokens: Vec<&str> = q.split_whitespace().collect();
    if tokens.is_empty() { return Err("Empty query".into()); }
    
    match tokens[0].to_uppercase().as_str() {
        "SELECT" => parse_select(&tokens),
        "DELETE" => parse_delete(&tokens),
        "INSERT" => parse_insert(&tokens),
        "UPDATE" => parse_update(&tokens),
        _ => Err(format!("Unsupported command: {}", tokens[0])),
    }
}

fn parse_select(tokens: &[&str]) -> Result<Query, String> {
    if tokens.len() < 2 { return Err("Expected * or COUNT after SELECT".into()); }
    
    let action = if tokens[1] == "*" {
        Action::SelectAll
    } else if tokens[1].to_uppercase() == "COUNT" {
        Action::SelectCount
    } else {
        return Err("Expected * or COUNT after SELECT".into());
    };

    let mut conditions = Vec::new();
    let mut limit = None;
    let mut order_desc = false;
    let mut i = 2;

    // Parse WHERE clause
    if i < tokens.len() && tokens[i].to_uppercase() == "WHERE" {
        i += 1;
        while i < tokens.len() {
            let upper = tokens[i].to_uppercase();
            if upper == "AND" { i += 1; continue; }
            if upper == "LIMIT" || upper == "ORDER" { break; }
            if tokens[i] != "key" { return Err("Can only filter on 'key'".into()); }
            
            if i + 2 >= tokens.len() { return Err("Incomplete condition".into()); }
            
            let op = match tokens[i+1] {
                "=" => Operator::Eq,
                ">=" => Operator::Gte,
                "<=" => Operator::Lte,
                _ => return Err(format!("Unsupported operator {}", tokens[i+1])),
            };
            
            let val = tokens[i+2].to_string();
            conditions.push(Condition::Key(op, val));
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
                i += 1; // default
            }
        } else {
            return Err("Expected BY after ORDER".into());
        }
    }

    // Parse LIMIT
    if i < tokens.len() && tokens[i].to_uppercase() == "LIMIT" {
        i += 1;
        if i >= tokens.len() { return Err("Expected number after LIMIT".into()); }
        limit = Some(tokens[i].parse::<usize>().map_err(|_| format!("Invalid LIMIT value: {}", tokens[i]))?);
    }

    Ok(Query { action, conditions, limit, order_desc })
}

fn parse_delete(tokens: &[&str]) -> Result<Query, String> {
    // DELETE WHERE key = 100
    // DELETE WHERE key >= 100 AND key <= 200
    let mut conditions = Vec::new();
    let mut i = 1;

    if i >= tokens.len() || tokens[i].to_uppercase() != "WHERE" {
        return Err("DELETE requires a WHERE clause".into());
    }
    i += 1;

    while i < tokens.len() {
        let upper = tokens[i].to_uppercase();
        if upper == "AND" { i += 1; continue; }
        if tokens[i] != "key" { return Err("Can only filter on 'key'".into()); }
        if i + 2 >= tokens.len() { return Err("Incomplete condition".into()); }
        
        let op = match tokens[i+1] {
            "=" => Operator::Eq,
            ">=" => Operator::Gte,
            "<=" => Operator::Lte,
            _ => return Err(format!("Unsupported operator {}", tokens[i+1])),
        };
        let val = tokens[i+2].to_string();
        conditions.push(Condition::Key(op, val));
        i += 3;
    }

    if conditions.is_empty() { return Err("DELETE WHERE requires at least one condition".into()); }
    Ok(Query { action: Action::Delete, conditions, limit: None, order_desc: false })
}

fn parse_insert(tokens: &[&str]) -> Result<Query, String> {
    // INSERT <key> <value...>
    if tokens.len() < 3 { return Err("INSERT requires <key> <value>".into()); }
    let key = tokens[1].to_string();
    let value = tokens[2..].join(" ");
    Ok(Query { action: Action::Insert(key, value), conditions: Vec::new(), limit: None, order_desc: false })
}

fn parse_update(tokens: &[&str]) -> Result<Query, String> {
    // UPDATE SET value = <val...> WHERE key = <key>
    if tokens.len() < 8 { return Err("UPDATE requires SET value = <val> WHERE key = <key>".into()); }
    if tokens[1].to_uppercase() != "SET" || tokens[2].to_uppercase() != "VALUE" || tokens[3] != "=" {
        return Err("Expected SET value =".into());
    }
    
    let mut i = 4;
    let mut value_tokens = Vec::new();
    while i < tokens.len() && tokens[i].to_uppercase() != "WHERE" {
        value_tokens.push(tokens[i]);
        i += 1;
    }
    
    if value_tokens.is_empty() { return Err("Missing value in UPDATE".into()); }
    let value = value_tokens.join(" ");
    
    if i >= tokens.len() || tokens[i].to_uppercase() != "WHERE" {
        return Err("UPDATE requires WHERE clause".into());
    }
    i += 1;
    
    if i + 2 >= tokens.len() || tokens[i].to_uppercase() != "KEY" || tokens[i+1] != "=" {
        return Err("UPDATE WHERE must be exactly: WHERE key = <key>".into());
    }
    
    let key = tokens[i+2].to_string();
    Ok(Query { action: Action::Update(key, value), conditions: Vec::new(), limit: None, order_desc: false })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_select_all() {
        let q = parse_query("SELECT * WHERE key >= 100 AND key <= 200").expect("unwrap failed");
        assert_eq!(q.action, Action::SelectAll);
        assert_eq!(q.conditions.len(), 2);
        assert_eq!(q.limit, None);
        assert!(!q.order_desc);
    }

    #[test]
    fn test_select_count() {
        let q = parse_query("SELECT COUNT WHERE key = 42").expect("unwrap failed");
        assert_eq!(q.action, Action::SelectCount);
        assert_eq!(q.conditions.len(), 1);
    }

    #[test]
    fn test_select_limit_order() {
        let q = parse_query("SELECT * WHERE key >= 0 ORDER BY DESC LIMIT 10").expect("unwrap failed");
        assert_eq!(q.action, Action::SelectAll);
        assert!(q.order_desc);
        assert_eq!(q.limit, Some(10));
    }

    #[test]
    fn test_delete() {
        let q = parse_query("DELETE WHERE key = 100").expect("unwrap failed");
        assert_eq!(q.action, Action::Delete);
        assert_eq!(q.conditions.len(), 1);
    }

    #[test]
    fn test_insert() {
        let q = parse_query("INSERT 42 Hello World").expect("unwrap failed");
        assert_eq!(q.action, Action::Insert("42".to_string(), "Hello World".to_string()));
    }

    #[test]
    fn test_delete_requires_where() {
        assert!(parse_query("DELETE").is_err());
    }

    #[test]
    fn test_insert_requires_args() {
        assert!(parse_query("INSERT").is_err());
        assert!(parse_query("INSERT 42").is_err());
    }

    #[test]
    fn test_update() {
        let q = parse_query("UPDATE SET value = new data WHERE key = 123").expect("unwrap failed");
        assert_eq!(q.action, Action::Update("123".to_string(), "new data".to_string()));
    }

    #[test]
    fn test_update_invalid() {
        assert!(parse_query("UPDATE SET value = 1").is_err());
        assert!(parse_query("UPDATE WHERE key = 1").is_err());
    }
}
