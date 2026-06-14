//! SQL-to-QueryPredicate bridge.
//!
//! Parses SQL WHERE clauses and converts them into Strata's `QueryPredicate`,
//! enabling SQL-driven queries with MARS range pruning support.
//!
//! # Supported SQL
//!
//! ```sql
//! SELECT * FROM trips WHERE status = 'ERROR' AND fare >= 100 AND fare <= 500
//! SELECT * FROM trips WHERE fare BETWEEN 50 AND 500
//! SELECT * FROM trips WHERE status IN ('OK', 'WARN') AND fare >= 100
//! SELECT COUNT(*), SUM(fare), AVG(fare) FROM trips WHERE fare >= 100
//! ```
//!
//! The bridge automatically detects:
//! - **Equality / IN** → `add_filter()` (hash routing for categoricals)
//! - **>=, >, <, <=** on numeric columns → `add_range_filter()` (MARS routing)
//! - **BETWEEN** → `add_range_filter()` (MARS routing)
//! - **COUNT/SUM/AVG/MIN/MAX** → metadata-only aggregation (no row scanning)

use crate::storage::reader::QueryPredicate;
use anyhow::{Context, Result};
use sqlparser::ast::{BinaryOperator, Expr as SqlExpr, Statement, Value as SqlValue};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SqlParser;

/// Result of parsing a SQL query.
#[derive(Debug)]
pub struct ParsedQuery {
    pub table: String,
    pub predicate: QueryPredicate,
    pub limit: Option<u64>,
    /// Requested aggregations, e.g. SELECT COUNT(*), SUM(fare) → [(Count, "*"), (Sum, "fare")]
    pub aggregations: Vec<AggregationRequest>,
}

/// A requested aggregation function and column.
#[derive(Debug, Clone)]
pub struct AggregationRequest {
    pub func: crate::storage::reader::AggFunc,
    pub column: String,
}

/// Parse a SQL query string and extract a QueryPredicate from the WHERE clause.
///
/// Also returns the table name, optional LIMIT, and any aggregation functions.
pub fn parse_query(sql: &str) -> Result<ParsedQuery> {
    let dialect = GenericDialect {};
    let statements = SqlParser::parse_sql(&dialect, sql).context("Failed to parse SQL")?;

    if statements.is_empty() {
        anyhow::bail!("Empty SQL statement");
    }

    match &statements[0] {
        Statement::Query(query) => {
            let body = &query.body;
            let select = match body.as_ref() {
                sqlparser::ast::SetExpr::Select(s) => s,
                _ => anyhow::bail!("Only SELECT statements are supported"),
            };

            // Extract table name
            let table = if let Some(twj) = select.from.first() {
                extract_table_name(&twj.relation)
            } else {
                "unknown".to_string()
            };

            // Extract LIMIT
            let limit = query.limit.as_ref().and_then(|l| match l {
                sqlparser::ast::Expr::Value(SqlValue::Number(n, _)) => n.parse::<u64>().ok(),
                _ => None,
            });

            // Extract aggregations from SELECT list
            let aggregations = extract_aggregations(select)?;

            // Extract WHERE into QueryPredicate
            let predicate = if let Some(where_expr) = &select.selection {
                expr_to_predicate(where_expr)?
            } else {
                QueryPredicate::new()
            };

            Ok(ParsedQuery {
                table,
                predicate,
                limit,
                aggregations,
            })
        }
        _ => anyhow::bail!("Only SELECT queries are supported"),
    }
}

/// Extract aggregation functions from the SELECT list.
fn extract_aggregations(select: &sqlparser::ast::Select) -> Result<Vec<AggregationRequest>> {
    let mut aggs = Vec::new();

    for item in &select.projection {
        match item {
            sqlparser::ast::SelectItem::UnnamedExpr(expr) => {
                if let Some((func, col)) = parse_agg_expr(expr) {
                    aggs.push(AggregationRequest { func, column: col });
                }
            }
            sqlparser::ast::SelectItem::ExprWithAlias { expr, .. } => {
                if let Some((func, col)) = parse_agg_expr(expr) {
                    aggs.push(AggregationRequest { func, column: col });
                }
            }
            sqlparser::ast::SelectItem::Wildcard(_) => {
                // SELECT *: no aggregation
            }
            _ => {}
        }
    }

    Ok(aggs)
}

/// Parse a single aggregation expression like COUNT(*), SUM(fare), AVG(amount), MIN(x), MAX(x).
fn parse_agg_expr(expr: &SqlExpr) -> Option<(crate::storage::reader::AggFunc, String)> {
    use crate::storage::reader::AggFunc;

    if let SqlExpr::Function(func) = expr {
        let name = func.name.to_string().to_uppercase();
        let agg_func = match name.as_str() {
            "COUNT" => AggFunc::Count,
            "SUM" => AggFunc::Sum,
            "AVG" => AggFunc::Avg,
            "MIN" => AggFunc::Min,
            "MAX" => AggFunc::Max,
            _ => return None,
        };

        // Extract the argument (column name or *)
        let args = match &func.args {
            sqlparser::ast::FunctionArguments::List(list) => &list.args,
            _ => return None,
        };
        if let Some(arg) = args.first() {
            let col = match arg {
                sqlparser::ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                    sqlparser::ast::FunctionArgExpr::Expr(e) => {
                        extract_identifier(e).unwrap_or("*".to_string())
                    }
                    sqlparser::ast::FunctionArgExpr::Wildcard => "*".to_string(),
                    _ => "*".to_string(),
                },
                _ => "*".to_string(),
            };
            return Some((agg_func, col));
        }
    }
    None
}

/// Convert a SQL expression into a QueryPredicate.
///
/// Handles AND by merging multiple filters, and converts comparison operators
/// into the appropriate exact-match or range filters.
fn expr_to_predicate(expr: &SqlExpr) -> Result<QueryPredicate> {
    let mut predicate = QueryPredicate::new();
    collect_filters(expr, &mut predicate)?;
    Ok(predicate)
}

/// Recursively collect filters from a SQL expression tree.
fn collect_filters(expr: &SqlExpr, predicate: &mut QueryPredicate) -> Result<()> {
    match expr {
        SqlExpr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                collect_filters(left, predicate)?;
                collect_filters(right, predicate)?;
            }
            BinaryOperator::Eq => {
                let (col, val) = extract_col_value(left, right)?;
                predicate.add_filter(col, vec![val]);
            }
            BinaryOperator::GtEq => {
                let (col, val) = extract_col_value(left, right)?;
                add_numeric_min(predicate, &col, &val);
            }
            BinaryOperator::Gt => {
                let (col, val) = extract_col_value(left, right)?;
                add_numeric_min(predicate, &col, &val);
            }
            BinaryOperator::LtEq => {
                let (col, val) = extract_col_value(left, right)?;
                add_numeric_max(predicate, &col, &val);
            }
            BinaryOperator::Lt => {
                let (col, val) = extract_col_value(left, right)?;
                add_numeric_max(predicate, &col, &val);
            }
            _ => {}
        },
        SqlExpr::InList { expr, list, .. } => {
            if let SqlExpr::Identifier(ident) = expr.as_ref() {
                let col = ident.value.to_string();
                let values: Vec<String> = list.iter().filter_map(extract_string_value).collect();
                if !values.is_empty() {
                    predicate.add_filter(col, values);
                }
            }
        }
        SqlExpr::Between {
            expr, low, high, ..
        } => {
            if let SqlExpr::Identifier(ident) = expr.as_ref() {
                let col = ident.value.to_string();
                if let (Some(lo), Some(hi)) =
                    (extract_string_value(low), extract_string_value(high))
                {
                    predicate.add_range_filter(
                        col,
                        lo.parse::<f64>().unwrap_or(f64::MIN),
                        hi.parse::<f64>().unwrap_or(f64::MAX),
                    );
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn add_numeric_min(predicate: &mut QueryPredicate, col: &str, val: &str) {
    let v = val.parse::<f64>().unwrap_or(f64::MIN);
    predicate.add_min_filter(col.to_string(), v);
}

fn add_numeric_max(predicate: &mut QueryPredicate, col: &str, val: &str) {
    let v = val.parse::<f64>().unwrap_or(f64::MAX);
    predicate.add_max_filter(col.to_string(), v);
}

fn extract_col_value(left: &SqlExpr, right: &SqlExpr) -> Result<(String, String)> {
    match (extract_identifier(left), extract_string_value(right)) {
        (Some(col), Some(val)) => Ok((col, val)),
        _ => match (extract_identifier(right), extract_string_value(left)) {
            (Some(col), Some(val)) => Ok((col, val)),
            _ => anyhow::bail!("Could not extract column=value from expression"),
        },
    }
}

fn extract_identifier(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Identifier(ident) => Some(ident.value.to_string()),
        _ => None,
    }
}

fn extract_string_value(expr: &SqlExpr) -> Option<String> {
    match expr {
        SqlExpr::Value(v) => match v {
            SqlValue::SingleQuotedString(s) => Some(s.clone()),
            SqlValue::DoubleQuotedString(s) => Some(s.clone()),
            SqlValue::Number(n, _) => Some(n.clone()),
            _ => None,
        },
        SqlExpr::UnaryOp { op, expr } => {
            if matches!(op, sqlparser::ast::UnaryOperator::Minus) {
                extract_string_value(expr).map(|s| format!("-{}", s))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn extract_table_name(table: &sqlparser::ast::TableFactor) -> String {
    match table {
        sqlparser::ast::TableFactor::Table { name, .. } => name
            .0
            .iter()
            .map(|i| i.value.as_str())
            .collect::<Vec<_>>()
            .join("."),
        _ => "unknown".to_string(),
    }
}
