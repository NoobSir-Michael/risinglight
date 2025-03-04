// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

//! The parser module directly uses the [`sqlparser`] crate
//! and re-exports its AST types.

pub use sqlparser::ast::*;
use sqlparser::dialect::MySqlDialect;
use sqlparser::parser::Parser;
pub use sqlparser::parser::ParserError;

/// Parse the SQL string into a list of ASTs.
pub fn parse(sql: &str) -> Result<Vec<Statement>, ParserError> {
    let dialect = MySqlDialect{};
    Parser::parse_sql(&dialect, sql)
}
