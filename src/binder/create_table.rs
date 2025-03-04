use std::collections::HashSet;
use std::fmt;
use std::str::FromStr;

use pretty_xmlish::helper::delegate_fmt;
use pretty_xmlish::Pretty;
use serde::{Deserialize, Serialize};

use super::*;
use crate::catalog::{ColumnCatalog, ColumnDesc, ColumnId, SchemaId};
use crate::types::DataType;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Serialize, Deserialize)]
pub struct CreateTable {
    pub schema_id: SchemaId,
    pub table_name: String,
    pub columns: Vec<ColumnCatalog>,
    pub ordered_pk_ids: Vec<ColumnId>,
}

impl fmt::Display for CreateTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let explainer = Pretty::childless_record("CreateTable", self.pretty_table());
        delegate_fmt(&explainer, f, String::with_capacity(1000))
    }
}

impl CreateTable {
    pub fn pretty_table<'a>(&self) -> Vec<(&'a str, Pretty<'a>)> {
        let cols = Pretty::Array(self.columns.iter().map(|c| c.desc().pretty()).collect());
        let ids = Pretty::Array(self.ordered_pk_ids.iter().map(Pretty::display).collect());
        vec![
            ("schema_id", Pretty::display(&self.schema_id)),
            ("name", Pretty::display(&self.table_name)),
            ("columns", cols),
            ("ordered_ids", ids),
        ]
    }
}

impl FromStr for CreateTable {
    type Err = ();

    fn from_str(_s: &str) -> std::result::Result<Self, Self::Err> {
        Err(())
    }
}

impl Binder {
    pub(super) fn bind_create_table(
        &mut self,
        name: ObjectName,
        columns: &[ColumnDef],
        constraints: &[TableConstraint],
    ) -> Result {
        let name = lower_case_name(&name);
        let (schema_name, table_name) = split_name(&name)?;
        let schema = self
            .catalog
            .get_schema_by_name(schema_name)
            .ok_or_else(|| BindError::InvalidSchema(schema_name.into()))?;
        if schema.get_table_by_name(table_name).is_some() {
            return Err(BindError::DuplicatedTable(table_name.into()));
        }

        // check duplicated column names
        let mut set = HashSet::new();
        for col in columns.iter() {
            if !set.insert(col.name.value.to_lowercase()) {
                return Err(BindError::DuplicatedColumn(col.name.value.clone()));
            }
        }

        let mut ordered_pk_ids = Binder::ordered_pks_from_columns(columns);
        let has_pk_from_column = !ordered_pk_ids.is_empty();

        if ordered_pk_ids.len() > 1 {
            // multi primary key should be declared by "primary key(c1, c2...)" syntax
            return Err(BindError::NotSupportedTSQL);
        }

        let pks_name_from_constraints = Binder::pks_name_from_constraints(constraints);
        if has_pk_from_column && !pks_name_from_constraints.is_empty() {
            // can't get primary key both from "primary key(c1, c2...)" syntax and
            // column's option
            return Err(BindError::NotSupportedTSQL);
        } else if !has_pk_from_column {
            for name in &pks_name_from_constraints {
                if !set.contains(name) {
                    return Err(BindError::InvalidColumn(name.clone()));
                }
            }
            // We have used `pks_name_from_constraints` to get the primary keys' name sorted by
            // declaration order in "primary key(c1, c2..)" syntax. Now we transfer the name to id
            // to get the sorted ID
            ordered_pk_ids = pks_name_from_constraints
                .iter()
                .map(|name| {
                    columns
                        .iter()
                        .position(|c| c.name.value.eq_ignore_ascii_case(name))
                        .unwrap() as ColumnId
                })
                .collect();
        }

        let mut columns: Vec<ColumnCatalog> = columns
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let mut col = ColumnCatalog::from(col);
                col.set_id(idx as ColumnId);
                col
            })
            .collect();

        for &index in &ordered_pk_ids {
            columns[index as usize].set_primary(true);
            columns[index as usize].set_nullable(false);
        }

        let create = self.egraph.add(Node::CreateTable(CreateTable {
            schema_id: schema.id(),
            table_name: table_name.into(),
            columns,
            ordered_pk_ids,
        }));
        Ok(create)
    }

    /// get primary keys' id in declared order。
    /// we use index in columns vector as column id
    fn ordered_pks_from_columns(columns: &[ColumnDef]) -> Vec<ColumnId> {
        let mut ordered_pks = Vec::new();

        for (index, col_def) in columns.iter().enumerate() {
            for option_def in &col_def.options {
                let is_primary_ = if let ColumnOption::Unique { is_primary } = option_def.option {
                    is_primary
                } else {
                    false
                };
                if is_primary_ {
                    ordered_pks.push(index as ColumnId);
                }
            }
        }
        ordered_pks
    }

    /// get the primary keys' name sorted by declaration order in "primary key(c1, c2..)" syntax.
    fn pks_name_from_constraints(constraints: &[TableConstraint]) -> Vec<String> {
        let mut pks_name_from_constraints = vec![];

        for constraint in constraints {
            match constraint {
                TableConstraint::Unique {
                    is_primary,
                    columns,
                    ..
                } if *is_primary => columns.iter().for_each(|ident| {
                    pks_name_from_constraints.push(ident.value.to_lowercase());
                }),
                _ => continue,
            }
        }
        pks_name_from_constraints
    }
}

impl From<&ColumnDef> for ColumnCatalog {
    fn from(cdef: &ColumnDef) -> Self {
        let mut is_nullable = true;
        let mut is_primary_ = false;
        let mut is_required = false;
        for opt in &cdef.options {
            match &opt.option {
                ColumnOption::Null => is_nullable = true,
                ColumnOption::NotNull => is_nullable = false,
                ColumnOption::Unique { is_primary } => is_primary_ = *is_primary,
                ColumnOption::Comment(comment) => is_required = comment.eq(&String::from("required")),
                _ => todo!("column options"),
            }
        }
        ColumnCatalog::new(
            0,
            ColumnDesc::new(
                DataType::new((&cdef.data_type).into(), is_nullable),
                cdef.name.value.to_lowercase(),
                is_primary_,
                is_required
            ),
        )
    }
}
