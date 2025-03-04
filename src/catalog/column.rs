// Copyright 2023 RisingLight Project Authors. Licensed under Apache-2.0.

use pretty_xmlish::Pretty;
use serde::{Deserialize, Serialize};

use super::ColumnId;
use crate::types::DataType;

/// A descriptor of a column.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ColumnDesc {
    datatype: DataType,
    name: String,
    is_primary: bool,
    is_required: bool,
}

impl ColumnDesc {
    pub const fn new(datatype: DataType, name: String, is_primary: bool, is_required: bool) -> Self {
        ColumnDesc {
            datatype,
            name,
            is_primary,
            is_required,
        }
    }

    pub fn set_is_required(&mut self, is_required: bool){
        self.is_required = is_required;
    }

    pub fn is_required(& self) -> bool{
        self.is_required
    }

    pub fn set_primary(&mut self, is_primary: bool) {
        self.is_primary = is_primary;
    }

    pub fn is_primary(&self) -> bool {
        self.is_primary
    }

    pub fn set_nullable(&mut self, is_nullable: bool) {
        self.datatype.nullable = is_nullable;
    }

    pub fn is_nullable(&self) -> bool {
        self.datatype.nullable
    }

    pub fn datatype(&self) -> &DataType {
        &self.datatype
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn pretty<'a>(&self) -> Pretty<'a> {
        let mut fields = vec![
            ("name", Pretty::display(&self.name)),
            ("type", Pretty::display(&self.datatype.kind)),
        ];
        if self.is_primary {
            fields.push(("primary", Pretty::display(&self.is_primary)));
        }
        if self.datatype.nullable {
            fields.push(("nullable", Pretty::display(&self.datatype.nullable)));
        }
        if self.is_required {
            fields.push(("required", Pretty::display(&self.is_required)));
        }
        Pretty::childless_record("Column", fields)
    }
}

impl DataType {
    pub const fn to_column(self, name: String, required:bool) -> ColumnDesc {
        ColumnDesc::new(self, name, false, required)
    }

    pub const fn to_column_primary_key(self, name: String, required:bool) -> ColumnDesc {
        ColumnDesc::new(self, name, true, required)
    }
}

/// The catalog of a column.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ColumnCatalog {
    id: ColumnId,
    desc: ColumnDesc,
}

impl ColumnCatalog {
    pub fn new(id: ColumnId, desc: ColumnDesc) -> ColumnCatalog {
        ColumnCatalog { id, desc }
    }

    pub fn id(&self) -> ColumnId {
        self.id
    }

    pub fn set_id(&mut self, id: ColumnId) {
        self.id = id
    }

    pub fn name(&self) -> &str {
        &self.desc.name
    }

    pub(crate) fn into_name(self) -> String {
        self.desc.name
    }

    pub fn desc(&self) -> &ColumnDesc {
        &self.desc
    }

    pub fn datatype(&self) -> DataType {
        self.desc.datatype.clone()
    }

    pub fn set_primary(&mut self, is_primary: bool) {
        self.desc.set_primary(is_primary);
    }

    pub fn is_primary(&self) -> bool {
        self.desc.is_primary()
    }

    pub fn set_nullable(&mut self, is_nullable: bool) {
        self.desc.set_nullable(is_nullable);
    }

    pub fn is_nullable(&self) -> bool {
        self.desc.is_nullable()
    }

    pub fn is_required(&self) -> bool {
        self.desc.is_required()
    }
}

/// Find the id of the sort key among column catalogs
pub fn find_sort_key_id(column_infos: &[ColumnCatalog]) -> Option<usize> {
    let mut key = None;
    for (id, column_info) in column_infos.iter().enumerate() {
        if column_info.is_primary() {
            if key.is_some() {
                panic!("only one primary key is supported");
            }
            key = Some(id);
        }
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataTypeKind;

    #[test]
    fn test_column_catalog() {
        let col_desc = DataTypeKind::Int32.not_null().to_column("grade".into(), false);
        let mut col_catalog = ColumnCatalog::new(0, col_desc);
        assert_eq!(col_catalog.id(), 0);
        assert!(!col_catalog.is_primary());
        assert!(!col_catalog.is_nullable());
        assert_eq!(col_catalog.name(), "grade");
        col_catalog.set_primary(true);
        assert!(col_catalog.is_primary());
    }
}
