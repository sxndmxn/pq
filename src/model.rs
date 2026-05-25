use serde::Serialize;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub type_name: String,
    pub nullable: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FileInfo {
    pub file: String,
    pub file_size_bytes: u64,
    pub num_rows: i64,
    pub num_columns: usize,
    pub num_row_groups: usize,
    pub compression: String,
    pub created_by: String,
    pub version: i32,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ColumnStats {
    pub column: String,
    #[serde(rename = "type")]
    pub dtype: String,
    pub null_count: u64,
    pub min: Option<String>,
    pub max: Option<String>,
}
