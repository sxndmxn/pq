use crate::model::ColumnInfo;

pub fn print_csv(columns: &[ColumnInfo], include_header: bool) {
    if include_header {
        println!("column,type,nullable");
    }

    for column in columns {
        println!(
            "{},{},{}",
            escape_csv(&column.name),
            escape_csv(&column.type_name),
            column.nullable
        );
    }
}

fn escape_csv(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}
