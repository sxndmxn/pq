//! Column statistics command

use crate::cli::args::{OutputFormat, StatsArgs};
use crate::dataset::Dataset;
use crate::error::{PqError, ResultExt};
use crate::Result;
use comfy_table::{Cell, Table};
use parquet::data_type::Int96;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::statistics::Statistics;
use serde::Serialize;
use std::fs::File;

/// Column statistics data
struct ColumnStats {
    name: String,
    physical_type: String,
    null_count: u64,
    min: Option<StatValue>,
    max: Option<StatValue>,
}

#[derive(Clone, Debug, PartialEq)]
enum StatValue {
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    ByteArray(Vec<u8>),
    Boolean(bool),
    FixedLenByteArray(Vec<u8>),
    Int96(Int96),
}

impl StatValue {
    fn display(&self) -> String {
        match self {
            Self::Int32(value) => value.to_string(),
            Self::Int64(value) => value.to_string(),
            Self::Float(value) => value.to_string(),
            Self::Double(value) => value.to_string(),
            Self::ByteArray(value) | Self::FixedLenByteArray(value) => {
                String::from_utf8_lossy(value).to_string()
            }
            Self::Boolean(value) => value.to_string(),
            Self::Int96(value) => format!("{value:?}"),
        }
    }

    fn partial_compare(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Self::Int32(left), Self::Int32(right)) => left.partial_cmp(right),
            (Self::Int64(left), Self::Int64(right)) => left.partial_cmp(right),
            (Self::Float(left), Self::Float(right)) => left.partial_cmp(right),
            (Self::Double(left), Self::Double(right)) => left.partial_cmp(right),
            (Self::ByteArray(left), Self::ByteArray(right)) => left.partial_cmp(right),
            (Self::Boolean(left), Self::Boolean(right)) => left.partial_cmp(right),
            (Self::FixedLenByteArray(left), Self::FixedLenByteArray(right)) => {
                left.partial_cmp(right)
            }
            (Self::Int96(left), Self::Int96(right)) => left.partial_cmp(right),
            _ => None,
        }
    }
}

/// Serializable representation for JSON/CSV output
#[derive(Serialize)]
struct StatRow {
    column: String,
    #[serde(rename = "type")]
    dtype: String,
    null_count: u64,
    min: Option<String>,
    max: Option<String>,
}

pub fn run(args: StatsArgs) -> Result<()> {
    let dataset = Dataset::from_inputs(args.inputs)?;

    for source in dataset.sources() {
        let path = source.path();
        if dataset.is_multi_source() && !args.quiet {
            println!("==> {} <==", path.display());
        }

        let file = File::open(path).with_path_context(path)?;
        let reader = SerializedFileReader::new(file).map_err(|e| {
            let msg = e.to_string().to_lowercase();
            if msg.contains("magic") || msg.contains("not a valid parquet") {
                PqError::invalid_parquet(path, &e)
            } else if msg.contains("eof") || msg.contains("truncat") {
                PqError::corrupted(path, &e)
            } else {
                PqError::read_error(path, &e)
            }
        })?;
        let metadata = reader.metadata();
        let schema = metadata.file_metadata().schema_descr();

        // Collect stats per column across all row groups
        let num_columns = schema.num_columns();
        let mut column_stats: Vec<ColumnStats> = (0..num_columns)
            .map(|i| ColumnStats {
                name: schema.column(i).name().to_string(),
                physical_type: format!("{:?}", schema.column(i).physical_type()),
                null_count: 0,
                min: None,
                max: None,
            })
            .collect();

        // Aggregate stats from all row groups
        for rg_idx in 0..metadata.num_row_groups() {
            let rg = metadata.row_group(rg_idx);
            for (col_idx, cs) in column_stats.iter_mut().enumerate().take(rg.num_columns()) {
                if let Some(stats) = rg.column(col_idx).statistics() {
                    cs.null_count += stats.null_count_opt().unwrap_or(0);

                    // Update min/max (format as strings for display)
                    update_min_max(cs, stats);
                }
            }
        }

        // Filter by column name if specified
        let stats_to_show: Vec<_> = if let Some(col_name) = args.column.as_deref() {
            column_stats
                .into_iter()
                .filter(|s| s.name == col_name)
                .collect()
        } else {
            column_stats
        };

        // Output based on format
        output_stats(&stats_to_show, args.output, args.quiet)?;
    }
    Ok(())
}

/// Update min/max values from statistics based on physical type
fn update_min_max(cs: &mut ColumnStats, stats: &Statistics) {
    match stats {
        Statistics::Int32(s) => {
            merge_min(&mut cs.min, s.min_opt().copied().map(StatValue::Int32));
            merge_max(&mut cs.max, s.max_opt().copied().map(StatValue::Int32));
        }
        Statistics::Int64(s) => {
            merge_min(&mut cs.min, s.min_opt().copied().map(StatValue::Int64));
            merge_max(&mut cs.max, s.max_opt().copied().map(StatValue::Int64));
        }
        Statistics::Float(s) => {
            merge_min(&mut cs.min, s.min_opt().copied().map(StatValue::Float));
            merge_max(&mut cs.max, s.max_opt().copied().map(StatValue::Float));
        }
        Statistics::Double(s) => {
            merge_min(&mut cs.min, s.min_opt().copied().map(StatValue::Double));
            merge_max(&mut cs.max, s.max_opt().copied().map(StatValue::Double));
        }
        Statistics::ByteArray(s) => {
            merge_min(
                &mut cs.min,
                s.min_opt()
                    .map(|min| StatValue::ByteArray(min.data().to_vec())),
            );
            merge_max(
                &mut cs.max,
                s.max_opt()
                    .map(|max| StatValue::ByteArray(max.data().to_vec())),
            );
        }
        Statistics::Boolean(s) => {
            merge_min(&mut cs.min, s.min_opt().copied().map(StatValue::Boolean));
            merge_max(&mut cs.max, s.max_opt().copied().map(StatValue::Boolean));
        }
        Statistics::FixedLenByteArray(s) => {
            merge_min(
                &mut cs.min,
                s.min_opt()
                    .map(|min| StatValue::FixedLenByteArray(min.data().to_vec())),
            );
            merge_max(
                &mut cs.max,
                s.max_opt()
                    .map(|max| StatValue::FixedLenByteArray(max.data().to_vec())),
            );
        }
        Statistics::Int96(s) => {
            merge_min(&mut cs.min, s.min_opt().copied().map(StatValue::Int96));
            merge_max(&mut cs.max, s.max_opt().copied().map(StatValue::Int96));
        }
    }
}

fn merge_min(current: &mut Option<StatValue>, candidate: Option<StatValue>) {
    merge_bound(current, candidate, |ordering| ordering.is_lt());
}

fn merge_max(current: &mut Option<StatValue>, candidate: Option<StatValue>) {
    merge_bound(current, candidate, |ordering| ordering.is_gt());
}

fn merge_bound(
    current: &mut Option<StatValue>,
    candidate: Option<StatValue>,
    should_replace: impl Fn(std::cmp::Ordering) -> bool,
) {
    let Some(candidate) = candidate else {
        return;
    };

    let replace = match current.as_ref() {
        None => true,
        Some(existing) => candidate
            .partial_compare(existing)
            .is_some_and(should_replace),
    };

    if replace {
        *current = Some(candidate);
    }
}

/// Output statistics in the requested format
fn output_stats(stats: &[ColumnStats], output: OutputFormat, quiet: bool) -> Result<()> {
    match output {
        OutputFormat::Table => {
            let mut tbl = Table::new();
            if !quiet {
                tbl.set_header(vec!["Column", "Type", "Nulls", "Min", "Max"]);
            }
            for s in stats {
                tbl.add_row(vec![
                    Cell::new(&s.name),
                    Cell::new(&s.physical_type),
                    Cell::new(s.null_count),
                    Cell::new(
                        s.min
                            .as_ref()
                            .map_or_else(|| "N/A".to_string(), StatValue::display),
                    ),
                    Cell::new(
                        s.max
                            .as_ref()
                            .map_or_else(|| "N/A".to_string(), StatValue::display),
                    ),
                ]);
            }
            println!("{tbl}");
        }
        OutputFormat::Json => {
            let rows: Vec<StatRow> = stats
                .iter()
                .map(|s| StatRow {
                    column: s.name.clone(),
                    dtype: s.physical_type.clone(),
                    null_count: s.null_count,
                    min: s.min.as_ref().map(StatValue::display),
                    max: s.max.as_ref().map(StatValue::display),
                })
                .collect();
            let json = serde_json::to_string_pretty(&rows)?;
            println!("{json}");
        }
        OutputFormat::Jsonl => {
            for s in stats {
                let row = StatRow {
                    column: s.name.clone(),
                    dtype: s.physical_type.clone(),
                    null_count: s.null_count,
                    min: s.min.as_ref().map(StatValue::display),
                    max: s.max.as_ref().map(StatValue::display),
                };
                let json = serde_json::to_string(&row)?;
                println!("{json}");
            }
        }
        OutputFormat::Csv => {
            if !quiet {
                println!("column,type,null_count,min,max");
            }
            for s in stats {
                println!(
                    "{},{},{},{},{}",
                    escape_csv(&s.name),
                    escape_csv(&s.physical_type),
                    s.null_count,
                    escape_csv(&s.min.as_ref().map_or_else(String::new, StatValue::display)),
                    escape_csv(&s.max.as_ref().map_or_else(String::new, StatValue::display))
                );
            }
        }
    }
    Ok(())
}

/// Escape a string for CSV output
fn escape_csv(s: &str) -> String {
    if s.contains(',') || s.contains('"') || s.contains('\n') {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}
