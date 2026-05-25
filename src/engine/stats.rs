use crate::model::ColumnStats;
use crate::Result;
use parquet::data_type::Int96;
use parquet::file::reader::FileReader;
use parquet::file::statistics::Statistics;
use std::path::Path;

pub fn column_stats(path: &Path, column_name: Option<&str>) -> Result<Vec<ColumnStats>> {
    let reader = super::parquet::serialized_reader(path)?;
    let metadata = reader.metadata();
    let schema = metadata.file_metadata().schema_descr();

    let mut column_stats: Vec<AccumulatedColumnStats> = (0..schema.num_columns())
        .map(|i| AccumulatedColumnStats {
            column: schema.column(i).name().to_string(),
            dtype: format!("{:?}", schema.column(i).physical_type()),
            null_count: 0,
            min: None,
            max: None,
        })
        .collect();

    for rg_idx in 0..metadata.num_row_groups() {
        let row_group = metadata.row_group(rg_idx);
        for (col_idx, stats) in column_stats
            .iter_mut()
            .enumerate()
            .take(row_group.num_columns())
        {
            if let Some(column_statistics) = row_group.column(col_idx).statistics() {
                stats.null_count += column_statistics.null_count_opt().unwrap_or(0);
                update_min_max(stats, column_statistics);
            }
        }
    }

    let rows = column_stats
        .into_iter()
        .filter(|stats| column_name.is_none_or(|name| stats.column == name))
        .map(AccumulatedColumnStats::into_row)
        .collect::<Vec<_>>();

    Ok(rows)
}

struct AccumulatedColumnStats {
    column: String,
    dtype: String,
    null_count: u64,
    min: Option<StatValue>,
    max: Option<StatValue>,
}

impl AccumulatedColumnStats {
    fn into_row(self) -> ColumnStats {
        ColumnStats {
            column: self.column,
            dtype: self.dtype,
            null_count: self.null_count,
            min: self.min.as_ref().map(StatValue::display),
            max: self.max.as_ref().map(StatValue::display),
        }
    }
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

    fn partial_cmp_value(&self, other: &Self) -> Option<std::cmp::Ordering> {
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

fn update_min_max(stats: &mut AccumulatedColumnStats, parquet_stats: &Statistics) {
    match parquet_stats {
        Statistics::Int32(source) => {
            merge_min(
                &mut stats.min,
                source.min_opt().copied().map(StatValue::Int32),
            );
            merge_max(
                &mut stats.max,
                source.max_opt().copied().map(StatValue::Int32),
            );
        }
        Statistics::Int64(source) => {
            merge_min(
                &mut stats.min,
                source.min_opt().copied().map(StatValue::Int64),
            );
            merge_max(
                &mut stats.max,
                source.max_opt().copied().map(StatValue::Int64),
            );
        }
        Statistics::Float(source) => {
            merge_min(
                &mut stats.min,
                source.min_opt().copied().map(StatValue::Float),
            );
            merge_max(
                &mut stats.max,
                source.max_opt().copied().map(StatValue::Float),
            );
        }
        Statistics::Double(source) => {
            merge_min(
                &mut stats.min,
                source.min_opt().copied().map(StatValue::Double),
            );
            merge_max(
                &mut stats.max,
                source.max_opt().copied().map(StatValue::Double),
            );
        }
        Statistics::ByteArray(source) => {
            merge_min(
                &mut stats.min,
                source
                    .min_opt()
                    .map(|value| StatValue::ByteArray(value.data().to_vec())),
            );
            merge_max(
                &mut stats.max,
                source
                    .max_opt()
                    .map(|value| StatValue::ByteArray(value.data().to_vec())),
            );
        }
        Statistics::Boolean(source) => {
            merge_min(
                &mut stats.min,
                source.min_opt().copied().map(StatValue::Boolean),
            );
            merge_max(
                &mut stats.max,
                source.max_opt().copied().map(StatValue::Boolean),
            );
        }
        Statistics::FixedLenByteArray(source) => {
            merge_min(
                &mut stats.min,
                source
                    .min_opt()
                    .map(|value| StatValue::FixedLenByteArray(value.data().to_vec())),
            );
            merge_max(
                &mut stats.max,
                source
                    .max_opt()
                    .map(|value| StatValue::FixedLenByteArray(value.data().to_vec())),
            );
        }
        Statistics::Int96(source) => {
            merge_min(
                &mut stats.min,
                source.min_opt().copied().map(StatValue::Int96),
            );
            merge_max(
                &mut stats.max,
                source.max_opt().copied().map(StatValue::Int96),
            );
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
            .partial_cmp_value(existing)
            .is_some_and(should_replace),
    };

    if replace {
        *current = Some(candidate);
    }
}
