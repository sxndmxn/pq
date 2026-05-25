use crate::dataset::Dataset;
use crate::engine::parquet;
use crate::plan::scan::{ScanMode, ScanPlan};
use crate::Result;
use arrow::array::RecordBatch;

pub fn scan(dataset: &Dataset, plan: &ScanPlan) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();

    for source in dataset.sources() {
        let mut source_batches = match plan.mode() {
            ScanMode::Head { rows } => parquet::read_head(source.path(), *rows)?,
            ScanMode::Tail { rows } => parquet::read_tail(source.path(), *rows)?,
        };
        batches.append(&mut source_batches);
    }

    Ok(batches)
}
