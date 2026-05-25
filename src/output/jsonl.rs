use crate::Result;
use arrow::array::RecordBatch;

pub fn print_batches(batches: &[RecordBatch]) -> Result<()> {
    crate::output::json::print_batches_jsonl(batches)
}
