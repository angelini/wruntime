use futures::StreamExt as _;
use wasmtime::component::Resource;

use super::bindings::CursorState;
use super::rows::pg_row_to_wit;
use super::wruntime::db::database::{DbError, HostRowCursor, Row};
use crate::state::ModuleState;

// ── HostRowCursor implementation ─────────────────────────────────────────

fn validate_batch_size(max: u32) -> Result<(), DbError> {
    if max == 0 {
        Err(DbError::Query("batch size must be > 0".into()))
    } else {
        Ok(())
    }
}

impl HostRowCursor for ModuleState {
    async fn next_batch(
        &mut self,
        self_: Resource<CursorState>,
        max: u32,
    ) -> Result<Vec<Row>, DbError> {
        validate_batch_size(max)?;
        let cursor = self
            .table()
            .get_mut(&self_)
            .map_err(|error| DbError::Connection(error.to_string()))?;
        if cursor.done {
            return Ok(vec![]);
        }
        let mut rows = Vec::with_capacity(max.min(256) as usize);
        for _ in 0..max {
            match cursor.stream.next().await {
                Some(Ok(pg_row)) => match pg_row_to_wit(&pg_row) {
                    Ok(row) => rows.push(row),
                    Err(error) => {
                        cursor.done = true;
                        cursor.telemetry.finish_error(&error);
                        return Err(error);
                    }
                },
                Some(Err(error)) => {
                    let error = DbError::Query(error.to_string());
                    cursor.done = true;
                    cursor.telemetry.finish_error(&error);
                    return Err(error);
                }
                None => {
                    cursor.done = true;
                    cursor.telemetry.add_rows(rows.len());
                    cursor.telemetry.finish_success();
                    return Ok(rows);
                }
            }
        }
        cursor.telemetry.add_rows(rows.len());
        Ok(rows)
    }

    async fn drop(&mut self, rep: Resource<CursorState>) -> wasmtime::Result<()> {
        let mut cursor = self.table().delete(rep)?;
        cursor.telemetry.finish_cancelled();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zero_batch_size_is_rejected() {
        assert!(matches!(validate_batch_size(0), Err(DbError::Query(_))));
        assert!(validate_batch_size(1).is_ok());
    }
}
