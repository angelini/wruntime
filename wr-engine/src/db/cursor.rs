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
            .map_err(|e| DbError::Connection(e.to_string()))?;
        if cursor.done {
            return Ok(vec![]);
        }
        let mut rows = Vec::with_capacity(max.min(256) as usize);
        for _ in 0..max {
            match cursor.stream.next().await {
                Some(Ok(pg_row)) => rows.push(pg_row_to_wit(&pg_row)),
                Some(Err(e)) => return Err(DbError::Query(e.to_string())),
                None => {
                    cursor.done = true;
                    break;
                }
            }
        }
        Ok(rows)
    }

    async fn drop(&mut self, rep: Resource<CursorState>) -> wasmtime::Result<()> {
        self.table().delete(rep)?;
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
