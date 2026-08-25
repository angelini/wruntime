use futures::StreamExt as _;
use wasmtime::component::Resource;

use wr_common::pool::pg_error_string;

use super::bindings::{CursorState, TxState};
use super::rows::pg_row_to_wit;
use super::wruntime::db::database::{DbError, HostRowCursor, Row};
use crate::state::ModuleState;

pub const MAX_CURSOR_BATCH_ROWS: u32 = 1_024;

fn validate_batch_size(max: u32) -> Result<(), DbError> {
    if (1..=MAX_CURSOR_BATCH_ROWS).contains(&max) {
        Ok(())
    } else {
        Err(DbError::Query(format!(
            "batch size must be between 1 and {MAX_CURSOR_BATCH_ROWS}, got {max}"
        )))
    }
}

impl HostRowCursor for ModuleState {
    async fn next_batch(
        &mut self,
        self_: Resource<CursorState>,
        max: u32,
    ) -> Result<Vec<Row>, DbError> {
        validate_batch_size(max)?;
        if self
            .table()
            .get(&self_)
            .map_err(|error| DbError::Connection(error.to_string()))?
            .done
        {
            return Ok(vec![]);
        }

        let mut rows = Vec::with_capacity(max.min(256) as usize);
        for _ in 0..max {
            let next = {
                let cursor = self
                    .table()
                    .get_mut(&self_)
                    .map_err(|error| DbError::Connection(error.to_string()))?;
                let stream = cursor
                    .stream
                    .as_mut()
                    .ok_or_else(|| DbError::Connection("cursor stream is unavailable".into()))?;
                stream.next().await
            };
            match next {
                Some(Ok(pg_row)) => match pg_row_to_wit(&pg_row) {
                    Ok(row) => rows.push(row),
                    Err(error) => {
                        self.finish_cursor(self_.rep(), Some(error.clone()), false, false)
                            .await?;
                        return Err(error);
                    }
                },
                Some(Err(error)) => {
                    let error = DbError::Query(pg_error_string(&error));
                    if let Some(lifecycle) = self
                        .table()
                        .get(&self_)
                        .ok()
                        .and_then(|cursor| cursor.lifecycle.clone())
                    {
                        lifecycle.mark_postgres_error();
                    }
                    self.finish_cursor(self_.rep(), Some(error.clone()), false, false)
                        .await?;
                    return Err(error);
                }
                None => {
                    self.table()
                        .get_mut(&self_)
                        .map_err(|error| DbError::Connection(error.to_string()))?
                        .telemetry
                        .add_rows(rows.len());
                    self.finish_cursor(self_.rep(), None, false, true).await?;
                    return Ok(rows);
                }
            }
        }
        self.table()
            .get_mut(&self_)
            .map_err(|error| DbError::Connection(error.to_string()))?
            .telemetry
            .add_rows(rows.len());
        Ok(rows)
    }

    async fn drop(&mut self, rep: Resource<CursorState>) -> wasmtime::Result<()> {
        let is_transaction = self.table().get(&rep)?.parent.is_some();
        if is_transaction {
            self.finish_cursor(rep.rep(), None, true, false)
                .await
                .map_err(|error| wasmtime::Error::msg(format!("{error:?}")))?;
        }
        let mut cursor = self.table().delete(rep)?;
        if !cursor.done {
            cursor.telemetry.finish_cancelled();
        }
        Ok(())
    }
}

impl ModuleState {
    async fn finish_cursor(
        &mut self,
        cursor_rep: u32,
        mut first_error: Option<DbError>,
        cancelled: bool,
        exhausted: bool,
    ) -> Result<(), DbError> {
        let cursor_resource = Resource::<CursorState>::new_borrow(cursor_rep);
        let (parent_rep, lifecycle) = {
            let cursor = self
                .table()
                .get(&cursor_resource)
                .map_err(|error| DbError::Connection(error.to_string()))?;
            if cursor.done {
                return Ok(());
            }
            (cursor.parent, cursor.lifecycle.clone())
        };

        let Some(parent_rep) = parent_rep else {
            let cursor = self
                .table()
                .get_mut(&cursor_resource)
                .map_err(|error| DbError::Connection(error.to_string()))?;
            cursor.done = true;
            cursor.stream.take();
            cursor.conn.take();
            if let Some(error) = first_error.as_ref() {
                cursor.telemetry.finish_error(error);
            } else if cancelled {
                cursor.telemetry.finish_cancelled();
            } else {
                cursor.telemetry.finish_success();
            }
            return Ok(());
        };
        let lifecycle = lifecycle.ok_or_else(|| {
            DbError::Connection("transaction cursor lifecycle is unavailable".into())
        })?;

        if !exhausted {
            loop {
                let next = {
                    let cursor = self
                        .table()
                        .get_mut(&cursor_resource)
                        .map_err(|error| DbError::Connection(error.to_string()))?;
                    match cursor.stream.as_mut() {
                        Some(stream) => stream.next().await,
                        None => None,
                    }
                };
                match next {
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        lifecycle.mark_postgres_error();
                        if first_error.is_none() {
                            first_error = Some(DbError::Query(pg_error_string(&error)));
                        }
                    }
                    None => break,
                }
            }
        }
        // Drop the completed response receiver before issuing the protocol
        // barrier so the connection driver can advance to the next request.
        self.table()
            .get_mut(&cursor_resource)
            .map_err(|error| DbError::Connection(error.to_string()))?
            .stream
            .take();

        let parent_resource = Resource::<TxState>::new_borrow(parent_rep);
        let synchronized = {
            let parent = self
                .table()
                .get(&parent_resource)
                .map_err(|error| DbError::Connection(error.to_string()))?;
            let client = parent.client.as_ref().ok_or_else(|| {
                DbError::Connection("transaction connection is unavailable".into())
            })?;
            client.simple_query("").await
        };
        if let Err(error) = synchronized {
            lifecycle.mark_discard();
            return Err(first_error.unwrap_or_else(|| {
                DbError::Connection(format!(
                    "failed to synchronize transaction cursor: {}",
                    pg_error_string(&error)
                ))
            }));
        }

        self.table()
            .remove_child(cursor_resource, parent_resource)
            .map_err(|error| {
                lifecycle.mark_discard();
                DbError::Connection(error.to_string())
            })?;
        lifecycle.release_cursor();

        let cursor_resource = Resource::<CursorState>::new_borrow(cursor_rep);
        let cursor = self
            .table()
            .get_mut(&cursor_resource)
            .map_err(|error| DbError::Connection(error.to_string()))?;
        cursor.done = true;
        cursor.stream.take();
        if let Some(error) = first_error.as_ref() {
            cursor.telemetry.finish_error(error);
        } else if cancelled {
            cursor.telemetry.finish_cancelled();
        } else {
            cursor.telemetry.finish_success();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_batch_size_boundaries_are_enforced() {
        assert!(matches!(validate_batch_size(0), Err(DbError::Query(_))));
        assert!(validate_batch_size(1).is_ok());
        assert!(validate_batch_size(MAX_CURSOR_BATCH_ROWS).is_ok());
        assert!(matches!(
            validate_batch_size(MAX_CURSOR_BATCH_ROWS + 1),
            Err(DbError::Query(_))
        ));
        assert!(matches!(
            validate_batch_size(u32::MAX),
            Err(DbError::Query(_))
        ));
    }
}
