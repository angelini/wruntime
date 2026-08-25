use futures::StreamExt as _;
use wasmtime::component::Resource;

use wr_common::pool::pg_error_string;

use super::bindings::{CursorOwner, CursorResourceState, CursorState, TxState};
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
        if matches!(
            self.table()
                .get(&self_)
                .map_err(|error| DbError::Connection(error.to_string()))?
                .state,
            CursorResourceState::Exhausted
        ) {
            return Ok(vec![]);
        }

        let mut rows = Vec::with_capacity(max.min(256) as usize);
        for _ in 0..max {
            let next = {
                let cursor = self
                    .table()
                    .get_mut(&self_)
                    .map_err(|error| DbError::Connection(error.to_string()))?;
                match &mut cursor.state {
                    CursorResourceState::Active { stream, .. } => stream.next().await,
                    CursorResourceState::Exhausted => return Ok(rows),
                }
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
                    if let Some(lifecycle) =
                        self.table()
                            .get(&self_)
                            .ok()
                            .and_then(|cursor| match &cursor.state {
                                CursorResourceState::Active {
                                    owner: CursorOwner::Transaction { lifecycle, .. },
                                    ..
                                } => Some(lifecycle.clone()),
                                _ => None,
                            })
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
        let active_transaction = matches!(
            &self.table().get(&rep)?.state,
            CursorResourceState::Active {
                owner: CursorOwner::Transaction { .. },
                ..
            }
        );
        if active_transaction {
            self.finish_cursor(rep.rep(), None, true, false)
                .await
                .map_err(|error| wasmtime::Error::msg(format!("{error:?}")))?;
        }
        let mut cursor = self.table().delete(rep)?;
        if matches!(cursor.state, CursorResourceState::Active { .. }) {
            cursor.state = CursorResourceState::Exhausted;
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
        let transaction = {
            let cursor = self
                .table()
                .get(&cursor_resource)
                .map_err(|error| DbError::Connection(error.to_string()))?;
            match &cursor.state {
                CursorResourceState::Exhausted => return Ok(()),
                CursorResourceState::Active {
                    owner: CursorOwner::Connection { .. },
                    ..
                } => None,
                CursorResourceState::Active {
                    owner: CursorOwner::Transaction { parent, lifecycle },
                    ..
                } => Some((*parent, lifecycle.clone())),
            }
        };

        let Some((parent_rep, lifecycle)) = transaction else {
            let cursor = self
                .table()
                .get_mut(&cursor_resource)
                .map_err(|error| DbError::Connection(error.to_string()))?;
            cursor.state = CursorResourceState::Exhausted;
            if let Some(error) = first_error.as_ref() {
                cursor.telemetry.finish_error(error);
            } else if cancelled {
                cursor.telemetry.finish_cancelled();
            } else {
                cursor.telemetry.finish_success();
            }
            return Ok(());
        };

        if !exhausted {
            loop {
                let next = {
                    let cursor = self
                        .table()
                        .get_mut(&cursor_resource)
                        .map_err(|error| DbError::Connection(error.to_string()))?;
                    match &mut cursor.state {
                        CursorResourceState::Active { stream, .. } => stream.next().await,
                        CursorResourceState::Exhausted => None,
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

        let owner = {
            let cursor = self
                .table()
                .get_mut(&cursor_resource)
                .map_err(|error| DbError::Connection(error.to_string()))?;
            match std::mem::replace(&mut cursor.state, CursorResourceState::Exhausted) {
                CursorResourceState::Active { stream, owner } => {
                    drop(stream);
                    owner
                }
                CursorResourceState::Exhausted => return Ok(()),
            }
        };
        debug_assert!(matches!(owner, CursorOwner::Transaction { .. }));

        let parent_resource = Resource::<TxState>::new_borrow(parent_rep);
        let synchronized = {
            let parent = self
                .table()
                .get(&parent_resource)
                .map_err(|error| DbError::Connection(error.to_string()))?;
            parent.client()?.simple_query("").await
        };
        if let Err(error) = synchronized {
            lifecycle.mark_discard();
            if first_error.is_none() {
                first_error = Some(DbError::Connection(format!(
                    "failed to synchronize transaction cursor: {}",
                    pg_error_string(&error)
                )));
            }
        }

        let remove_result = self
            .table()
            .remove_child(cursor_resource, parent_resource)
            .map_err(|error| DbError::Connection(error.to_string()));
        if remove_result.is_err() {
            lifecycle.mark_discard();
        }
        lifecycle.release_cursor();

        let cursor_resource = Resource::<CursorState>::new_borrow(cursor_rep);
        let cursor = self
            .table()
            .get_mut(&cursor_resource)
            .map_err(|error| DbError::Connection(error.to_string()))?;
        if let Some(error) = first_error.as_ref() {
            cursor.telemetry.finish_error(error);
        } else if cancelled {
            cursor.telemetry.finish_cancelled();
        } else {
            cursor.telemetry.finish_success();
        }

        remove_result?;
        if let Some(error) = first_error {
            return Err(error);
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
