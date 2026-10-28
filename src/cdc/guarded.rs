//! Guarded SQL reads of view metadata and retained source/group Values.
use serde::de::DeserializeOwned;

use crate::{
    query::{GuardedRows, Row, json_unescape_working_bytes, owned_value_memory_bound},
    repl::{ReadConsistency, Replication},
    storage::{ReadBudget, ReadReservation, StorageError},
};

use super::{
    MATERIALIZED_VIEW_STATE_TABLE, MATERIALIZED_VIEWS_TABLE, MaterializedViewError,
    MaterializedViewSpec, MaterializedViewState,
};

struct Leased<T> {
    value: T,
    _reservation: Box<dyn ReadReservation>,
}

fn read<T: DeserializeOwned>(
    repl: &dyn Replication,
    table: &str,
    name: &str,
    state: bool,
    budget: &dyn ReadBudget,
) -> Result<Leased<T>, MaterializedViewError> {
    let mut result = None;
    repl.visit_get(
        table,
        name.as_bytes(),
        ReadConsistency::Strong,
        budget,
        &mut |bytes| {
            if let Some(bytes) = bytes {
                let _scratch = budget.reserve(json_unescape_working_bytes(bytes))?;
                let bound = super::guarded_decode::bound(bytes, state, budget)?;
                let reservation = budget.reserve(bound.saturating_add(512))?;
                let value = serde_json::from_slice(bytes)
                    .map_err(|error| StorageError::Corruption(error.to_string()))?;
                budget.check()?;
                result = Some(Leased {
                    value,
                    _reservation: reservation,
                });
            }
            Ok(false)
        },
    )?;
    result.ok_or_else(|| MaterializedViewError::MissingView(name.to_owned()))
}

pub(crate) fn read_materialized_view_guarded(
    repl: &dyn Replication,
    name: &str,
    budget: &dyn ReadBudget,
) -> Result<GuardedRows, MaterializedViewError> {
    budget.check()?;
    let spec: Leased<MaterializedViewSpec> =
        read(repl, MATERIALIZED_VIEWS_TABLE, name, false, budget)?;
    let mut state: Leased<MaterializedViewState> =
        read(repl, MATERIALIZED_VIEW_STATE_TABLE, name, true, budget)?;
    super::upgrade_legacy_view_counts(&spec.value, &mut state.value, Some(budget))?;
    let mut rows = GuardedRows::new();
    for group in state.value.groups.values() {
        budget.check()?;
        let width = 1_usize.saturating_add(spec.value.aggregates.len().min(group.aggregates.len()));
        let reservation = budget.reserve(
            owned_value_memory_bound(&group.group_value)
                .saturating_add(width.saturating_mul(std::mem::size_of::<crate::model::Value>()))
                .saturating_add(512),
        )?;
        let mut row: Row = Vec::with_capacity(width);
        row.push(group.group_value.clone());
        row.extend(
            spec.value
                .aggregates
                .iter()
                .zip(&group.aggregates)
                .map(|(_, aggregate)| super::aggregate_to_value(aggregate)),
        );
        rows.push(row, reservation);
    }
    rows.sort_json(budget)?;
    budget.check()?;
    Ok(rows)
}
