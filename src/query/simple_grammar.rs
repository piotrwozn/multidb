//! The direct equality executor accepts only the clauses it actually evaluates.

use std::ops::ControlFlow;

use sqlparser::ast::{Expr, Query, SelectItem, SetExpr, TableFactor, visit_expressions};

pub(super) fn supported(query: &Query) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };
    let [relation] = select.from.as_slice() else {
        return false;
    };
    let TableFactor::Table { name, .. } = &relation.relation else {
        return false;
    };
    // Aliases, renamed columns, temporal versions, table arguments and hints need
    // DataFusion's full name resolution and table semantics.
    if relation.to_string() != name.to_string() {
        return false;
    }
    for item in &select.projection {
        match item {
            SelectItem::Wildcard(_) if item.to_string() == "*" => {}
            SelectItem::UnnamedExpr(Expr::Identifier(_)) => {}
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(_),
                alias,
            } if alias.quote_style.is_some() || alias.value == alias.value.to_ascii_lowercase() => {
            }
            _ => return false,
        }
    }
    let ControlFlow::Continue(()) = visit_expressions(query, |expr| {
        match expr {
            Expr::CompoundIdentifier(_) | Expr::Function(_) => return ControlFlow::Break(()),
            Expr::Identifier(ident)
                if ident.quote_style.is_none()
                    && ident.value != ident.value.to_ascii_lowercase() =>
            {
                return ControlFlow::Break(());
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }) else {
        return false;
    };
    let Some(selection) = &select.selection else {
        return false;
    };
    let projections = select
        .projection
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let mut accepted = format!("SELECT {projections} FROM {name} WHERE {selection}");
    if let Some(limit) = &query.limit_clause {
        accepted.push(' ');
        accepted.push_str(limit.to_string().trim_start());
    }
    query.to_string() == accepted
}
