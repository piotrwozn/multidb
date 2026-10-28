//! Typed allocation preflight for the persisted materialized-view JSON shape.
use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};

use super::{
    AggregateSpec, AggregateValue, GroupState, MaterializedViewSpec, MaterializedViewState,
};
use crate::{
    model::ValueMemoryCounter,
    storage::{ReadBudget, StorageError},
};

pub(super) fn bound(
    bytes: &[u8],
    state: bool,
    budget: &dyn ReadBudget,
) -> Result<usize, StorageError> {
    let limit = budget.remaining_bytes();
    let mut counter = ValueMemoryCounter::new(limit);
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    let result = if state {
        State(&mut counter, budget).deserialize(&mut decoder)
    } else {
        Spec(&mut counter, budget).deserialize(&mut decoder)
    };
    budget.check()?;
    if counter.used() > limit {
        return Err(StorageError::ResourceLimit(
            "materialized-view decoded state exceeds read budget".to_owned(),
        ));
    }
    result.map_err(|error| StorageError::Corruption(error.to_string()))?;
    decoder
        .end()
        .map_err(|error| StorageError::Corruption(error.to_string()))?;
    Ok(counter.used())
}

fn check<E: de::Error>(budget: &dyn ReadBudget) -> Result<(), E> {
    budget.check().map_err(E::custom)
}

fn mark<E: de::Error>(seen: &mut u8, bit: u8) -> Result<(), E> {
    if *seen & bit != 0 {
        return Err(E::custom("duplicate materialized-view field"));
    }
    *seen |= bit;
    Ok(())
}

fn required<E: de::Error>(seen: u8, mask: u8) -> Result<(), E> {
    if seen & mask == mask {
        Ok(())
    } else {
        Err(E::custom("missing materialized-view field"))
    }
}

struct Text<'a>(&'a mut ValueMemoryCounter);
impl<'de> DeserializeSeed<'de> for Text<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_str(self)
    }
}
impl Visitor<'_> for Text<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("text")
    }
    fn visit_str<E: de::Error>(self, text: &str) -> Result<(), E> {
        self.0.charge(text.len())
    }
}

#[derive(serde::Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum StateField {
    FreshToLsn,
    SourceRows,
    Groups,
    #[serde(other)]
    Other,
}

struct State<'a>(&'a mut ValueMemoryCounter, &'a dyn ReadBudget);
impl<'de> DeserializeSeed<'de> for State<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        self.0
            .charge(std::mem::size_of::<MaterializedViewState>())?;
        decoder.deserialize_struct(
            "MaterializedViewState",
            &["fresh_to_lsn", "source_rows", "groups"],
            self,
        )
    }
}
impl<'de> Visitor<'de> for State<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("materialized-view state")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let mut seen = 0;
        while let Some(field) = map.next_key::<StateField>()? {
            check(self.1)?;
            match field {
                StateField::FreshToLsn => {
                    mark(&mut seen, 1)?;
                    map.next_value::<u64>()?;
                }
                StateField::SourceRows => {
                    mark(&mut seen, 2)?;
                    map.next_value_seed(Rows(self.0, self.1, false))?;
                }
                StateField::Groups => {
                    mark(&mut seen, 4)?;
                    map.next_value_seed(Rows(self.0, self.1, true))?;
                }
                StateField::Other => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        required(seen, 7)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        sequence
            .next_element::<u64>()?
            .ok_or_else(|| de::Error::invalid_length(0, &self))?;
        sequence
            .next_element_seed(Rows(self.0, self.1, false))?
            .ok_or_else(|| de::Error::custom("missing source_rows"))?;
        sequence
            .next_element_seed(Rows(self.0, self.1, true))?
            .ok_or_else(|| de::Error::custom("missing groups"))?;
        Ok(())
    }
}

struct Rows<'a>(&'a mut ValueMemoryCounter, &'a dyn ReadBudget, bool);
impl<'de> DeserializeSeed<'de> for Rows<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_map(self)
    }
}
impl<'de> Visitor<'de> for Rows<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("view row map")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let mut first = true;
        while map.next_key_seed(Text(self.0))?.is_some() {
            check(self.1)?;
            self.0.charge::<A::Error>(if first { 2_048 } else { 512 })?;
            first = false;
            if self.2 {
                map.next_value_seed(Group(self.0, self.1))?;
            } else {
                map.next_value_seed(self.0.row_seed())?;
            }
        }
        Ok(())
    }
}

#[derive(serde::Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum GroupField {
    GroupValue,
    Aggregates,
    RowCount,
    #[serde(other)]
    Other,
}

struct Group<'a>(&'a mut ValueMemoryCounter, &'a dyn ReadBudget);
impl<'de> DeserializeSeed<'de> for Group<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        self.0.charge(std::mem::size_of::<GroupState>())?;
        decoder.deserialize_struct(
            "GroupState",
            &["group_value", "aggregates", "row_count"],
            self,
        )
    }
}
impl<'de> Visitor<'de> for Group<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("materialized group")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let mut seen = 0;
        while let Some(field) = map.next_key::<GroupField>()? {
            check(self.1)?;
            match field {
                GroupField::GroupValue => {
                    mark(&mut seen, 1)?;
                    map.next_value_seed(self.0.value_seed())?;
                }
                GroupField::Aggregates => {
                    mark(&mut seen, 2)?;
                    map.next_value_seed(Aggregates(self.0, self.1, false))?;
                }
                GroupField::RowCount => {
                    mark(&mut seen, 4)?;
                    map.next_value::<Option<u64>>()?;
                }
                GroupField::Other => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        required(seen, 3)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        sequence
            .next_element_seed(self.0.value_seed())?
            .ok_or_else(|| de::Error::custom("missing group_value"))?;
        sequence
            .next_element_seed(Aggregates(self.0, self.1, false))?
            .ok_or_else(|| de::Error::custom("missing aggregates"))?;
        sequence.next_element::<Option<u64>>()?;
        Ok(())
    }
}

struct Aggregates<'a>(&'a mut ValueMemoryCounter, &'a dyn ReadBudget, bool);
impl<'de> DeserializeSeed<'de> for Aggregates<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_seq(self)
    }
}
impl<'de> Visitor<'de> for Aggregates<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("aggregate array")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        let mut first = true;
        loop {
            check(self.1)?;
            let next = if self.2 {
                sequence.next_element_seed(Aggregate(self.0, self.1))?
            } else {
                sequence.next_element::<AggregateValue>()?.map(|_| ())
            };
            if next.is_none() {
                break;
            }
            let item = if self.2 {
                std::mem::size_of::<AggregateSpec>()
            } else {
                std::mem::size_of::<AggregateValue>()
            };
            self.0
                .charge::<A::Error>(item.saturating_mul(if first { 4 } else { 2 }))?;
            first = false;
        }
        Ok(())
    }
}

#[derive(serde::Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum SpecField {
    Name,
    SourceTable,
    Filter,
    GroupBy,
    Aggregates,
    #[serde(other)]
    Other,
}

struct Spec<'a>(&'a mut ValueMemoryCounter, &'a dyn ReadBudget);
impl<'de> DeserializeSeed<'de> for Spec<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        self.0.charge(std::mem::size_of::<MaterializedViewSpec>())?;
        decoder.deserialize_struct(
            "MaterializedViewSpec",
            &["name", "source_table", "filter", "group_by", "aggregates"],
            self,
        )
    }
}
impl<'de> Visitor<'de> for Spec<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("materialized-view spec")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let mut seen = 0;
        while let Some(field) = map.next_key::<SpecField>()? {
            check(self.1)?;
            match field {
                SpecField::Name => {
                    mark(&mut seen, 1)?;
                    map.next_value_seed(Text(self.0))?;
                }
                SpecField::SourceTable => {
                    mark(&mut seen, 2)?;
                    map.next_value_seed(Text(self.0))?;
                }
                SpecField::Filter => {
                    mark(&mut seen, 4)?;
                    map.next_value_seed(Predicate(self.0, self.1, true))?;
                }
                SpecField::GroupBy => {
                    mark(&mut seen, 8)?;
                    map.next_value::<usize>()?;
                }
                SpecField::Aggregates => {
                    mark(&mut seen, 16)?;
                    map.next_value_seed(Aggregates(self.0, self.1, true))?;
                }
                SpecField::Other => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        required(seen, 1 | 2 | 8 | 16)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        sequence
            .next_element_seed(Text(self.0))?
            .ok_or_else(|| de::Error::custom("missing name"))?;
        sequence
            .next_element_seed(Text(self.0))?
            .ok_or_else(|| de::Error::custom("missing source_table"))?;
        sequence
            .next_element_seed(Predicate(self.0, self.1, true))?
            .ok_or_else(|| de::Error::custom("missing filter"))?;
        sequence
            .next_element::<usize>()?
            .ok_or_else(|| de::Error::custom("missing group_by"))?;
        sequence
            .next_element_seed(Aggregates(self.0, self.1, true))?
            .ok_or_else(|| de::Error::custom("missing aggregates"))?;
        Ok(())
    }
}

#[derive(serde::Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum PredicateField {
    Column,
    Equals,
    #[serde(other)]
    Other,
}

struct Predicate<'a>(&'a mut ValueMemoryCounter, &'a dyn ReadBudget, bool);
impl<'de> DeserializeSeed<'de> for Predicate<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        if self.2 {
            decoder.deserialize_option(self)
        } else {
            decoder.deserialize_struct("SimplePredicate", &["column", "equals"], self)
        }
    }
}
impl<'de> Visitor<'de> for Predicate<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("view predicate")
    }
    fn visit_none<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_some<D: de::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        Predicate(self.0, self.1, false).deserialize(decoder)
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let mut seen = 0;
        while let Some(field) = map.next_key::<PredicateField>()? {
            check(self.1)?;
            match field {
                PredicateField::Column => {
                    mark(&mut seen, 1)?;
                    map.next_value::<usize>()?;
                }
                PredicateField::Equals => {
                    mark(&mut seen, 2)?;
                    map.next_value_seed(self.0.value_seed())?;
                }
                PredicateField::Other => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        required(seen, 3)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        sequence
            .next_element::<usize>()?
            .ok_or_else(|| de::Error::custom("missing column"))?;
        sequence
            .next_element_seed(self.0.value_seed())?
            .ok_or_else(|| de::Error::custom("missing equals"))?;
        Ok(())
    }
}

#[derive(serde::Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum AggregateField {
    OutputName,
    Kind,
    Column,
    #[serde(other)]
    Other,
}

struct Aggregate<'a>(&'a mut ValueMemoryCounter, &'a dyn ReadBudget);
impl<'de> DeserializeSeed<'de> for Aggregate<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_struct("AggregateSpec", &["output_name", "kind", "column"], self)
    }
}
impl<'de> Visitor<'de> for Aggregate<'_> {
    type Value = ();
    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("aggregate spec")
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        let mut seen = 0;
        while let Some(field) = map.next_key::<AggregateField>()? {
            check(self.1)?;
            match field {
                AggregateField::OutputName => {
                    mark(&mut seen, 1)?;
                    map.next_value_seed(Text(self.0))?;
                }
                AggregateField::Kind => {
                    mark(&mut seen, 2)?;
                    map.next_value::<super::AggregateKind>()?;
                }
                AggregateField::Column => {
                    mark(&mut seen, 4)?;
                    map.next_value::<Option<usize>>()?;
                }
                AggregateField::Other => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        required(seen, 3)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        sequence
            .next_element_seed(Text(self.0))?
            .ok_or_else(|| de::Error::custom("missing output_name"))?;
        sequence
            .next_element::<super::AggregateKind>()?
            .ok_or_else(|| de::Error::custom("missing kind"))?;
        sequence.next_element::<Option<usize>>()?;
        Ok(())
    }
}
