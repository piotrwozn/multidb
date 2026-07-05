use std::collections::{BTreeMap, BTreeSet};

use crate::{
    cdc::{self, ChangeOp, ChangefeedFilter, ChangefeedOptions, ChangefeedTarget, ResumeToken},
    db::CatalogEntry,
    keyenc,
    model::{
        CollectionId, DOCUMENT_TABLE, DocumentId, FieldPath, Value, decode_value, extract_path,
    },
    repl::{Op, ReadConsistency, ReplError, Replication, propose_system_batch},
    storage::{Bytes, StorageError},
};

pub const GEO_POINTS_TABLE: &str = "geo_points";
pub const GEO_INDEX_TABLE: &str = "geo_indexes";
pub const GEO_META_TABLE: &str = "__geo_meta";

const EARTH_RADIUS_METERS: f64 = 6_371_000.0;

#[derive(Clone, Copy, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct GeoPoint {
    pub lon: f64,
    pub lat: f64,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct GeoIndexConfig {
    pub name: String,
    pub collection_id: CollectionId,
    pub path: FieldPath,
    pub precision: u8,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GeoHit {
    pub id: DocumentId,
    pub point: GeoPoint,
    pub distance_meters: f64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct GeoIndexState {
    pub refreshed_to: ResumeToken,
    pub indexed_points: usize,
}

#[derive(thiserror::Error, Debug)]
pub enum GeoError {
    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("cdc: {0}")]
    Cdc(#[from] cdc::FeedError),

    #[error("invalid geo value: {0}")]
    InvalidPoint(String),

    #[error("metadata serialization: {0}")]
    Serde(String),
}

pub struct GeoIndex<'repl, R: Replication + ?Sized> {
    repl: &'repl R,
    config: GeoIndexConfig,
}

impl GeoPoint {
    /// Creates a validated lon/lat point.
    /// # Errors
    /// Fails when coordinates are out of range or not finite.
    pub fn new(lon: f64, lat: f64) -> Result<Self, GeoError> {
        let point = Self { lon, lat };
        validate_point(point)?;
        Ok(point)
    }
}

impl GeoIndexConfig {
    #[must_use]
    pub fn new(name: impl Into<String>, collection_id: CollectionId, path: FieldPath) -> Self {
        Self {
            name: name.into(),
            collection_id,
            path,
            precision: 6,
        }
    }
}

impl<'repl, R: Replication + ?Sized> GeoIndex<'repl, R> {
    /// Opens a geo index handle.
    /// # Errors
    /// Fails when config is invalid.
    pub fn new(repl: &'repl R, config: GeoIndexConfig) -> Result<Self, GeoError> {
        if config.name.is_empty() {
            return Err(GeoError::InvalidPoint(
                "index name cannot be empty".to_owned(),
            ));
        }
        Ok(Self { repl, config })
    }

    /// Persists index metadata.
    /// # Errors
    /// Fails when metadata cannot be written.
    pub fn create_metadata(&self) -> Result<(), GeoError> {
        propose_system_batch(self.repl, Self::metadata_ops(&self.config)?)?;
        Ok(())
    }

    /// Builds system metadata writes for atomic catalog DDL.
    /// # Errors
    /// Fails when metadata cannot be serialized.
    pub fn metadata_ops(config: &GeoIndexConfig) -> Result<Vec<Op>, GeoError> {
        let state = GeoIndexState {
            refreshed_to: ResumeToken::default(),
            indexed_points: 0,
        };
        Ok(vec![
            put_json_op(GEO_META_TABLE, meta_config_key(&config.name), config)?,
            put_json_op(GEO_META_TABLE, meta_state_key(&config.name), &state)?,
        ])
    }

    /// Rebuilds the derived geo index from documents.
    /// # Errors
    /// Fails when documents cannot be read or index writes fail.
    pub fn refresh_full(&self) -> Result<usize, GeoError> {
        let (prefix, end) = keyenc::u32_prefix_range(self.config.collection_id.as_u32());
        let rows = self
            .repl
            .range(DOCUMENT_TABLE, &prefix, &end, ReadConsistency::Strong)?;
        let refreshed_to = cdc::current_resume_token(self.repl)?;
        let mut ops = clear_index_ops(self.repl, &self.config.name)?;
        let mut indexed = 0_usize;
        for (key, value) in rows {
            let Some(id) = document_id_from_key(&key) else {
                continue;
            };
            if let Some(point) = document_point(&value, &self.config.path)? {
                append_point_ops(&self.config, id, point, &mut ops)?;
                indexed += 1;
            }
        }
        let state = GeoIndexState {
            refreshed_to,
            indexed_points: indexed,
        };
        ops.push(put_json_op(
            GEO_META_TABLE,
            meta_state_key(&self.config.name),
            &state,
        )?);
        propose_system_batch(self.repl, ops)?;
        Ok(indexed)
    }

    /// Replays committed document changes after the stored LSN.
    /// # Errors
    /// Fails when CDC, document decoding, or index writes fail.
    pub fn refresh_incremental(
        &self,
        catalog: &BTreeMap<String, CatalogEntry>,
    ) -> Result<GeoIndexState, GeoError> {
        let Some(collection_name) = collection_name_for(catalog, self.config.collection_id) else {
            self.refresh_full()?;
            return self.state().map(Option::unwrap_or_default);
        };

        let mut state = self.state()?.unwrap_or_default();
        let target_lsn = cdc::current_resume_token(self.repl)?.lsn;
        while state.refreshed_to.lsn < target_lsn {
            let filter = ChangefeedFilter {
                target: ChangefeedTarget::Collection(collection_name.clone()),
            };
            let (events, next) = cdc::poll_changefeed(
                self.repl,
                catalog,
                &state.refreshed_to,
                &filter,
                &ChangefeedOptions::default(),
                1_024,
            )?;
            if next.lsn == state.refreshed_to.lsn {
                break;
            }

            let mut ops = Vec::new();
            for event in events {
                match event.op {
                    ChangeOp::Upsert { key, value_after } => {
                        let Some(id) = document_id_from_key(&key) else {
                            continue;
                        };
                        remove_existing_point_ops(self.repl, &self.config, id, &mut ops)?;
                        if let Some(point) = document_point(&value_after, &self.config.path)? {
                            append_point_ops(&self.config, id, point, &mut ops)?;
                        }
                    }
                    ChangeOp::Delete { key } => {
                        if let Some(id) = document_id_from_key(&key) {
                            remove_existing_point_ops(self.repl, &self.config, id, &mut ops)?;
                        }
                    }
                    ChangeOp::TxBegin | ChangeOp::TxCommit | ChangeOp::Ddl { .. } => {}
                }
            }
            state.refreshed_to = next;
            state.indexed_points = count_points_after_ops(self.repl, &self.config.name, &ops)?;
            ops.push(put_json_op(
                GEO_META_TABLE,
                meta_state_key(&self.config.name),
                &state,
            )?);
            propose_system_batch(self.repl, ops)?;
        }
        Ok(state)
    }

    /// Finds points in radius using filter/refine semantics.
    /// # Errors
    /// Fails when index entries cannot be read.
    pub fn within_radius(
        &self,
        center: GeoPoint,
        radius_meters: f64,
    ) -> Result<Vec<GeoHit>, GeoError> {
        validate_point(center)?;
        if !radius_meters.is_finite() || radius_meters < 0.0 {
            return Err(GeoError::InvalidPoint(
                "radius must be a non-negative finite number".to_owned(),
            ));
        }
        let bbox = bounding_box(center, radius_meters);
        let mut hits = self.within_bbox(bbox.0, bbox.1)?;
        hits.retain(|hit| hit.distance_meters <= radius_meters);
        hits.sort_by(|left, right| {
            left.distance_meters
                .total_cmp(&right.distance_meters)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(hits)
    }

    /// Finds points in a bounding box and computes distance from the box center.
    /// # Errors
    /// Fails when storage cannot be read.
    pub fn within_bbox(&self, min: GeoPoint, max: GeoPoint) -> Result<Vec<GeoHit>, GeoError> {
        validate_point(min)?;
        validate_point(max)?;
        let prefix = index_prefix(&self.config.name);
        let end = keyenc::range_end(&prefix);
        let rows = self
            .repl
            .range(GEO_INDEX_TABLE, &prefix, &end, ReadConsistency::Strong)?;
        let center = GeoPoint {
            lon: midpoint_lon(min.lon, max.lon),
            lat: f64::midpoint(min.lat, max.lat),
        };
        let mut hits = Vec::new();
        for (key, value) in rows {
            let Some(id) = trailing_doc_id(&key) else {
                continue;
            };
            let point = if value.is_empty() {
                self.read_point(id)?
            } else {
                Some(
                    serde_json::from_slice(&value)
                        .map_err(|error| GeoError::Serde(error.to_string()))?,
                )
            };
            let Some(point) = point else {
                continue;
            };
            if point_in_bbox(point, min, max) {
                hits.push(GeoHit {
                    id,
                    point,
                    distance_meters: haversine_meters(center, point),
                });
            }
        }
        Ok(hits)
    }

    /// Reads persisted refresh state.
    /// # Errors
    /// Fails when state metadata is corrupt.
    pub fn state(&self) -> Result<Option<GeoIndexState>, GeoError> {
        read_json(
            self.repl,
            GEO_META_TABLE,
            &meta_state_key(&self.config.name),
        )
    }

    fn read_point(&self, id: DocumentId) -> Result<Option<GeoPoint>, GeoError> {
        read_json(
            self.repl,
            GEO_POINTS_TABLE,
            &point_key(&self.config.name, id),
        )
    }
}

#[must_use]
pub fn haversine_meters(a: GeoPoint, b: GeoPoint) -> f64 {
    let d_lat = (b.lat - a.lat).to_radians();
    let d_lon = (b.lon - a.lon).to_radians();
    let lat1 = a.lat.to_radians();
    let lat2 = b.lat.to_radians();
    let h = (d_lat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (d_lon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_METERS * h.sqrt().asin()
}

fn document_point(bytes: &[u8], path: &FieldPath) -> Result<Option<GeoPoint>, GeoError> {
    let value = decode_value(bytes)?;
    match extract_path(&value, path) {
        Some(value) => value_to_point(value),
        None => Ok(None),
    }
}

fn value_to_point(value: &Value) -> Result<Option<GeoPoint>, GeoError> {
    match value {
        Value::GeoPoint { lon, lat } => GeoPoint::new(*lon, *lat).map(Some),
        Value::Object(map) => {
            let (Some(Value::Float(lon)), Some(Value::Float(lat))) =
                (map.get("lon"), map.get("lat"))
            else {
                return Ok(None);
            };
            GeoPoint::new(*lon, *lat).map(Some)
        }
        _ => Ok(None),
    }
}

fn append_point_ops(
    config: &GeoIndexConfig,
    id: DocumentId,
    point: GeoPoint,
    ops: &mut Vec<Op>,
) -> Result<(), GeoError> {
    let point_bytes =
        serde_json::to_vec(&point).map_err(|error| GeoError::Serde(error.to_string()))?;
    ops.push(Op::Put {
        table: GEO_POINTS_TABLE.to_owned(),
        key: point_key(&config.name, id),
        value: point_bytes.clone(),
    });
    ops.push(Op::Put {
        table: GEO_INDEX_TABLE.to_owned(),
        key: geo_index_key(config, point, id),
        value: point_bytes,
    });
    Ok(())
}

fn remove_existing_point_ops<R: Replication + ?Sized>(
    repl: &R,
    config: &GeoIndexConfig,
    id: DocumentId,
    ops: &mut Vec<Op>,
) -> Result<(), GeoError> {
    let key = point_key(&config.name, id);
    let Some(bytes) = repl.read(GEO_POINTS_TABLE, &key, ReadConsistency::Strong)? else {
        return Ok(());
    };
    let point: GeoPoint =
        serde_json::from_slice(&bytes).map_err(|error| GeoError::Serde(error.to_string()))?;
    ops.push(Op::Delete {
        table: GEO_POINTS_TABLE.to_owned(),
        key,
    });
    ops.push(Op::Delete {
        table: GEO_INDEX_TABLE.to_owned(),
        key: geo_index_key(config, point, id),
    });
    Ok(())
}

fn clear_index_ops<R: Replication + ?Sized>(repl: &R, index: &str) -> Result<Vec<Op>, GeoError> {
    let mut ops = Vec::new();
    for table in [GEO_POINTS_TABLE, GEO_INDEX_TABLE] {
        let prefix = index_prefix(index);
        let end = keyenc::range_end(&prefix);
        for (key, _) in repl.range(table, &prefix, &end, ReadConsistency::Strong)? {
            ops.push(Op::Delete {
                table: table.to_owned(),
                key,
            });
        }
    }
    Ok(ops)
}

fn count_points_after_ops<R: Replication + ?Sized>(
    repl: &R,
    index: &str,
    ops: &[Op],
) -> Result<usize, GeoError> {
    let prefix = index_prefix(index);
    let end = keyenc::range_end(&prefix);
    let mut points = repl
        .range(GEO_POINTS_TABLE, &prefix, &end, ReadConsistency::Strong)?
        .into_iter()
        .map(|(key, _)| key)
        .collect::<BTreeSet<_>>();
    for op in ops {
        match op {
            Op::Put { table, key, .. } if table == GEO_POINTS_TABLE => {
                points.insert(key.clone());
            }
            Op::Delete { table, key } if table == GEO_POINTS_TABLE => {
                points.remove(key);
            }
            _ => {}
        }
    }
    Ok(points.len())
}

fn put_json_op<T: serde::Serialize>(table: &str, key: Bytes, value: &T) -> Result<Op, GeoError> {
    Ok(Op::Put {
        table: table.to_owned(),
        key,
        value: serde_json::to_vec(value).map_err(|error| GeoError::Serde(error.to_string()))?,
    })
}

fn read_json<T: serde::de::DeserializeOwned, R: Replication + ?Sized>(
    repl: &R,
    table: &str,
    key: &[u8],
) -> Result<Option<T>, GeoError> {
    let Some(bytes) = repl.read(table, key, ReadConsistency::Strong)? else {
        return Ok(None);
    };
    Ok(Some(
        serde_json::from_slice(&bytes).map_err(|error| GeoError::Serde(error.to_string()))?,
    ))
}

fn collection_name_for(
    catalog: &BTreeMap<String, CatalogEntry>,
    collection_id: CollectionId,
) -> Option<String> {
    catalog.iter().find_map(|(name, entry)| match entry {
        CatalogEntry::Collection {
            collection_id: candidate,
            ..
        } if *candidate == collection_id => Some(name.clone()),
        _ => None,
    })
}

fn validate_point(point: GeoPoint) -> Result<(), GeoError> {
    if !point.lon.is_finite() || !point.lat.is_finite() {
        return Err(GeoError::InvalidPoint(
            "coordinates must be finite".to_owned(),
        ));
    }
    if !(-180.0..=180.0).contains(&point.lon) || !(-90.0..=90.0).contains(&point.lat) {
        return Err(GeoError::InvalidPoint(
            "coordinates out of lon/lat range".to_owned(),
        ));
    }
    Ok(())
}

fn bounding_box(center: GeoPoint, radius_meters: f64) -> (GeoPoint, GeoPoint) {
    let lat_delta = (radius_meters / EARTH_RADIUS_METERS).to_degrees();
    let lon_delta = if center.lat.abs() >= 89.999 {
        180.0
    } else {
        (radius_meters / (EARTH_RADIUS_METERS * center.lat.to_radians().cos().abs())).to_degrees()
    };
    (
        GeoPoint {
            lon: normalize_lon(center.lon - lon_delta),
            lat: (center.lat - lat_delta).max(-90.0),
        },
        GeoPoint {
            lon: normalize_lon(center.lon + lon_delta),
            lat: (center.lat + lat_delta).min(90.0),
        },
    )
}

fn point_in_bbox(point: GeoPoint, min: GeoPoint, max: GeoPoint) -> bool {
    let lon_ok = if min.lon <= max.lon {
        point.lon >= min.lon && point.lon <= max.lon
    } else {
        point.lon >= min.lon || point.lon <= max.lon
    };
    lon_ok && point.lat >= min.lat && point.lat <= max.lat
}

fn midpoint_lon(min: f64, max: f64) -> f64 {
    if min <= max {
        f64::midpoint(min, max)
    } else {
        normalize_lon((min + max + 360.0) / 2.0)
    }
}

fn normalize_lon(mut lon: f64) -> f64 {
    while lon < -180.0 {
        lon += 360.0;
    }
    while lon > 180.0 {
        lon -= 360.0;
    }
    lon
}

fn geo_hash(point: GeoPoint, precision: u8) -> String {
    let scale = 10_f64.powi(i32::from(precision.min(9)));
    let lon = ((point.lon + 180.0) * scale).floor();
    let lat = ((point.lat + 90.0) * scale).floor();
    format!("{lon:020.0}:{lat:020.0}")
}

fn index_prefix(index: &str) -> Bytes {
    let mut key = Vec::new();
    keyenc::push_len_bytes(&mut key, index.as_bytes());
    key
}

fn point_key(index: &str, id: DocumentId) -> Bytes {
    let mut key = index_prefix(index);
    key.extend_from_slice(&id.as_bytes());
    key
}

fn geo_index_key(config: &GeoIndexConfig, point: GeoPoint, id: DocumentId) -> Bytes {
    let mut key = index_prefix(&config.name);
    keyenc::push_len_bytes(&mut key, geo_hash(point, config.precision).as_bytes());
    key.extend_from_slice(&id.as_bytes());
    key
}

fn meta_config_key(index: &str) -> Bytes {
    let mut key = b"config:".to_vec();
    key.extend_from_slice(index.as_bytes());
    key
}

fn meta_state_key(index: &str) -> Bytes {
    let mut key = b"state:".to_vec();
    key.extend_from_slice(index.as_bytes());
    key
}

fn trailing_doc_id(key: &[u8]) -> Option<DocumentId> {
    if key.len() < 16 {
        return None;
    }
    let mut raw = [0_u8; 16];
    raw.copy_from_slice(&key[key.len() - 16..]);
    Some(DocumentId::from_bytes(raw))
}

fn document_id_from_key(key: &[u8]) -> Option<DocumentId> {
    if key.len() != 20 {
        return None;
    }
    let mut raw = [0_u8; 16];
    raw.copy_from_slice(&key[4..20]);
    Some(DocumentId::from_bytes(raw))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, sync::Arc};

    use crate::{
        db::{DbConfig, Profile, create_database},
        model::{DocumentCollection, FieldPath, Value},
        repl::Replication,
    };

    use super::{GeoIndex, GeoIndexConfig, GeoPoint, haversine_meters};

    #[test]
    fn geo_radius_uses_haversine_and_handles_antimeridian() -> Result<(), Box<dyn std::error::Error>>
    {
        let warsaw = GeoPoint::new(21.0122, 52.2297)?;
        let london = GeoPoint::new(-0.1276, 51.5072)?;
        assert!(haversine_meters(warsaw, london) > 1_000_000.0);

        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let repl: Arc<dyn Replication> = Arc::new(database);
        let docs = DocumentCollection::new(repl.as_ref(), crate::model::CollectionId::new(9));
        let near = docs.insert(&doc(179.9, 0.0))?;
        docs.insert(&doc(10.0, 0.0))?;
        let index = GeoIndex::new(
            repl.as_ref(),
            GeoIndexConfig::new(
                "places_geo",
                crate::model::CollectionId::new(9),
                FieldPath::new(["point"]),
            ),
        )?;
        assert_eq!(index.refresh_full()?, 2);
        let hits = index.within_radius(GeoPoint::new(-179.9, 0.0)?, 50_000.0)?;
        assert_eq!(hits.first().map(|hit| hit.id), Some(near));
        docs.delete(near)?;
        assert_eq!(index.refresh_full()?, 1);
        assert!(
            index
                .within_radius(GeoPoint::new(-179.9, 0.0)?, 50_000.0)?
                .is_empty()
        );
        Ok(())
    }

    fn doc(lon: f64, lat: f64) -> Value {
        Value::Object(BTreeMap::from([(
            "point".to_owned(),
            Value::GeoPoint { lon, lat },
        )]))
    }
}
