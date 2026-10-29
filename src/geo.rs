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
/// Source documents read per page, and indexed documents buffered per batch, while
/// rebuilding an index.
const REFRESH_PAGE_ROWS: usize = 256;
/// Index writes buffered before a rebuild flushes a batch.
const REFRESH_PAGE_OPS: usize = 4_096;
/// Index keys deleted per batch while clearing an index.
const CLEAR_PAGE_KEYS: usize = 1_024;

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
    /// Number of indexed points at the last rebuild, backfill or incremental refresh.
    pub indexed_points: usize,
    /// Documents skipped by the last rebuild or backfill because the indexed field held
    /// something other than a valid lon/lat point. Missing and `Null` fields are not counted.
    #[serde(default)]
    pub skipped_documents: usize,
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
    commit_log_available: bool,
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
        Ok(Self {
            repl,
            config,
            commit_log_available: repl.supports_commit_log(),
        })
    }

    /// Records whether the backing replication writes the commit log that
    /// incremental refresh replays.
    #[must_use]
    pub(crate) const fn with_commit_log(mut self, available: bool) -> Self {
        self.commit_log_available = available;
        self
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
        Self::metadata_ops_with_state(config, &GeoIndexState::default())
    }

    /// Builds system metadata writes that publish `state` together with the config.
    /// # Errors
    /// Fails when metadata cannot be serialized.
    pub(crate) fn metadata_ops_with_state(
        config: &GeoIndexConfig,
        state: &GeoIndexState,
    ) -> Result<Vec<Op>, GeoError> {
        Ok(vec![
            put_json_op(GEO_META_TABLE, meta_config_key(&config.name), config)?,
            put_json_op(GEO_META_TABLE, meta_state_key(&config.name), state)?,
        ])
    }

    /// Rebuilds the derived geo index from documents and records the refresh state.
    /// Returns the number of indexed points.
    ///
    /// This is a repair tool: document writes made through `Database::collection` handles
    /// already maintain the index in their own batch. The rebuild clears and rewrites the
    /// index in bounded batches, so readers can see partial results while it runs. The
    /// resume token is taken before the scan, so `refresh_incremental` replays writes that
    /// land during the rebuild. Documents with an invalid point are skipped and counted in
    /// `GeoIndexState::skipped_documents`.
    /// # Errors
    /// Fails when documents cannot be read or decoded, or index writes fail.
    pub fn refresh_full(&self) -> Result<usize, GeoError> {
        let state = self.rebuild_entries()?;
        propose_system_batch(
            self.repl,
            vec![put_json_op(
                GEO_META_TABLE,
                meta_state_key(&self.config.name),
                &state,
            )?],
        )?;
        Ok(state.indexed_points)
    }

    /// Clears and rebuilds the index entries in bounded batches without writing the
    /// refresh state.
    /// # Errors
    /// Fails when documents cannot be read or decoded, or index writes fail.
    pub(crate) fn rebuild_entries(&self) -> Result<GeoIndexState, GeoError> {
        // Take the token before scanning: a write that commits during the scan is then
        // replayed by `refresh_incremental` instead of being lost.
        let refreshed_to = if self.repl.supports_commit_log() {
            cdc::current_resume_token(self.repl)?
        } else {
            ResumeToken::default()
        };
        clear_index_entries(self.repl, &self.config.name)?;

        let (mut cursor, end) = keyenc::u32_prefix_range(self.config.collection_id.as_u32());
        let mut state = GeoIndexState {
            refreshed_to,
            ..GeoIndexState::default()
        };
        let mut ops = Vec::new();
        let mut buffered_docs = 0_usize;
        loop {
            let page = scan_page(self.repl, DOCUMENT_TABLE, &cursor, &end, REFRESH_PAGE_ROWS)?;
            for (key, value) in &page {
                let Some(id) = document_id_from_key(key) else {
                    continue;
                };
                if !self.append_rebuild_ops(id, value, &mut state, &mut ops)? {
                    continue;
                }
                buffered_docs += 1;
                if buffered_docs >= REFRESH_PAGE_ROWS || ops.len() >= REFRESH_PAGE_OPS {
                    propose_system_batch(self.repl, std::mem::take(&mut ops))?;
                    buffered_docs = 0;
                }
            }
            let Some(next) = next_page_start(&page, REFRESH_PAGE_ROWS, &end) else {
                break;
            };
            cursor = next;
        }
        if !ops.is_empty() {
            propose_system_batch(self.repl, ops)?;
        }
        if state.skipped_documents > 0 {
            tracing::warn!(
                index = %self.config.name,
                skipped = state.skipped_documents,
                "geo rebuild skipped documents with invalid points"
            );
        }
        Ok(state)
    }

    /// Queues the entries of one source document during a rebuild and returns whether a
    /// point was indexed. Invalid points are counted instead of aborting the rebuild.
    fn append_rebuild_ops(
        &self,
        id: DocumentId,
        bytes: &[u8],
        state: &mut GeoIndexState,
        ops: &mut Vec<Op>,
    ) -> Result<bool, GeoError> {
        let doc = decode_value(bytes)?;
        match extract_path(&doc, &self.config.path) {
            None | Some(Value::Null) => Ok(false),
            Some(value) => {
                let Some(point) = value_to_point(value) else {
                    state.skipped_documents += 1;
                    return Ok(false);
                };
                append_point_ops(&self.config, id, point, ops)?;
                state.indexed_points += 1;
                Ok(true)
            }
        }
    }

    /// Replays committed document changes after the stored LSN.
    ///
    /// This is a catch-up/repair tool for raw-key writes that bypass `Database::collection`
    /// handles (stored procedures, raw batches). Changes to the same document within one
    /// changefeed page are coalesced, so only its final point is indexed; invalid points
    /// are not indexed.
    /// # Errors
    /// Fails when the database uses AP replication (no commit log to replay; use
    /// `refresh_full`), or CDC, document decoding, or index writes fail.
    pub fn refresh_incremental(
        &self,
        catalog: &BTreeMap<String, CatalogEntry>,
    ) -> Result<GeoIndexState, GeoError> {
        if !self.commit_log_available || !self.repl.supports_commit_log() {
            return Err(GeoError::Repl(ReplError::Unsupported(
                "AP replication does not support incremental geo refresh: AP writes are not \
                 recorded in the commit log; use refresh_full"
                    .to_owned(),
            )));
        }
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
            for (id, value_after) in coalesce_document_changes(events) {
                remove_existing_point_ops(self.repl, &self.config, id, &mut ops)?;
                if let Some(value_after) = value_after
                    && let Some(point) = document_point(&value_after, &self.config.path)?
                {
                    append_point_ops(&self.config, id, point, &mut ops)?;
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
        for hit in &mut hits {
            hit.distance_meters = haversine_meters(center, hit.point);
        }
        hits.retain(|hit| hit.distance_meters <= radius_meters);
        hits.sort_by(|left, right| {
            left.distance_meters
                .total_cmp(&right.distance_meters)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(hits)
    }

    /// Finds points in a bounding box and computes distance from the box center.
    ///
    /// Each candidate cell entry is verified against the document's stored point row, so
    /// stale cell entries never produce hits.
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
            if !value.is_empty() {
                let cell_point: GeoPoint = serde_json::from_slice(&value)
                    .map_err(|error| GeoError::Serde(error.to_string()))?;
                if !point_in_bbox(cell_point, min, max) {
                    continue;
                }
            }
            // Only trust a cell entry while the point row still maps to it; a stale entry
            // left by a bypassing write or an interrupted rebuild is skipped.
            let Some(point) = self.read_point(id)? else {
                continue;
            };
            if geo_index_key(&self.config, point, id) != key || !point_in_bbox(point, min, max) {
                continue;
            }
            hits.push(GeoHit {
                id,
                point,
                distance_meters: haversine_meters(center, point),
            });
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
    let d_lon = normalize_lon(b.lon - a.lon).to_radians();
    let lat1 = a.lat.to_radians();
    let lat2 = b.lat.to_radians();
    let cos1 = if a.lat.abs() >= 90.0 { 0.0 } else { lat1.cos() };
    let cos2 = if b.lat.abs() >= 90.0 { 0.0 } else { lat2.cos() };
    let h = (d_lat / 2.0).sin().powi(2) + cos1 * cos2 * (d_lon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS_METERS * h.clamp(0.0, 1.0).sqrt().asin()
}

/// Appends the index writes for one document change.
///
/// The writes are a diff between the `old` and `new` document values, so they can be
/// queued in the same batch as the document write itself. Nothing is emitted when the
/// indexed point is unchanged; invalid points are not indexed.
/// # Errors
/// Fails when the point cannot be serialized.
pub(crate) fn append_document_change_ops(
    config: &GeoIndexConfig,
    id: DocumentId,
    old: Option<&Value>,
    new: Option<&Value>,
    ops: &mut Vec<Op>,
) -> Result<(), GeoError> {
    let old_point = old.and_then(|doc| value_point(doc, &config.path));
    let new_point = new.and_then(|doc| value_point(doc, &config.path));
    if old_point == new_point {
        return Ok(());
    }

    if let Some(old_point) = old_point {
        let old_key = geo_index_key(config, old_point, id);
        if new_point.is_none_or(|new_point| geo_index_key(config, new_point, id) != old_key) {
            ops.push(Op::Delete {
                table: GEO_INDEX_TABLE.to_owned(),
                key: old_key,
            });
        }
        if new_point.is_none() {
            ops.push(Op::Delete {
                table: GEO_POINTS_TABLE.to_owned(),
                key: point_key(&config.name, id),
            });
        }
    }
    if let Some(new_point) = new_point {
        append_point_ops(config, id, new_point, ops)?;
    }
    Ok(())
}

fn document_point(bytes: &[u8], path: &FieldPath) -> Result<Option<GeoPoint>, GeoError> {
    let value = decode_value(bytes)?;
    Ok(value_point(&value, path))
}

fn value_point(doc: &Value, path: &FieldPath) -> Option<GeoPoint> {
    extract_path(doc, path).and_then(value_to_point)
}

/// Reads a `Value::GeoPoint` or a `{lon, lat}` object with integer or float coordinates.
/// Returns `None` for any other value and for out-of-range or non-finite coordinates.
fn value_to_point(value: &Value) -> Option<GeoPoint> {
    let (lon, lat) = match value {
        Value::GeoPoint { lon, lat } => (*lon, *lat),
        Value::Object(map) => (coordinate(map.get("lon")?)?, coordinate(map.get("lat")?)?),
        _ => return None,
    };
    GeoPoint::new(lon, lat).ok()
}

fn coordinate(value: &Value) -> Option<f64> {
    match value {
        Value::Float(value) => Some(*value),
        Value::Int(value) => i32::try_from(*value).ok().map(f64::from),
        _ => None,
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

/// Keeps the final version of each document changed in one changefeed page; `None`
/// marks a delete.
fn coalesce_document_changes(
    events: impl IntoIterator<Item = cdc::ChangeEvent>,
) -> BTreeMap<DocumentId, Option<Bytes>> {
    let mut changes = BTreeMap::new();
    for event in events {
        match event.op {
            ChangeOp::Upsert { key, value_after } => {
                if let Some(id) = document_id_from_key(&key) {
                    changes.insert(id, Some(value_after));
                }
            }
            ChangeOp::Delete { key } => {
                if let Some(id) = document_id_from_key(&key) {
                    changes.insert(id, None);
                }
            }
            ChangeOp::TxBegin | ChangeOp::TxCommit | ChangeOp::Ddl { .. } => {}
        }
    }
    changes
}

/// Deletes every entry of `index` in bounded batches.
fn clear_index_entries<R: Replication + ?Sized>(repl: &R, index: &str) -> Result<(), GeoError> {
    let prefix = index_prefix(index);
    let end = keyenc::range_end(&prefix);
    for table in [GEO_POINTS_TABLE, GEO_INDEX_TABLE] {
        let mut cursor = prefix.clone();
        loop {
            let page = scan_page(repl, table, &cursor, &end, CLEAR_PAGE_KEYS)?;
            let next = next_page_start(&page, CLEAR_PAGE_KEYS, &end);
            let ops = page
                .into_iter()
                .map(|(key, _)| Op::Delete {
                    table: table.to_owned(),
                    key,
                })
                .collect::<Vec<_>>();
            if !ops.is_empty() {
                propose_system_batch(repl, ops)?;
            }
            let Some(next) = next else {
                break;
            };
            cursor = next;
        }
    }
    Ok(())
}

/// Reads at most `rows` entries starting at `start`.
fn scan_page<R: Replication + ?Sized>(
    repl: &R,
    table: &str,
    start: &[u8],
    end: &[u8],
    rows: usize,
) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
    let mut page = Vec::new();
    repl.scan_range_batches(
        table,
        start,
        end,
        ReadConsistency::Strong,
        rows,
        &|| false,
        &mut |batch| {
            page.extend_from_slice(batch);
            Ok(false)
        },
    )?;
    Ok(page)
}

/// Returns the first key after a full page, or `None` when the range is exhausted.
fn next_page_start(page: &[(Bytes, Bytes)], rows: usize, end: &[u8]) -> Option<Bytes> {
    if page.len() < rows {
        return None;
    }
    let (last, _) = page.last()?;
    let mut next = last.clone();
    next.push(0);
    (end.is_empty() || next.as_slice() < end).then_some(next)
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
    // The angular radius never needs to exceed a half circumference. A cap that
    // touches either pole includes every longitude. Other caps use the spherical
    // longitude extremum, rather than a tangent-plane approximation.
    let angular = (radius_meters / EARTH_RADIUS_METERS).min(std::f64::consts::PI);
    let padding = 1.0e-9; // Conservative outward rounding; the exact distance filters hits.
    let lat_delta = angular.to_degrees() + padding;
    let min_lat = (center.lat - lat_delta).max(-90.0);
    let max_lat = (center.lat + lat_delta).min(90.0);
    if min_lat <= -90.0 || max_lat >= 90.0 {
        return (
            GeoPoint {
                lon: -180.0,
                lat: min_lat,
            },
            GeoPoint {
                lon: 180.0,
                lat: max_lat,
            },
        );
    }
    let lon_delta = (angular.sin() / center.lat.to_radians().cos())
        .clamp(-1.0, 1.0)
        .asin()
        .to_degrees()
        + padding;
    (
        GeoPoint {
            lon: normalize_lon(center.lon - lon_delta),
            lat: min_lat,
        },
        GeoPoint {
            lon: normalize_lon(center.lon + lon_delta),
            lat: max_lat,
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

fn normalize_lon(lon: f64) -> f64 {
    if (-180.0..=180.0).contains(&lon) {
        return lon;
    }
    let wrapped = lon.rem_euclid(360.0);
    if wrapped > 180.0 {
        wrapped - 360.0
    } else if wrapped >= 180.0 && lon < 0.0 {
        -180.0
    } else {
        wrapped
    }
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
    use std::{
        collections::{BTreeMap, BTreeSet},
        sync::Arc,
    };

    use crate::{
        db::{Database, DbConfig, Profile, create_database},
        keyenc,
        model::{CollectionId, DocumentCollection, DocumentId, FieldPath, Value},
        query::{ColumnType, DocField},
        repl::{Op, ReadConsistency, Replication},
    };

    use super::{
        GEO_INDEX_TABLE, GEO_POINTS_TABLE, GeoHit, GeoIndex, GeoIndexConfig, GeoPoint,
        geo_index_key, haversine_meters, index_prefix,
    };

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    const WARSAW: (f64, f64) = (21.0122, 52.2297);
    const LONDON: (f64, f64) = (-0.1276, 51.5072);
    const PARIS: (f64, f64) = (2.3522, 48.8566);
    const BERLIN: (f64, f64) = (13.405, 52.52);
    const ROME: (f64, f64) = (12.4964, 41.9028);

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
                .is_empty(),
            "expected an empty value, got {:?}",
            index.within_radius(GeoPoint::new(-179.9, 0.0)?, 50_000.0)?
        );
        Ok(())
    }

    #[test]
    fn huge_finite_radius_covers_the_globe_and_normalization_is_constant_time() -> TestResult {
        for (longitude, expected) in [
            (f64::MAX, 128.0),
            (-f64::MAX, -128.0),
            (1.0e30, 16.0),
            (-1.0e30, -16.0),
        ] {
            let wrapped = super::normalize_lon(longitude);
            assert!(
                (-180.0..=180.0).contains(&wrapped),
                "{longitude} became {wrapped}"
            );
            assert!((wrapped - expected).abs() <= f64::EPSILON);
        }
        let mut database = places_database()?;
        let ids = [(0.0, 0.0), (180.0, 0.0), (-179.0, -89.0)]
            .map(|(lon, lat)| {
                database
                    .collection("places")
                    .and_then(|collection| collection.insert(&doc(lon, lat)).map_err(Into::into))
            })
            .into_iter()
            .collect::<Result<BTreeSet<_>, _>>()?;
        database.create_geo_index(places_geo_config())?;
        let hits = database
            .geo_index("places_geo")?
            .within_radius(GeoPoint::new(0.0, 80.0)?, f64::MAX)?;
        assert_eq!(hits.iter().map(|hit| hit.id).collect::<BTreeSet<_>>(), ids);
        assert!(hits.iter().all(|hit| hit.distance_meters.is_finite()));
        Ok(())
    }

    #[test]
    fn polar_radius_bounds_include_spherical_caps_and_use_the_original_center() -> TestResult {
        for sign in [1.0, -1.0] {
            let mut database = places_database()?;
            let collection = database.collection("places")?;
            let across_pole = collection.insert(&doc(180.0, sign * 89.0))?;
            let same_meridian = collection.insert(&doc(0.0, sign * 89.0))?;
            let curved_cap = collection.insert(&doc(60.0, sign * 85.0))?;
            database.create_geo_index(places_geo_config())?;
            let index = database.geo_index("places_geo")?;
            let center = GeoPoint::new(0.0, sign * 80.0)?;
            let hits = index.within_radius(center, 2_000_000.0)?;
            assert_eq!(
                hits.iter().map(|hit| hit.id).collect::<BTreeSet<_>>(),
                BTreeSet::from([across_pole, same_meridian, curved_cap])
            );
            for hit in hits {
                assert!((hit.distance_meters - haversine_meters(center, hit.point)).abs() < 1e-6);
            }
            let narrow = index.within_radius(center, 1_000_000.0)?;
            assert_eq!(
                narrow.iter().map(|hit| hit.id).collect::<Vec<_>>(),
                vec![curved_cap]
            );
        }
        Ok(())
    }

    #[test]
    fn dateline_bounds_preserve_nearby_hits_and_equivalent_endpoints() -> TestResult {
        let mut database = places_database()?;
        let collection = database.collection("places")?;
        let west = collection.insert(&doc(-179.9, 0.0))?;
        let east = collection.insert(&doc(179.9, 0.0))?;
        let boundary = collection.insert(&doc(-180.0, 0.0))?;
        collection.insert(&doc(0.0, 0.0))?;
        database.create_geo_index(places_geo_config())?;
        let index = database.geo_index("places_geo")?;
        for longitude in [-179.95, 179.95] {
            let hits = index.within_radius(GeoPoint::new(longitude, 0.0)?, 30_000.0)?;
            assert_eq!(
                hits.iter().map(|hit| hit.id).collect::<BTreeSet<_>>(),
                BTreeSet::from([west, east, boundary])
            );
        }
        let hits = index.within_radius(GeoPoint::new(180.0, 0.0)?, 0.0)?;
        assert_eq!(
            hits.iter().map(|hit| hit.id).collect::<Vec<_>>(),
            vec![boundary]
        );
        assert!(hits[0].distance_meters.abs() <= f64::EPSILON);
        assert!(
            haversine_meters(GeoPoint::new(0.0, 90.0)?, GeoPoint::new(180.0, 90.0)?).abs()
                <= f64::EPSILON
        );
        let antipodal = haversine_meters(GeoPoint::new(0.0, -88.03)?, GeoPoint::new(180.0, 88.03)?);
        assert!((antipodal - std::f64::consts::PI * super::EARTH_RADIUS_METERS).abs() < 1e-6);
        Ok(())
    }

    #[test]
    fn geo_index_tracks_point_moves_without_refresh() -> TestResult {
        let mut database = places_database()?;
        let id = database
            .collection("places")?
            .insert(&doc(WARSAW.0, WARSAW.1))?;
        database.create_geo_index(places_geo_config())?;
        let index = database.geo_index("places_geo")?;
        assert_eq!(near(&index, WARSAW)?, BTreeSet::from([id]));

        let places = database.collection("places")?;
        places.update(id, &doc(LONDON.0, LONDON.1))?;
        places.update(id, &doc(PARIS.0, PARIS.1))?;
        assert!(near(&index, WARSAW)?.is_empty());
        assert!(near(&index, LONDON)?.is_empty());
        assert_eq!(near(&index, PARIS)?, BTreeSet::from([id]));
        assert_eq!(entries_for(&database, id)?, (1, 1));

        database.transaction(|txn| {
            txn.update_document("places", id, &doc(BERLIN.0, BERLIN.1))?;
            txn.update_document("places", id, &doc(ROME.0, ROME.1))
        })?;
        assert!(near(&index, BERLIN)?.is_empty());
        assert!(near(&index, PARIS)?.is_empty());
        assert_eq!(near(&index, ROME)?, BTreeSet::from([id]));
        assert_eq!(entries_for(&database, id)?, (1, 1));

        database.transaction(|txn| txn.delete_document("places", id))?;
        assert_eq!(entries_for(&database, id)?, (0, 0));
        assert!(near(&index, ROME)?.is_empty());
        Ok(())
    }

    #[test]
    fn geo_index_accepts_int_coordinates_and_skips_invalid_points() -> TestResult {
        let mut database = places_database()?;
        let int_id = {
            let places = database.collection("places")?;
            let int_id = places.insert(&point_doc(lon_lat(Value::Int(21), Value::Int(52))))?;
            places.insert(&point_doc(lon_lat(Value::Float(500.0), Value::Float(0.0))))?;
            places.insert(&point_doc(Value::Str("nowhere".to_owned())))?;
            places.insert(&point_doc(Value::Null))?;
            int_id
        };
        database.create_geo_index(places_geo_config())?;
        let index = database.geo_index("places_geo")?;
        assert_eq!(near(&index, (21.0, 52.0))?, BTreeSet::from([int_id]));
        assert_eq!(index.state()?.map(|state| state.skipped_documents), Some(2));

        assert_eq!(index.refresh_full()?, 1);
        assert_eq!(index.state()?.map(|state| state.skipped_documents), Some(2));

        let out_of_range = database
            .collection("places")?
            .insert(&point_doc(lon_lat(Value::Float(0.0), Value::Float(95.0))))?;
        assert_eq!(entries_for(&database, out_of_range)?, (0, 0));
        Ok(())
    }

    #[test]
    fn within_radius_ignores_ghost_cell_entries() -> TestResult {
        let mut database = places_database()?;
        let id = database
            .collection("places")?
            .insert(&doc(PARIS.0, PARIS.1))?;
        database.create_geo_index(places_geo_config())?;
        let warsaw = GeoPoint::new(WARSAW.0, WARSAW.1)?;
        Replication::propose_batch(
            &database,
            vec![Op::Put {
                table: GEO_INDEX_TABLE.to_owned(),
                key: geo_index_key(&places_geo_config(), warsaw, id),
                value: serde_json::to_vec(&warsaw)?,
            }],
        )?;
        let index = database.geo_index("places_geo")?;
        assert!(near(&index, WARSAW)?.is_empty());
        assert_eq!(near(&index, PARIS)?, BTreeSet::from([id]));
        Ok(())
    }

    #[test]
    fn geo_refresh_incremental_coalesces_repeated_moves() -> TestResult {
        let mut database = places_database()?;
        database.create_geo_index(places_geo_config())?;
        // A bare handle has no derived indexes attached, like a raw-key writer.
        let raw = DocumentCollection::new(&database, CollectionId::new(9));
        let id = raw.insert(&doc(WARSAW.0, WARSAW.1))?;
        raw.update(id, &doc(LONDON.0, LONDON.1))?;
        raw.update(id, &doc(PARIS.0, PARIS.1))?;

        let index = database.geo_index("places_geo")?;
        index.refresh_incremental(database.catalog())?;
        assert_eq!(entries_for(&database, id)?, (1, 1));
        assert!(near(&index, WARSAW)?.is_empty());
        assert!(near(&index, LONDON)?.is_empty());
        assert_eq!(near(&index, PARIS)?, BTreeSet::from([id]));

        raw.delete(id)?;
        index.refresh_incremental(database.catalog())?;
        assert_eq!(entries_for(&database, id)?, (0, 0));
        Ok(())
    }

    fn places_database() -> Result<Database, Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.create_collection(
            "places",
            CollectionId::new(9),
            vec![DocField::path(
                "name",
                FieldPath::new(["name"]),
                ColumnType::Str,
            )],
            Vec::new(),
        )?;
        Ok(database)
    }

    fn places_geo_config() -> GeoIndexConfig {
        GeoIndexConfig::new(
            "places_geo",
            CollectionId::new(9),
            FieldPath::new(["point"]),
        )
    }

    fn near<R: Replication + ?Sized>(
        index: &GeoIndex<'_, R>,
        (lon, lat): (f64, f64),
    ) -> Result<BTreeSet<DocumentId>, Box<dyn std::error::Error>> {
        let hits: Vec<GeoHit> = index.within_radius(GeoPoint::new(lon, lat)?, 1_000.0)?;
        Ok(hits.into_iter().map(|hit| hit.id).collect())
    }

    /// Counts the `GEO_POINTS_TABLE` and `GEO_INDEX_TABLE` entries of one document.
    fn entries_for(
        database: &Database,
        id: DocumentId,
    ) -> Result<(usize, usize), Box<dyn std::error::Error>> {
        let prefix = index_prefix("places_geo");
        let end = keyenc::range_end(&prefix);
        let mut counts = [0_usize; 2];
        for (count, table) in counts.iter_mut().zip([GEO_POINTS_TABLE, GEO_INDEX_TABLE]) {
            *count = Replication::range(database, table, &prefix, &end, ReadConsistency::Strong)?
                .into_iter()
                .filter(|(key, _)| key.ends_with(&id.as_bytes()))
                .count();
        }
        Ok((counts[0], counts[1]))
    }

    fn lon_lat(lon: Value, lat: Value) -> Value {
        Value::Object(BTreeMap::from([
            ("lon".to_owned(), lon),
            ("lat".to_owned(), lat),
        ]))
    }

    fn point_doc(point: Value) -> Value {
        Value::Object(BTreeMap::from([("point".to_owned(), point)]))
    }

    fn doc(lon: f64, lat: f64) -> Value {
        point_doc(Value::GeoPoint { lon, lat })
    }
}
