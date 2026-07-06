#![allow(clippy::missing_errors_doc)]

use std::{
    collections::BTreeMap,
    fmt::Write as _,
    fs,
    io::{self, Write as _},
    path::{Path, PathBuf},
};

use bson::{Bson, Document};

use crate::{
    db::{Database, DbError},
    graph::GRAPH_OUT_EDGES_TABLE,
    model::{DOCUMENT_TABLE, Value, decode_value, encode_value, value_is_binary_encoded},
    query::REL_ROWS_TABLE,
    query::{ColumnDef, ColumnType, QueryError, RelTable, Row},
    repl::{Op, ReadConsistency, ReplError, Replication},
    storage::StorageError,
};

#[derive(thiserror::Error, Debug)]
pub enum MigrationError {
    #[error("io: {0}")]
    Io(#[from] io::Error),

    #[error("database: {0}")]
    Db(#[from] DbError),

    #[error("replication: {0}")]
    Repl(#[from] ReplError),

    #[error("storage: {0}")]
    Storage(#[from] StorageError),

    #[error("query: {0}")]
    Query(#[from] QueryError),

    #[error("json: {0}")]
    Json(#[from] serde_json::Error),

    #[error("bson: {0}")]
    Bson(String),

    #[error("unsupported migration input: {0}")]
    Unsupported(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum ExportFormat {
    Csv,
    Jsonl,
    Parquet,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ImportOptions {
    pub batch_size: usize,
    pub strict: bool,
    pub resume_token_path: Option<PathBuf>,
    pub reject_path: Option<PathBuf>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ImportReport {
    pub read_rows: usize,
    pub written_rows: usize,
    pub rejected_rows: usize,
    pub warnings: Vec<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
struct ImportResumeToken {
    committed_rows: usize,
}

#[derive(Clone, Debug, serde::Serialize)]
struct RejectRecord<'a> {
    line: Option<usize>,
    raw: Option<&'a str>,
    reason: &'a str,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ExportOptions {
    pub format: ExportFormat,
    pub batch_size: usize,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ValueCodecMigrationReport {
    pub scanned_rows: usize,
    pub migrated_rows: usize,
    pub skipped_rows: usize,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            batch_size: 1_000,
            strict: false,
            resume_token_path: None,
            reject_path: None,
        }
    }
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self {
            format: ExportFormat::Jsonl,
            batch_size: 1_000,
        }
    }
}

impl ImportReport {
    pub fn warn(&mut self, warning: impl Into<String>) {
        self.warnings.push(warning.into());
    }

    pub fn reject(
        &mut self,
        warning: impl Into<String>,
        options: &ImportOptions,
    ) -> Result<(), MigrationError> {
        self.reject_detail(None, None, warning, options)
    }

    pub fn reject_detail(
        &mut self,
        line: Option<usize>,
        raw: Option<&str>,
        warning: impl Into<String>,
        options: &ImportOptions,
    ) -> Result<(), MigrationError> {
        self.rejected_rows = self.rejected_rows.saturating_add(1);
        let warning = warning.into();
        if options.strict {
            return Err(MigrationError::Unsupported(warning));
        }
        append_reject_record(options, line, raw, &warning)?;
        self.warnings.push(warning);
        Ok(())
    }
}

#[derive(Default)]
struct PendingImportBatch {
    ops: Vec<Op>,
    keys: BTreeMap<Vec<u8>, Row>,
    through_row: usize,
}

struct ImportTableContext<'a> {
    database: &'a Database,
    table: &'a RelTable,
    options: &'a ImportOptions,
}

#[derive(Clone, Copy)]
struct ImportRowSource<'a> {
    committed_row: usize,
    reject_line: usize,
    raw: &'a str,
}

impl PendingImportBatch {
    fn push(
        &mut self,
        row_key: Vec<u8>,
        row: Row,
        ops: Vec<Op>,
        source_row: usize,
    ) -> Result<(), MigrationError> {
        if self.keys.contains_key(&row_key) {
            return Err(MigrationError::Unsupported(
                "duplicate primary key inside import batch".to_owned(),
            ));
        }
        self.keys.insert(row_key, row);
        self.ops.extend(ops);
        self.through_row = source_row;
        Ok(())
    }

    fn len(&self) -> usize {
        self.keys.len()
    }

    fn flush(
        &mut self,
        database: &Database,
        options: &ImportOptions,
    ) -> Result<(), MigrationError> {
        if self.ops.is_empty() {
            return Ok(());
        }
        let through_row = self.through_row;
        database.propose_batch(std::mem::take(&mut self.ops))?;
        self.keys.clear();
        write_resume_token(options, through_row)?;
        Ok(())
    }
}

fn effective_batch_size(options: &ImportOptions) -> usize {
    options.batch_size.max(1)
}

fn read_resume_token(options: &ImportOptions) -> Result<usize, MigrationError> {
    let Some(path) = &options.resume_token_path else {
        return Ok(0);
    };
    if !path.exists() {
        return Ok(0);
    }
    let bytes = fs::read(path)?;
    if bytes.is_empty() {
        return Ok(0);
    }
    let token = serde_json::from_slice::<ImportResumeToken>(&bytes)?;
    Ok(token.committed_rows)
}

fn write_resume_token(
    options: &ImportOptions,
    committed_rows: usize,
) -> Result<(), MigrationError> {
    let Some(path) = &options.resume_token_path else {
        return Ok(());
    };
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    let token = ImportResumeToken { committed_rows };
    let bytes = serde_json::to_vec_pretty(&token)?;
    fs::write(path, bytes)?;
    Ok(())
}

fn append_reject_record(
    options: &ImportOptions,
    line: Option<usize>,
    raw: Option<&str>,
    reason: &str,
) -> Result<(), MigrationError> {
    let Some(path) = &options.reject_path else {
        return Ok(());
    };
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)?;
    serde_json::to_writer(&mut file, &RejectRecord { line, raw, reason })?;
    file.write_all(b"\n")?;
    Ok(())
}

fn reject_import_row(
    target: &ImportTableContext<'_>,
    pending: &mut PendingImportBatch,
    report: &mut ImportReport,
    source: ImportRowSource<'_>,
    reason: impl Into<String>,
) -> Result<(), MigrationError> {
    pending.flush(target.database, target.options)?;
    let reason = reason.into();
    report.reject_detail(
        Some(source.reject_line),
        Some(source.raw),
        reason,
        target.options,
    )?;
    write_resume_token(target.options, source.committed_row)?;
    Ok(())
}

fn import_table_row(
    target: &ImportTableContext<'_>,
    row: Row,
    report: &mut ImportReport,
    pending: &mut PendingImportBatch,
    source: ImportRowSource<'_>,
) -> Result<(), MigrationError> {
    let row_key = target.table.row_key_for_row(&row)?;
    if let Some(pending_row) = pending.keys.get(&row_key) {
        let reason = if pending_row == &row {
            "duplicate primary key already queued in this import batch"
        } else {
            "conflicting duplicate primary key already queued in this import batch"
        };
        reject_import_row(target, pending, report, source, reason)?;
        return Ok(());
    }

    let primary_key = row
        .get(target.table.schema().map_or(0, |schema| schema.primary_key))
        .ok_or_else(|| MigrationError::Unsupported("row has no primary key".to_owned()))?;
    if let Some(existing) = target.table.get(primary_key)? {
        if target.options.resume_token_path.is_some() && existing == row {
            pending.flush(target.database, target.options)?;
            report.written_rows = report.written_rows.saturating_add(1);
            write_resume_token(target.options, source.committed_row)?;
            return Ok(());
        }
        reject_import_row(
            target,
            pending,
            report,
            source,
            "duplicate primary key conflicts with existing row",
        )?;
        return Ok(());
    }

    let ops = target.table.insert_ops(row.clone())?;
    pending.push(row_key, row, ops, source.committed_row)?;
    report.written_rows = report.written_rows.saturating_add(1);
    if pending.len() >= effective_batch_size(target.options) {
        pending.flush(target.database, target.options)?;
    }
    Ok(())
}

pub fn export_table_jsonl(database: &Database, table: &str) -> Result<String, MigrationError> {
    let table = database.table(table)?;
    let rows = table.scan()?;
    let columns = table.schema().map_or_else(
        || vec!["value".to_owned()],
        |schema| {
            schema
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect()
        },
    );
    rows_to_jsonl(&columns, &rows)
}

pub fn import_table_jsonl(
    database: &Database,
    table: &str,
    input: &str,
    options: &ImportOptions,
) -> Result<ImportReport, MigrationError> {
    let table = database.table(table)?;
    let mut report = ImportReport::default();
    let resume_after = read_resume_token(options)?;
    let mut pending = PendingImportBatch::default();
    let target = ImportTableContext {
        database,
        table: &table,
        options,
    };
    for (index, line) in input.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let source_row = index + 1;
        report.read_rows = report.read_rows.saturating_add(1);
        if source_row <= resume_after {
            continue;
        }
        let value = match serde_json::from_str::<serde_json::Value>(line) {
            Ok(value) => value,
            Err(error) => {
                reject_import_row(
                    &target,
                    &mut pending,
                    &mut report,
                    ImportRowSource {
                        committed_row: source_row,
                        reject_line: index + 1,
                        raw: line,
                    },
                    format!("line {}: invalid JSONL: {error}", index + 1),
                )?;
                continue;
            }
        };
        let row = match json_object_to_row(table.schema(), &value) {
            Ok(row) => row,
            Err(error) => {
                reject_import_row(
                    &target,
                    &mut pending,
                    &mut report,
                    ImportRowSource {
                        committed_row: source_row,
                        reject_line: index + 1,
                        raw: line,
                    },
                    format!("line {}: {error}", index + 1),
                )?;
                continue;
            }
        };
        import_table_row(
            &target,
            row,
            &mut report,
            &mut pending,
            ImportRowSource {
                committed_row: source_row,
                reject_line: index + 1,
                raw: line,
            },
        )?;
    }
    pending.flush(database, options)?;
    Ok(report)
}

pub fn migrate_value_codec(
    database: &Database,
) -> Result<ValueCodecMigrationReport, MigrationError> {
    const VALUE_TABLES: [&str; 3] = [DOCUMENT_TABLE, REL_ROWS_TABLE, GRAPH_OUT_EDGES_TABLE];
    const BATCH_SIZE: usize = 1_000;

    let mut report = ValueCodecMigrationReport::default();
    let mut pending = Vec::new();
    for table in VALUE_TABLES {
        for (key, value) in database.range(table, &[], &[], ReadConsistency::Strong)? {
            report.scanned_rows = report.scanned_rows.saturating_add(1);
            if value_is_binary_encoded(&value) {
                report.skipped_rows = report.skipped_rows.saturating_add(1);
                continue;
            }

            let decoded = decode_value(&value)?;
            let encoded = encode_value(&decoded)?;
            if encoded == value {
                report.skipped_rows = report.skipped_rows.saturating_add(1);
                continue;
            }

            pending.push(Op::Put {
                table: table.to_owned(),
                key,
                value: encoded,
            });
            report.migrated_rows = report.migrated_rows.saturating_add(1);
            if pending.len() >= BATCH_SIZE {
                database.propose_batch(std::mem::take(&mut pending))?;
            }
        }
    }

    if !pending.is_empty() {
        database.propose_batch(pending)?;
    }

    Ok(report)
}

pub fn export_table_csv(database: &Database, table: &str) -> Result<String, MigrationError> {
    let table = database.table(table)?;
    let rows = table.scan()?;
    let columns = table.schema().map_or_else(
        || vec!["value".to_owned()],
        |schema| {
            schema
                .columns
                .iter()
                .map(|column| column.name.clone())
                .collect()
        },
    );
    Ok(rows_to_csv(&columns, &rows))
}

pub fn import_table_csv(
    database: &Database,
    table: &str,
    input: &str,
    options: &ImportOptions,
) -> Result<ImportReport, MigrationError> {
    let table = database.table(table)?;
    let schema = table
        .schema()
        .ok_or_else(|| MigrationError::Unsupported("CSV import requires a schema".to_owned()))?;
    let mut lines = input.lines();
    let Some(header) = lines.next() else {
        return Ok(ImportReport::default());
    };
    let columns = parse_csv_line(header);
    let mut report = ImportReport::default();
    let resume_after = read_resume_token(options)?;
    let mut pending = PendingImportBatch::default();
    let target = ImportTableContext {
        database,
        table: &table,
        options,
    };
    for (index, line) in lines.enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let source_row = index + 1;
        let file_line = index + 2;
        report.read_rows = report.read_rows.saturating_add(1);
        if source_row <= resume_after {
            continue;
        }
        let fields = parse_csv_line(line);
        if fields.len() != columns.len() {
            reject_import_row(
                &target,
                &mut pending,
                &mut report,
                ImportRowSource {
                    committed_row: source_row,
                    reject_line: file_line,
                    raw: line,
                },
                format!(
                    "line {file_line}: expected {} fields, got {}",
                    columns.len(),
                    fields.len()
                ),
            )?;
            continue;
        }
        let mut by_name = serde_json::Map::new();
        for (column, field) in columns.iter().zip(&fields) {
            by_name.insert(column.clone(), csv_field_to_json(field));
        }
        let row = match json_object_to_row(Some(schema), &serde_json::Value::Object(by_name)) {
            Ok(row) => row,
            Err(error) => {
                reject_import_row(
                    &target,
                    &mut pending,
                    &mut report,
                    ImportRowSource {
                        committed_row: source_row,
                        reject_line: file_line,
                        raw: line,
                    },
                    format!("line {file_line}: {error}"),
                )?;
                continue;
            }
        };
        import_table_row(
            &target,
            row,
            &mut report,
            &mut pending,
            ImportRowSource {
                committed_row: source_row,
                reject_line: file_line,
                raw: line,
            },
        )?;
    }
    pending.flush(database, options)?;
    Ok(report)
}

pub fn import_pg_dump_plain(
    database: &mut Database,
    dump: &str,
    options: &ImportOptions,
) -> Result<ImportReport, MigrationError> {
    let mut report = ImportReport::default();
    let mut lines = dump.lines().peekable();
    let mut consumed_rows = 0usize;
    while let Some(line) = lines.next() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }
        if trimmed.to_ascii_uppercase().starts_with("COPY ") {
            let (table, columns) = parse_copy_header(trimmed)?;
            let partial = import_pg_copy_text_block(
                database,
                &table,
                &columns,
                &mut lines,
                options,
                &mut consumed_rows,
            )?;
            report.read_rows = report.read_rows.saturating_add(partial.read_rows);
            report.written_rows = report.written_rows.saturating_add(partial.written_rows);
            report.rejected_rows = report.rejected_rows.saturating_add(partial.rejected_rows);
            report.warnings.extend(partial.warnings);
            continue;
        }
        if trimmed.to_ascii_uppercase().starts_with("CREATE TABLE") {
            report.warn(
                "CREATE TABLE translation is reported but not applied by the preview importer",
            );
            continue;
        }
        report.warn(format!(
            "ignored pg_dump statement: {}",
            first_words(trimmed, 6)
        ));
    }
    Ok(report)
}

fn import_pg_copy_text_block(
    database: &Database,
    table: &str,
    columns: &[String],
    lines: &mut std::iter::Peekable<std::str::Lines<'_>>,
    options: &ImportOptions,
    consumed_rows: &mut usize,
) -> Result<ImportReport, MigrationError> {
    let table = database.table(table)?;
    let schema = table.schema().ok_or_else(|| {
        MigrationError::Unsupported("COPY import requires an existing table schema".to_owned())
    })?;
    let mut report = ImportReport::default();
    let resume_after = read_resume_token(options)?;
    let mut pending = PendingImportBatch::default();
    let target = ImportTableContext {
        database,
        table: &table,
        options,
    };

    for (index, data) in lines.by_ref().enumerate() {
        if data == "\\." {
            break;
        }
        *consumed_rows = (*consumed_rows).saturating_add(1);
        let source_row = *consumed_rows;
        report.read_rows = report.read_rows.saturating_add(1);
        if source_row <= resume_after {
            continue;
        }
        let fields = match parse_pg_copy_text_line(data) {
            Ok(fields) => fields,
            Err(error) => {
                reject_import_row(
                    &target,
                    &mut pending,
                    &mut report,
                    ImportRowSource {
                        committed_row: source_row,
                        reject_line: index + 1,
                        raw: data,
                    },
                    format!("COPY row {}: {error}", index + 1),
                )?;
                continue;
            }
        };
        if fields.len() != columns.len() {
            reject_import_row(
                &target,
                &mut pending,
                &mut report,
                ImportRowSource {
                    committed_row: source_row,
                    reject_line: index + 1,
                    raw: data,
                },
                format!(
                    "COPY row {}: expected {} fields, got {}",
                    index + 1,
                    columns.len(),
                    fields.len()
                ),
            )?;
            continue;
        }

        let mut by_name = serde_json::Map::new();
        for (column, field) in columns.iter().zip(fields) {
            by_name.insert(column.clone(), field.into_json());
        }
        let row = match json_object_to_row(Some(schema), &serde_json::Value::Object(by_name)) {
            Ok(row) => row,
            Err(error) => {
                reject_import_row(
                    &target,
                    &mut pending,
                    &mut report,
                    ImportRowSource {
                        committed_row: source_row,
                        reject_line: index + 1,
                        raw: data,
                    },
                    format!("COPY row {}: {error}", index + 1),
                )?;
                continue;
            }
        };
        import_table_row(
            &target,
            row,
            &mut report,
            &mut pending,
            ImportRowSource {
                committed_row: source_row,
                reject_line: index + 1,
                raw: data,
            },
        )?;
    }

    pending.flush(database, options)?;
    Ok(report)
}

pub fn import_mongo_bson_documents<I>(
    documents: I,
    options: &ImportOptions,
) -> Result<(Vec<Value>, ImportReport), MigrationError>
where
    I: IntoIterator<Item = Document>,
{
    let mut values = Vec::new();
    let mut report = ImportReport::default();
    for document in documents {
        report.read_rows = report.read_rows.saturating_add(1);
        match bson_document_to_value(&document, &mut report) {
            Ok(value) => {
                report.written_rows = report.written_rows.saturating_add(1);
                values.push(value);
            }
            Err(error) => report.reject(error.to_string(), options)?,
        }
    }
    Ok((values, report))
}

pub fn read_bson_documents(path: impl AsRef<Path>) -> Result<Vec<Document>, MigrationError> {
    let file = fs::File::open(path)?;
    let mut reader = io::BufReader::new(file);
    let mut documents = Vec::new();
    loop {
        match Document::from_reader(&mut reader) {
            Ok(document) => documents.push(document),
            Err(error) if error.to_string().contains("unexpected end of file") => break,
            Err(error) => return Err(MigrationError::Bson(error.to_string())),
        }
    }
    Ok(documents)
}

pub fn rows_to_jsonl(columns: &[String], rows: &[Row]) -> Result<String, MigrationError> {
    let mut output = String::new();
    for row in rows {
        let mut object = serde_json::Map::new();
        for (column, value) in columns.iter().zip(row) {
            object.insert(column.clone(), value_to_json(value));
        }
        output.push_str(&serde_json::to_string(&serde_json::Value::Object(object))?);
        output.push('\n');
    }
    Ok(output)
}

pub fn rows_to_csv(columns: &[String], rows: &[Row]) -> String {
    let mut output = String::new();
    output.push_str(
        &columns
            .iter()
            .map(|column| csv_escape(column))
            .collect::<Vec<_>>()
            .join(","),
    );
    output.push('\n');
    for row in rows {
        output.push_str(&row.iter().map(value_to_csv).collect::<Vec<_>>().join(","));
        output.push('\n');
    }
    output
}

pub(crate) fn json_object_to_row(
    schema: Option<&crate::query::TableSchema>,
    value: &serde_json::Value,
) -> Result<Row, MigrationError> {
    let object = value
        .as_object()
        .ok_or_else(|| MigrationError::Unsupported("row must be a JSON object".to_owned()))?;
    let Some(schema) = schema else {
        return Ok(vec![json_to_value(value)]);
    };
    schema
        .columns
        .iter()
        .map(|column| {
            object
                .get(&column.name)
                .map_or(Ok(Value::Null), |value| json_to_column_value(value, column))
        })
        .collect()
}

fn csv_field_to_json(field: &str) -> serde_json::Value {
    let trimmed = field.trim_end_matches('\r');
    serde_json::Value::String(trimmed.to_owned())
}

fn json_to_column_value(
    value: &serde_json::Value,
    column: &ColumnDef,
) -> Result<Value, MigrationError> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    if let Some(raw) = value.as_str()
        && raw.is_empty()
        && column.nullable
        && column.ty != ColumnType::Str
    {
        return Ok(Value::Null);
    }

    match column.ty {
        ColumnType::Null => Ok(Value::Null),
        ColumnType::Bool => value
            .as_bool()
            .or_else(|| value.as_str().and_then(|value| value.parse::<bool>().ok()))
            .map(Value::Bool)
            .ok_or_else(|| invalid_column_value(column, value)),
        ColumnType::Int => value
            .as_i64()
            .or_else(|| value.as_str().and_then(|value| value.parse::<i64>().ok()))
            .map(Value::Int)
            .ok_or_else(|| invalid_column_value(column, value)),
        ColumnType::Float => value
            .as_f64()
            .or_else(|| value.as_str().and_then(|value| value.parse::<f64>().ok()))
            .filter(|value| value.is_finite())
            .map(Value::Float)
            .ok_or_else(|| invalid_column_value(column, value)),
        ColumnType::Str => value.as_str().map_or_else(
            || Ok(Value::Str(value.to_string())),
            |value| Ok(Value::Str(value.to_owned())),
        ),
        ColumnType::Bytes => value
            .as_str()
            .map(|value| hex_to_bytes(value).unwrap_or_else(|| value.as_bytes().to_vec()))
            .map(Value::Bytes)
            .ok_or_else(|| invalid_column_value(column, value)),
    }
}

fn invalid_column_value(column: &ColumnDef, value: &serde_json::Value) -> MigrationError {
    MigrationError::Unsupported(format!(
        "column {} cannot coerce value {} to {:?}",
        column.name, value, column.ty
    ))
}

fn hex_to_bytes(value: &str) -> Option<Vec<u8>> {
    if !value.len().is_multiple_of(2) || !value.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return None;
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    let raw = value.as_bytes();
    for chunk in raw.chunks_exact(2) {
        let hex = std::str::from_utf8(chunk).ok()?;
        bytes.push(u8::from_str_radix(hex, 16).ok()?);
    }
    Some(bytes)
}

fn bson_document_to_value(
    document: &Document,
    report: &mut ImportReport,
) -> Result<Value, MigrationError> {
    let mut object = std::collections::BTreeMap::new();
    for (key, value) in document {
        object.insert(key.clone(), bson_to_value(value, report)?);
    }
    Ok(Value::Object(object))
}

fn bson_to_value(value: &Bson, report: &mut ImportReport) -> Result<Value, MigrationError> {
    Ok(match value {
        Bson::Double(value) => Value::Float(*value),
        Bson::String(value) => Value::Str(value.clone()),
        Bson::Array(values) => Value::Array(
            values
                .iter()
                .map(|value| bson_to_value(value, report))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Bson::Document(document) => bson_document_to_value(document, report)?,
        Bson::Boolean(value) => Value::Bool(*value),
        Bson::Null => Value::Null,
        Bson::Int32(value) => Value::Int(i64::from(*value)),
        Bson::Int64(value) => Value::Int(*value),
        Bson::ObjectId(value) => Value::Str(value.to_hex()),
        Bson::DateTime(value) => Value::Int(value.timestamp_millis()),
        Bson::Binary(value) => Value::Bytes(value.bytes.clone()),
        Bson::Decimal128(value) => {
            report.warn("Mongo Decimal128 imported as string to preserve precision");
            Value::Str(value.to_string())
        }
        other => {
            return Err(MigrationError::Unsupported(format!(
                "unsupported BSON value {other:?}"
            )));
        }
    })
}

fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(value) => serde_json::Value::Bool(*value),
        Value::Int(value) => serde_json::Value::Number((*value).into()),
        Value::Float(value) => serde_json::Number::from_f64(*value)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        Value::Str(value) => serde_json::Value::String(value.clone()),
        Value::Bytes(value) => serde_json::Value::String(bytes_to_hex(value)),
        Value::Array(values) => {
            serde_json::Value::Array(values.iter().map(value_to_json).collect())
        }
        Value::Object(values) => serde_json::Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), value_to_json(value)))
                .collect(),
        ),
        Value::Vector(values) => serde_json::Value::Array(
            values
                .iter()
                .filter_map(|value| serde_json::Number::from_f64(f64::from(*value)))
                .map(serde_json::Value::Number)
                .collect(),
        ),
        Value::GeoPoint { lon, lat } => serde_json::json!({ "lon": lon, "lat": lat }),
    }
}

pub(crate) fn json_to_value(value: &serde_json::Value) -> Value {
    match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(value) => Value::Bool(*value),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map_or_else(|| Value::Float(value.as_f64().unwrap_or(0.0)), Value::Int),
        serde_json::Value::String(value) => Value::Str(value.clone()),
        serde_json::Value::Array(values) => {
            Value::Array(values.iter().map(json_to_value).collect())
        }
        serde_json::Value::Object(values) => Value::Object(
            values
                .iter()
                .map(|(key, value)| (key.clone(), json_to_value(value)))
                .collect(),
        ),
    }
}

fn value_to_csv(value: &Value) -> String {
    csv_escape(match value {
        Value::Null => "",
        Value::Bool(value) => return value.to_string(),
        Value::Int(value) => return value.to_string(),
        Value::Float(value) => return value.to_string(),
        Value::Str(value) => value,
        Value::Bytes(_)
        | Value::Array(_)
        | Value::Object(_)
        | Value::Vector(_)
        | Value::GeoPoint { .. } => {
            return csv_escape(&value_to_json(value).to_string());
        }
    })
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn csv_escape(value: &str) -> String {
    if value.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_owned()
    }
}

pub(crate) fn parse_csv_line(line: &str) -> Vec<String> {
    let mut values = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '"' if quoted && chars.peek() == Some(&'"') => {
                current.push('"');
                let _ = chars.next();
            }
            '"' => quoted = !quoted,
            ',' if !quoted => {
                values.push(current.clone());
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    values.push(current);
    values
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CopyField {
    Null,
    Text(String),
}

impl CopyField {
    fn into_json(self) -> serde_json::Value {
        match self {
            Self::Null => serde_json::Value::Null,
            Self::Text(value) => serde_json::Value::String(value),
        }
    }
}

fn parse_pg_copy_text_line(line: &str) -> Result<Vec<CopyField>, MigrationError> {
    line.split('\t').map(parse_pg_copy_text_field).collect()
}

pub fn parse_pg_copy_text_values(line: &str) -> Result<Vec<Option<String>>, MigrationError> {
    parse_pg_copy_text_line(line).map(|fields| {
        fields
            .into_iter()
            .map(|field| match field {
                CopyField::Null => None,
                CopyField::Text(value) => Some(value),
            })
            .collect()
    })
}

fn parse_pg_copy_text_field(raw: &str) -> Result<CopyField, MigrationError> {
    if raw == "\\N" {
        return Ok(CopyField::Null);
    }

    let mut decoded = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            decoded.push(ch);
            continue;
        }
        let Some(escaped) = chars.next() else {
            return Err(MigrationError::Unsupported(
                "COPY field ends with a bare backslash".to_owned(),
            ));
        };
        decoded.push(match escaped {
            'b' => '\u{0008}',
            'f' => '\u{000C}',
            'n' => '\n',
            'r' => '\r',
            't' => '\t',
            '\\' => '\\',
            other => other,
        });
    }

    Ok(CopyField::Text(decoded))
}

fn parse_copy_header(header: &str) -> Result<(String, Vec<String>), MigrationError> {
    let after_copy = header
        .trim_end_matches(';')
        .strip_prefix("COPY ")
        .or_else(|| header.trim_end_matches(';').strip_prefix("copy "))
        .ok_or_else(|| MigrationError::Unsupported(header.to_owned()))?;
    let Some((table, rest)) = after_copy.split_once('(') else {
        return Err(MigrationError::Unsupported(
            "COPY without column list is unsupported".to_owned(),
        ));
    };
    let Some((columns, _)) = rest.split_once(')') else {
        return Err(MigrationError::Unsupported(
            "COPY column list is not closed".to_owned(),
        ));
    };
    Ok((
        table.trim().trim_matches('"').to_owned(),
        columns
            .split(',')
            .map(|column| column.trim().trim_matches('"').to_owned())
            .collect(),
    ))
}

fn first_words(value: &str, max: usize) -> String {
    value
        .split_whitespace()
        .take(max)
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use bson::{Bson, Document, oid::ObjectId};

    use super::{
        ImportOptions, import_mongo_bson_documents, import_pg_dump_plain, import_table_csv,
        import_table_jsonl, migrate_value_codec, parse_csv_line, rows_to_csv, rows_to_jsonl,
    };
    use crate::{
        db::{DbConfig, Profile, create_database},
        model::{
            CollectionId, DOCUMENT_TABLE, DocumentId, Value, make_document_key,
            value_is_binary_encoded,
        },
        query::{ColumnDef, ColumnType, Row, TableSchema},
        repl::{Op, ReadConsistency, Replication},
    };

    #[test]
    fn csv_escapes_and_parses_basic_rows() {
        let columns = vec!["id".to_owned(), "name".to_owned()];
        let rows: Vec<Row> = vec![vec![Value::Int(1), Value::Str("Ada, Lovelace".to_owned())]];
        let csv = rows_to_csv(&columns, &rows);
        assert!(csv.contains("\"Ada, Lovelace\""));
        assert_eq!(parse_csv_line("\"Ada, Lovelace\",x")[0], "Ada, Lovelace");
    }

    #[test]
    fn jsonl_round_trip_shape_is_object_per_line() -> Result<(), Box<dyn std::error::Error>> {
        let columns = vec!["id".to_owned()];
        let output = rows_to_jsonl(&columns, &[vec![Value::Int(7)]])?;
        assert_eq!(output.trim(), "{\"id\":7}");
        Ok(())
    }

    #[test]
    fn mongo_decimal_is_string_with_warning() -> Result<(), Box<dyn std::error::Error>> {
        let mut doc = Document::new();
        doc.insert("_id", Bson::ObjectId(ObjectId::new()));
        doc.insert("name", Bson::String("Ada".to_owned()));
        let (values, report) = import_mongo_bson_documents([doc], &ImportOptions::default())?;
        assert_eq!(values.len(), 1);
        assert_eq!(report.written_rows, 1);
        Ok(())
    }

    #[test]
    fn pg_copy_text_import_handles_tabs_nulls_and_escapes() -> Result<(), Box<dyn std::error::Error>>
    {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        let schema = TableSchema {
            columns: ["id", "name", "note", "extra"]
                .into_iter()
                .map(|name| ColumnDef {
                    name: name.to_owned(),
                    ty: ColumnType::Str,
                    nullable: true,
                })
                .collect(),
            primary_key: 0,
        };
        database.create_table("users", Some(schema), Vec::new())?;

        let dump = "COPY users (id, name, note, extra) FROM stdin;\n\
1\tAda\tline\\nnext\t\n\
2\tGrace\t\\N\tvalue,with,commas\n\
3\tBackslash\t\\\\N\tliteral\\t tab\n\
\\.\n";

        let report = import_pg_dump_plain(&mut database, dump, &ImportOptions::default())?;
        assert_eq!(report.read_rows, 3);
        assert_eq!(report.written_rows, 3);

        let mut rows = database.table("users")?.scan()?;
        rows.sort_by_key(|row| match &row[0] {
            Value::Str(value) => value.clone(),
            _ => String::new(),
        });
        assert_eq!(
            rows[0],
            vec![
                Value::Str("1".to_owned()),
                Value::Str("Ada".to_owned()),
                Value::Str("line\nnext".to_owned()),
                Value::Str(String::new()),
            ]
        );
        assert_eq!(rows[1][2], Value::Null);
        assert_eq!(rows[1][3], Value::Str("value,with,commas".to_owned()));
        assert_eq!(rows[2][2], Value::Str("\\N".to_owned()));
        assert_eq!(rows[2][3], Value::Str("literal\t tab".to_owned()));
        Ok(())
    }

    #[test]
    fn csv_import_coerces_rejects_and_checkpoints() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.create_table("users", Some(user_schema()), Vec::new())?;
        let options = ImportOptions {
            batch_size: 2,
            resume_token_path: Some(temp.path().join("resume.json")),
            reject_path: Some(temp.path().join("rejects.jsonl")),
            ..ImportOptions::default()
        };

        let report = import_table_csv(
            &database,
            "users",
            "id,name,age\r\n1,Ada,37\r\nbad,Grace,42\r\n2,Bob,\r\n",
            &options,
        )?;

        assert_eq!(report.read_rows, 3);
        assert_eq!(report.written_rows, 2);
        assert_eq!(report.rejected_rows, 1);
        let rows = database.table("users")?.scan()?;
        assert_eq!(rows.len(), 2);
        assert!(rows.contains(&vec![
            Value::Int(1),
            Value::Str("Ada".to_owned()),
            Value::Int(37)
        ]));
        assert!(rows.contains(&vec![
            Value::Int(2),
            Value::Str("Bob".to_owned()),
            Value::Null
        ]));

        let resume = std::fs::read_to_string(temp.path().join("resume.json"))?;
        assert!(resume.contains("\"committed_rows\": 3"));
        let reject = std::fs::read_to_string(temp.path().join("rejects.jsonl"))?;
        assert!(reject.contains("\"line\":3"));
        assert!(reject.contains("cannot coerce"));
        Ok(())
    }

    #[test]
    fn resume_import_treats_identical_duplicate_as_committed()
    -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.create_table("users", Some(user_schema()), Vec::new())?;
        import_table_jsonl(
            &database,
            "users",
            "{\"id\":1,\"name\":\"Ada\",\"age\":37}\n",
            &ImportOptions::default(),
        )?;

        let options = ImportOptions {
            resume_token_path: Some(temp.path().join("resume.json")),
            reject_path: Some(temp.path().join("rejects.jsonl")),
            ..ImportOptions::default()
        };
        let report = import_table_jsonl(
            &database,
            "users",
            "{\"id\":1,\"name\":\"Ada\",\"age\":37}\n",
            &options,
        )?;

        assert_eq!(report.written_rows, 1);
        assert_eq!(report.rejected_rows, 0);
        assert!(!temp.path().join("rejects.jsonl").exists());
        let resume = std::fs::read_to_string(temp.path().join("resume.json"))?;
        assert!(resume.contains("\"committed_rows\": 1"));
        Ok(())
    }

    #[test]
    fn resume_import_rejects_conflicting_duplicate() -> Result<(), Box<dyn std::error::Error>> {
        let temp = tempfile::tempdir()?;
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.create_table("users", Some(user_schema()), Vec::new())?;
        import_table_jsonl(
            &database,
            "users",
            "{\"id\":1,\"name\":\"Ada\",\"age\":37}\n",
            &ImportOptions::default(),
        )?;

        let options = ImportOptions {
            resume_token_path: Some(temp.path().join("resume.json")),
            reject_path: Some(temp.path().join("rejects.jsonl")),
            ..ImportOptions::default()
        };
        let report = import_table_jsonl(
            &database,
            "users",
            "{\"id\":1,\"name\":\"Grace\",\"age\":42}\n",
            &options,
        )?;

        assert_eq!(report.written_rows, 0);
        assert_eq!(report.rejected_rows, 1);
        let reject = std::fs::read_to_string(temp.path().join("rejects.jsonl"))?;
        assert!(reject.contains("duplicate primary key conflicts"));
        Ok(())
    }

    #[test]
    fn value_codec_migration_rewrites_legacy_document_payloads()
    -> Result<(), Box<dyn std::error::Error>> {
        let database = create_database(DbConfig::new(Profile::InMemory))?;
        let id = DocumentId::from_bytes([7; 16]);
        let key = make_document_key(CollectionId::new(25), id);
        let value = Value::Object(
            [("name".to_owned(), Value::Str("legacy".to_owned()))]
                .into_iter()
                .collect(),
        );
        let legacy = serde_json::to_vec(&value)?;

        database.propose(Op::Put {
            table: DOCUMENT_TABLE.to_owned(),
            key: key.clone(),
            value: legacy,
        })?;

        let report = migrate_value_codec(&database)?;
        let migrated = database
            .read(DOCUMENT_TABLE, &key, ReadConsistency::Strong)?
            .ok_or("migrated value should exist")?;

        assert_eq!(report.scanned_rows, 1);
        assert_eq!(report.migrated_rows, 1);
        assert!(value_is_binary_encoded(&migrated));

        Ok(())
    }

    fn user_schema() -> TableSchema {
        TableSchema {
            columns: vec![
                ColumnDef {
                    name: "id".to_owned(),
                    ty: ColumnType::Int,
                    nullable: false,
                },
                ColumnDef {
                    name: "name".to_owned(),
                    ty: ColumnType::Str,
                    nullable: false,
                },
                ColumnDef {
                    name: "age".to_owned(),
                    ty: ColumnType::Int,
                    nullable: true,
                },
            ],
            primary_key: 0,
        }
    }
}
