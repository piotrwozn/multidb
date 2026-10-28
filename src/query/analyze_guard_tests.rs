use super::*;
use crate::{
    db::{DbConfig, Profile, create_database},
    security::{Permission, Resource},
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

#[derive(Default)]
struct AnalyzeGuard {
    allowed: BTreeSet<String>,
    seen: BTreeSet<SqlReadTarget>,
    policies: BTreeMap<String, SqlRelationPolicy>,
}

impl SqlReadGuard for AnalyzeGuard {
    fn authorize_reads(&mut self, reads: &BTreeSet<SqlReadTarget>) -> Result<(), AuthzError> {
        self.seen.extend(reads.iter().cloned());
        for read in reads {
            if let SqlReadTarget::Catalog(name) = read
                && !self.allowed.contains(name)
            {
                return Err(AuthzError::Denied {
                    principal: "analyzer".to_owned(),
                    resource: Resource::Table(name.clone()),
                    permission: Permission::Read,
                });
            }
        }
        Ok(())
    }

    fn relation_policies(&self) -> BTreeMap<String, SqlRelationPolicy> {
        self.policies.clone()
    }
}

fn fixture() -> Result<SqlEngine, Box<dyn std::error::Error>> {
    let repl: Arc<dyn Replication> = Arc::new(create_database(DbConfig::new(Profile::InMemory))?);
    let mut engine = SqlEngine::new(repl);
    for name in ["public_t", "protected"] {
        engine.create_table(
            name,
            Some(TableSchema::new(
                vec![ColumnDef::new("id", ColumnType::Int, false)],
                0,
            )),
            Vec::new(),
        )?;
        engine
            .tables
            .get(name)
            .ok_or("missing table")?
            .insert(vec![Value::Int(1)])?;
    }
    Ok(engine)
}

fn targets(names: &[&str]) -> BTreeSet<SqlReadTarget> {
    names
        .iter()
        .map(|name| SqlReadTarget::Catalog((*name).to_owned()))
        .collect()
}

fn no_statistics(engine: &SqlEngine) -> TestResult {
    for name in ["public_t", "protected"] {
        assert!(
            StatsCatalog::read_table(engine.repl.as_ref(), name)?.is_none(),
            "denied ANALYZE must not persist statistics for {name}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn every_analyze_form_authorizes_reads_before_any_statement_runs() -> TestResult {
    for sql in [
        "ANALYZE FULL protected",
        "ANALYZE TABLE protected",
        "ANALYZE",
        "SELECT 1; ANALYZE TABLE protected",
        "INSERT INTO public_t VALUES (2); ANALYZE TABLE protected",
    ] {
        let engine = fixture()?;
        let mut guard = AnalyzeGuard {
            allowed: BTreeSet::from(["public_t".to_owned()]),
            ..AnalyzeGuard::default()
        };
        let result = engine.execute_with_read_guard(sql, Some(&mut guard)).await;
        assert!(
            matches!(result, Err(QueryError::AuthzDenied(_))),
            "{sql}: {result:?}"
        );
        assert!(
            guard
                .seen
                .contains(&SqlReadTarget::Catalog("protected".to_owned())),
            "{sql}: {:?}",
            guard.seen
        );
        no_statistics(&engine)?;
        assert_eq!(
            engine
                .tables
                .get("public_t")
                .ok_or("missing table")?
                .scan()?,
            vec![vec![Value::Int(1)]],
            "a later denied ANALYZE must prevent earlier writes: {sql}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn analyze_all_authorizes_every_relation_and_allowed_analysis_still_works() -> TestResult {
    for (sql, expected) in [
        ("ANALYZE FULL protected", targets(&["protected"])),
        ("ANALYZE", targets(&["protected", "public_t"])),
        ("SELECT 1; ANALYZE TABLE protected", targets(&["protected"])),
    ] {
        let engine = fixture()?;
        let mut guard = AnalyzeGuard {
            allowed: BTreeSet::from(["public_t".to_owned(), "protected".to_owned()]),
            ..AnalyzeGuard::default()
        };
        engine
            .execute_with_read_guard(sql, Some(&mut guard))
            .await?;
        assert_eq!(guard.seen, expected, "{sql}");
        assert!(
            StatsCatalog::read_table(engine.repl.as_ref(), "protected")?.is_some(),
            "allowed ANALYZE must still collect statistics: {sql}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn analyze_refuses_raw_statistics_for_row_or_masking_policy_relations() -> TestResult {
    for policy in [
        SqlRelationPolicy {
            visible_when: vec![Vec::new()],
            masks: Vec::new(),
        },
        SqlRelationPolicy {
            visible_when: Vec::new(),
            masks: vec![(FieldPath::new(["id"]), Value::Int(0))],
        },
    ] {
        for sql in [
            "ANALYZE FULL protected",
            "ANALYZE TABLE protected",
            "ANALYZE",
            "INSERT INTO public_t VALUES (2); ANALYZE TABLE protected",
        ] {
            let engine = fixture()?;
            let mut guard = AnalyzeGuard {
                allowed: BTreeSet::from(["public_t".to_owned(), "protected".to_owned()]),
                policies: BTreeMap::from([("protected".to_owned(), policy.clone())]),
                ..AnalyzeGuard::default()
            };
            let result = engine.execute_with_read_guard(sql, Some(&mut guard)).await;
            assert!(
                matches!(result, Err(QueryError::Unsupported(_))),
                "{sql}: {result:?}"
            );
            no_statistics(&engine)?;
            assert_eq!(
                engine
                    .tables
                    .get("public_t")
                    .ok_or("missing table")?
                    .scan()?,
                vec![vec![Value::Int(1)]],
                "a policy-bearing ANALYZE must prevent earlier writes: {sql}"
            );
        }
    }
    Ok(())
}
