//! Deterministic mutations at the boundary between metadata authorization and commit.

use std::sync::{Arc, Mutex};

use super::*;
use crate::{
    continuous::{self, CONTINUOUS_QUERY_STATE_TABLE, ContinuousQuerySpec},
    db::{DbConfig, Profile, create_database},
    security::Role,
    txn::WriteAuthorization,
};

type TestResult = Result<(), Box<dyn std::error::Error>>;

struct RacingMetadata {
    database: Arc<Database>,
    before_commit: Mutex<Option<Vec<Op>>>,
}

impl Replication for RacingMetadata {
    fn propose(&self, op: Op) -> Result<(), ReplError> {
        self.database.propose(op)
    }
    fn propose_batch(&self, ops: Vec<Op>) -> Result<(), ReplError> {
        self.database.propose_batch(ops)
    }
    fn propose_authorized_batch(
        &self,
        ops: Vec<Op>,
        authorization: WriteAuthorization,
    ) -> Result<(), ReplError> {
        self.database.propose_authorized_batch(ops, authorization)
    }
    fn propose_authorized_conditional_batch(
        &self,
        batch: ConditionalBatch,
        authorization: WriteAuthorization,
    ) -> Result<(), ReplError> {
        let mutation = self
            .before_commit
            .lock()
            .map_err(|_| ReplError::Rejected("test lock poisoned".to_owned()))?
            .take();
        if let Some(ops) = mutation {
            propose_system_batch(self.database.as_ref(), ops)?;
        }
        self.database
            .propose_authorized_conditional_batch(batch, authorization)
    }
    fn read(
        &self,
        table: &str,
        key: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Option<Bytes>, ReplError> {
        self.database.read(table, key, consistency)
    }
    fn range(
        &self,
        table: &str,
        start: &[u8],
        end: &[u8],
        consistency: ReadConsistency,
    ) -> Result<Vec<(Bytes, Bytes)>, ReplError> {
        self.database.range(table, start, end, consistency)
    }
}

fn authz() -> AuthzPolicy {
    AuthzPolicy::new([Role::new("reader").grant(Resource::Database, Permission::Read)])
}

fn principal(name: &str) -> Principal {
    Principal::new(name).with_role("reader")
}

fn put_subscription(state: &SubscriptionState) -> Result<Op, serde_json::Error> {
    Ok(Op::Put {
        table: CDC_SUBSCRIPTIONS_TABLE.to_owned(),
        key: state.config.name.as_bytes().to_vec(),
        value: serde_json::to_vec(state)?,
    })
}

#[test]
fn racing_first_creates_cannot_overwrite_another_subscription_owner() -> TestResult {
    let database = Arc::new(create_database(DbConfig::new(Profile::InMemory))?);
    let config = SubscriptionConfig::new("feed", ResumeToken::default());
    let mut other = create_subscription(
        database.as_ref(),
        &authz(),
        &principal("bob"),
        SubscriptionConfig::new("template", ResumeToken::default()),
    )?;
    other.config = config.clone();
    let repl = RacingMetadata {
        database: Arc::clone(&database),
        before_commit: Mutex::new(Some(vec![put_subscription(&other)?])),
    };
    assert!(matches!(
        create_subscription(&repl, &authz(), &principal("alice"), config),
        Err(SubscriptionError::Repl(ReplError::Conflict))
    ));
    assert_eq!(subscription_state(database.as_ref(), "feed")?, other);
    Ok(())
}

#[test]
fn racing_adoption_cannot_acknowledge_the_new_owners_cursor() -> TestResult {
    let database = Arc::new(create_database(DbConfig::new(Profile::InMemory))?);
    let mut adopted = create_subscription(
        database.as_ref(),
        &authz(),
        &principal("alice"),
        SubscriptionConfig::new("feed", ResumeToken::default()),
    )?;
    adopted.principal = Some(principal("bob"));
    let repl = RacingMetadata {
        database: Arc::clone(&database),
        before_commit: Mutex::new(Some(vec![put_subscription(&adopted)?])),
    };
    assert!(matches!(
        ack_subscription_as(
            &repl,
            &authz(),
            &principal("alice"),
            "feed",
            ResumeToken::new(DEFAULT_TIMELINE_ID, 10)
        ),
        Err(SubscriptionError::Repl(ReplError::Conflict))
    ));
    assert_eq!(subscription_state(database.as_ref(), "feed")?, adopted);
    Ok(())
}

#[test]
fn racing_acknowledgements_cannot_lose_progress_or_move_backwards() -> TestResult {
    let database = Arc::new(create_database(DbConfig::new(Profile::InMemory))?);
    let mut advanced = create_subscription(
        database.as_ref(),
        &authz(),
        &principal("alice"),
        SubscriptionConfig::new("feed", ResumeToken::new(DEFAULT_TIMELINE_ID, 10)),
    )?;
    advanced.last_ack.lsn = 20;
    let repl = RacingMetadata {
        database: Arc::clone(&database),
        before_commit: Mutex::new(Some(vec![put_subscription(&advanced)?])),
    };
    assert!(matches!(
        ack_subscription(&repl, "feed", ResumeToken::new(DEFAULT_TIMELINE_ID, 15)),
        Err(SubscriptionError::Repl(ReplError::Conflict))
    ));
    assert!(matches!(
        ack_subscription(
            database.as_ref(),
            "feed",
            ResumeToken::new(DEFAULT_TIMELINE_ID, 19)
        ),
        Err(SubscriptionError::Policy(_))
    ));
    assert_eq!(
        subscription_state(database.as_ref(), "feed")?.last_ack.lsn,
        20
    );
    Ok(())
}

#[test]
fn continuous_ack_is_atomic_with_subscription_ownership_and_both_offsets() -> TestResult {
    let database = Arc::new(create_database(DbConfig::new(Profile::InMemory))?);
    let spec = ContinuousQuerySpec {
        name: "query".to_owned(),
        sql: "SELECT * FROM users".to_owned(),
        filter: ChangefeedFilter::default(),
        start: ResumeToken::default(),
        buffer_limit: 10,
    };
    let original = continuous::create_continuous_query_as(
        database.as_ref(),
        &authz(),
        &principal("alice"),
        &spec,
    )?;
    let mut adopted = subscription_state(database.as_ref(), "query")?;
    adopted.principal = Some(principal("bob"));
    let repl = RacingMetadata {
        database: Arc::clone(&database),
        before_commit: Mutex::new(Some(vec![put_subscription(&adopted)?])),
    };
    assert!(matches!(
        continuous::ack_continuous_query_and_subscription_as(
            &repl,
            &authz(),
            &principal("alice"),
            "query",
            ResumeToken::new(DEFAULT_TIMELINE_ID, 10)
        ),
        Err(continuous::ContinuousError::Repl(ReplError::Conflict))
    ));
    assert_eq!(
        continuous::read_continuous_query_state(database.as_ref(), "query")?,
        original
    );
    assert_eq!(subscription_state(database.as_ref(), "query")?, adopted);
    Ok(())
}

#[test]
fn reapplying_a_continuous_query_cannot_overwrite_an_inflight_ack() -> TestResult {
    let database = Arc::new(create_database(DbConfig::new(Profile::InMemory))?);
    let spec = ContinuousQuerySpec {
        name: "query".to_owned(),
        sql: "SELECT * FROM users".to_owned(),
        filter: ChangefeedFilter::default(),
        start: ResumeToken::default(),
        buffer_limit: 10,
    };
    let mut advanced = continuous::create_continuous_query_as(
        database.as_ref(),
        &authz(),
        &principal("alice"),
        &spec,
    )?;
    let mut subscription = subscription_state(database.as_ref(), "query")?;
    advanced.last_ack.lsn = 20;
    subscription.last_ack.lsn = 20;
    let repl = RacingMetadata {
        database: Arc::clone(&database),
        before_commit: Mutex::new(Some(vec![
            Op::Put {
                table: CONTINUOUS_QUERY_STATE_TABLE.to_owned(),
                key: spec.name.as_bytes().to_vec(),
                value: serde_json::to_vec(&advanced)?,
            },
            put_subscription(&subscription)?,
        ])),
    };
    assert!(matches!(
        continuous::create_continuous_query_as(&repl, &authz(), &principal("alice"), &spec),
        Err(continuous::ContinuousError::Repl(ReplError::Conflict))
    ));
    assert_eq!(
        continuous::read_continuous_query_state(database.as_ref(), "query")?,
        advanced
    );
    assert_eq!(
        subscription_state(database.as_ref(), "query")?,
        subscription
    );
    Ok(())
}
