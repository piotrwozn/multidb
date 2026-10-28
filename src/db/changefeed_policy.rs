use super::{CatalogEntry, Database, DbError, policy_has_row_or_masking};
use crate::{
    cdc::{ChangeEvent, LogicalTarget},
    extension::PolicyConfig,
    security::{Permission, Principal, Resource},
    text::TextSource,
};

impl Database {
    /// The row key contains the original primary key independently of the masked payload.
    /// Hide these events rather than exposing the key or inventing colliding masked identities.
    pub(super) fn primary_key_changefeed_event_visible(
        &self,
        policy: &PolicyConfig,
        event: &ChangeEvent,
    ) -> Result<bool, DbError> {
        let LogicalTarget::Table(name) = &event.target else {
            return Ok(true);
        };
        let resource = Resource::Table(name.clone());
        let masks = policy
            .masking
            .iter()
            .filter(|mask| mask.resource == resource)
            .collect::<Vec<_>>();
        if masks.is_empty() {
            return Ok(true);
        }
        let table = self.table(name)?;
        let Some(schema) = table.schema() else {
            return Ok(false);
        };
        let Some(primary_key) = schema.columns.get(schema.primary_key) else {
            return Ok(false);
        };
        Ok(!masks.iter().any(|mask| {
            mask.paths.iter().any(|path| {
                let segments = path.segments();
                segments.is_empty() || (segments.len() == 1 && segments[0] == primary_key.name)
            })
        }))
    }

    /// Derived index keys contain unmasked terms and locations. Until those events can be
    /// reconstructed from policy-filtered source documents, hide the whole index feed whenever
    /// its source is protected. A missing source also fails closed.
    pub(super) fn index_changefeed_event_visible_to(
        &self,
        principal: &Principal,
        policy: &PolicyConfig,
        event: &ChangeEvent,
    ) -> bool {
        let collection_id = match &event.target {
            LogicalTarget::FullTextIndex(name) => match self.catalog.get(name) {
                Some(CatalogEntry::FullTextIndex { config }) => match &config.source {
                    TextSource::Collection { collection_id, .. } => *collection_id,
                },
                _ => return false,
            },
            LogicalTarget::GeoIndex(name) => match self.catalog.get(name) {
                Some(CatalogEntry::GeoIndex { config }) => config.collection_id,
                _ => return false,
            },
            _ => return true,
        };
        self.catalog.iter().any(|(name, entry)| {
            matches!(entry, CatalogEntry::Collection { collection_id: id, .. } if *id == collection_id)
                && {
                    let source = Resource::Collection(name.clone());
                    self.authz.authorize(principal, &source, Permission::Read).is_ok()
                        && !policy_has_row_or_masking(policy, &source)
                }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{ChangeEvent, Database, LogicalTarget, PolicyConfig};
    use crate::{
        cdc::{ChangefeedFilter, ChangefeedOptions, ChangefeedTarget, ResumeToken},
        db::{DbConfig, Profile, create_database},
        extension::{MaskingPolicy, RowPolicy},
        geo::GeoIndexConfig,
        model::{CollectionId, FieldPath, Value},
        query::{ColumnDef, ColumnType, DocField, TableSchema},
        security::{AuthzPolicy, Permission, Principal, Resource, Role},
        text::FullTextIndexConfig,
    };

    type TestResult = Result<(), Box<dyn std::error::Error>>;

    fn indexed_database() -> Result<Database, Box<dyn std::error::Error>> {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        let id = CollectionId::new(17);
        database.create_collection(
            "posts",
            id,
            vec![
                DocField::path("body", FieldPath::new(["body"]), ColumnType::Str),
                DocField::path("tenant", FieldPath::new(["tenant"]), ColumnType::Str),
            ],
            Vec::new(),
        )?;
        database.create_full_text_index(FullTextIndexConfig::collection(
            "posts_text",
            id,
            FieldPath::new(["body"]),
        ))?;
        database.create_geo_index(GeoIndexConfig::new(
            "posts_geo",
            id,
            FieldPath::new(["point"]),
        ))?;
        database
            .collection("posts")?
            .insert(&Value::Object(BTreeMap::from([
                ("body".to_owned(), Value::Str("secretterm".to_owned())),
                ("tenant".to_owned(), Value::Str("private".to_owned())),
                (
                    "point".to_owned(),
                    Value::Object(BTreeMap::from([
                        ("lon".to_owned(), Value::Float(21.0)),
                        ("lat".to_owned(), Value::Float(52.0)),
                    ])),
                ),
            ])))?;
        database.set_authz_policy(AuthzPolicy::new([
            Role::new("reader").grant(Resource::Database, Permission::Read),
            Role::new("index_only")
                .grant(
                    Resource::FullTextIndex("posts_text".to_owned()),
                    Permission::Read,
                )
                .grant(Resource::GeoIndex("posts_geo".to_owned()), Permission::Read),
        ]));
        Ok(database)
    }

    fn derived(event: &ChangeEvent) -> bool {
        matches!(
            event.target,
            LogicalTarget::FullTextIndex(_) | LogicalTarget::GeoIndex(_)
        )
    }

    #[test]
    fn protected_source_never_leaks_derived_index_events() -> TestResult {
        let database = indexed_database()?;
        let reader = Principal::new("alice").with_role("reader");
        let options = ChangefeedOptions::default();
        let filters = [
            ChangefeedFilter::default(),
            ChangefeedFilter {
                target: ChangefeedTarget::FullTextIndex("posts_text".to_owned()),
            },
            ChangefeedFilter {
                target: ChangefeedTarget::GeoIndex("posts_geo".to_owned()),
            },
        ];
        for policy in [
            PolicyConfig {
                masking: vec![MaskingPolicy {
                    resource: Resource::Collection("posts".to_owned()),
                    paths: vec![FieldPath::new(["body"])],
                    replacement: Value::Str("***".to_owned()),
                }],
                ..PolicyConfig::default()
            },
            PolicyConfig {
                row_policies: vec![RowPolicy {
                    resource: Resource::Collection("posts".to_owned()),
                    path: FieldPath::new(["tenant"]),
                    equals: Value::Str("public".to_owned()),
                }],
                ..PolicyConfig::default()
            },
        ] {
            database.set_policy_config(&policy)?;
            for filter in &filters {
                let (trusted, _) =
                    database.poll_changefeed(&ResumeToken::default(), filter, &options, 1_000)?;
                assert!(
                    trusted.iter().any(derived),
                    "fixture must contain index events"
                );
                let (events, _) = database.poll_changefeed_as(
                    &reader,
                    &ResumeToken::default(),
                    filter,
                    &options,
                    1_000,
                )?;
                assert!(!events.iter().any(derived), "{events:?}");
                let page = database.poll_changefeed_page_as(
                    &reader,
                    &ResumeToken::default(),
                    filter,
                    &options,
                    1_000,
                )?;
                assert_eq!(page.events, events);
            }
        }
        Ok(())
    }

    #[test]
    fn index_events_require_source_read_even_without_policies() -> TestResult {
        let database = indexed_database()?;
        let index_only = Principal::new("alice").with_role("index_only");
        let reader = Principal::new("alice").with_role("reader");
        for target in [
            ChangefeedTarget::FullTextIndex("posts_text".to_owned()),
            ChangefeedTarget::GeoIndex("posts_geo".to_owned()),
        ] {
            let filter = ChangefeedFilter { target };
            let (events, _) = database.poll_changefeed_as(
                &index_only,
                &ResumeToken::default(),
                &filter,
                &ChangefeedOptions::default(),
                1_000,
            )?;
            assert!(!events.iter().any(derived));
            let (events, _) = database.poll_changefeed_as(
                &reader,
                &ResumeToken::default(),
                &filter,
                &ChangefeedOptions::default(),
                1_000,
            )?;
            assert!(events.iter().any(derived));
        }
        Ok(())
    }

    #[tokio::test]
    async fn principal_subscription_hides_protected_index_events() -> TestResult {
        let database = indexed_database()?;
        let reader = Principal::new("alice").with_role("reader");
        database.set_policy_config(&PolicyConfig {
            masking: vec![MaskingPolicy {
                resource: Resource::Collection("posts".to_owned()),
                paths: vec![FieldPath::new(["body"])],
                replacement: Value::Str("***".to_owned()),
            }],
            ..PolicyConfig::default()
        })?;
        database.create_subscription_as(
            &reader,
            crate::cdc::SubscriptionConfig::new("feed", ResumeToken::default()),
        )?;
        let (sender, mut receiver) = tokio::sync::mpsc::channel(4_096);
        crate::cdc::push_subscription_once_as(&database, &reader, "feed", &sender).await?;
        drop(sender);
        let mut collection_event = false;
        while let Some(event) = receiver.recv().await {
            assert!(!derived(&event));
            if let (
                LogicalTarget::Collection(_),
                crate::cdc::ChangeOp::Upsert { value_after, .. },
            ) = (&event.target, &event.op)
            {
                collection_event = true;
                assert!(
                    !value_after
                        .windows(b"secretterm".len())
                        .any(|bytes| bytes == b"secretterm")
                );
            }
        }
        assert!(collection_event);
        Ok(())
    }

    #[test]
    fn protected_deletes_do_not_reveal_hidden_row_keys() -> TestResult {
        let database = indexed_database()?;
        let id = database.collection("posts")?.scan()?[0].0;
        database.set_policy_config(&PolicyConfig {
            row_policies: vec![RowPolicy {
                resource: Resource::Collection("posts".to_owned()),
                path: FieldPath::new(["tenant"]),
                equals: Value::Str("public".to_owned()),
            }],
            ..PolicyConfig::default()
        })?;
        database.collection("posts")?.delete(id)?;
        let reader = Principal::new("alice").with_role("reader");
        let filter = ChangefeedFilter {
            target: ChangefeedTarget::Collection("posts".to_owned()),
        };
        let (events, _) = database.poll_changefeed_as(
            &reader,
            &ResumeToken::default(),
            &filter,
            &ChangefeedOptions::default(),
            1_000,
        )?;
        assert!(!events.iter().any(|event| matches!(
            event.op,
            crate::cdc::ChangeOp::Upsert { .. } | crate::cdc::ChangeOp::Delete { .. }
        )));
        Ok(())
    }

    #[test]
    fn masked_primary_key_is_not_exposed_in_changefeed_keys() -> TestResult {
        let mut database = create_database(DbConfig::new(Profile::InMemory))?;
        database.create_table(
            "users",
            Some(TableSchema::new(
                vec![
                    ColumnDef::new("email", ColumnType::Str, false),
                    ColumnDef::new("name", ColumnType::Str, false),
                ],
                0,
            )),
            Vec::new(),
        )?;
        database.set_authz_policy(AuthzPolicy::new([
            Role::new("reader").grant(Resource::Database, Permission::Read)
        ]));
        database.set_policy_config(&PolicyConfig {
            masking: vec![MaskingPolicy {
                resource: Resource::Table("users".to_owned()),
                paths: vec![FieldPath::new(["email"])],
                replacement: Value::Str("***".to_owned()),
            }],
            ..PolicyConfig::default()
        })?;
        let email = Value::Str("private-address@example.org".to_owned());
        database
            .table("users")?
            .insert(vec![email.clone(), Value::Str("Alice".to_owned())])?;
        database.table("users")?.delete(&email)?;
        let reader = Principal::new("alice").with_role("reader");
        for filter in [
            ChangefeedFilter::default(),
            ChangefeedFilter {
                target: ChangefeedTarget::Table("users".to_owned()),
            },
        ] {
            let (events, _) = database.poll_changefeed_as(
                &reader,
                &ResumeToken::default(),
                &filter,
                &ChangefeedOptions::default(),
                1_000,
            )?;
            assert!(!events.iter().any(|event| matches!(
                (&event.target, &event.op),
                (
                    LogicalTarget::Table(_),
                    crate::cdc::ChangeOp::Upsert { .. } | crate::cdc::ChangeOp::Delete { .. }
                )
            )));
            assert_eq!(
                database
                    .poll_changefeed_page_as(
                        &reader,
                        &ResumeToken::default(),
                        &filter,
                        &ChangefeedOptions::default(),
                        1_000,
                    )?
                    .events,
                events
            );
        }
        Ok(())
    }
}
