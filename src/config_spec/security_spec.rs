//! Spec version 2 `security` (roles and principals managed by provisioning) and `provisioning`
//! blocks, and their validation. Grants name catalog objects or the whole database: no secret
//! and no `System` scope can be expressed.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use super::physical_spec::{PlannedKind, PlannedObject};
use super::{
    CertificationImpact, DatabaseSpec, ValidationIssue, ValidationSeverity, suggest,
    validation_issue,
};

/// Role names a spec cannot manage: the bootstrap administrator role.
pub const RESERVED_ROLE_NAMES: &[&str] = &["admin"];
/// User and principal names a spec cannot manage: the bootstrap administrator principals.
pub const RESERVED_PRINCIPAL_NAMES: &[&str] = &["admin", "root"];
/// The most roles of one spec.
pub const MAX_ROLES: usize = 256;
/// The most grants of one role.
pub const MAX_GRANTS_PER_ROLE: usize = 1_024;
/// The most principals of one spec.
pub const MAX_PRINCIPALS: usize = 4_096;
/// The longest role name, in bytes.
const MAX_ROLE_NAME_BYTES: usize = 64;
/// The longest user or principal name, in bytes.
const MAX_PRINCIPAL_NAME_BYTES: usize = 128;

/// `security`: the roles and principals provisioning manages. Roles and principals the spec
/// does not name are never changed.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SecuritySpec {
    #[serde(default)]
    pub roles: Vec<RoleSpec>,
    #[serde(default)]
    pub principals: Vec<PrincipalSpec>,
}

/// A managed role. The name matches `^[a-z][a-z0-9_]{0,63}$`.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RoleSpec {
    pub name: String,
    pub grants: Vec<GrantSpec>,
}

/// One grant: exactly one of `object` (a collection, or a full-text, geo or history object of
/// the spec) or `resource` (the whole database).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GrantSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<GrantScope>,
    pub permissions: Vec<PermissionSpec>,
}

/// The scopes a grant can name besides objects. There is deliberately no `system` scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GrantScope {
    Database,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PermissionSpec {
    Read,
    Write,
    Admin,
}

/// A managed principal: a login user mapped to a principal name (the user by default) and
/// roles. Passwords never appear in a spec.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PrincipalSpec {
    pub user: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<String>,
    pub roles: Vec<String>,
}

impl PrincipalSpec {
    /// The principal name: `principal`, or the user name when it is unset.
    #[must_use]
    pub fn principal_name(&self) -> &str {
        self.principal.as_deref().unwrap_or(&self.user)
    }
}

/// `provisioning`: how provisioning treats the live database beyond the spec.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProvisioningPolicySpec {
    #[serde(default)]
    pub unmanaged: UnmanagedPolicy,
}

/// What a plan does with catalog objects the spec does not declare: ignore them, report them as
/// Warnings (the default), or refuse to apply.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UnmanagedPolicy {
    Ignore,
    #[default]
    Report,
    Fail,
}

/// Checks `spec.security` against the planned catalog objects. Version 2 only: the version
/// gate refuses `security` in a version 1 document.
pub(crate) fn check_security(
    spec: &DatabaseSpec,
    objects: &[PlannedObject],
    issues: &mut Vec<ValidationIssue>,
) {
    let Some(security) = &spec.security else {
        return;
    };
    let objects = objects
        .iter()
        .map(|object| (object.name.as_str(), object))
        .collect::<BTreeMap<_, _>>();
    if security.roles.len() > MAX_ROLES {
        issues.push(error(
            "SPEC_LIMIT_EXCEEDED",
            "security.roles",
            format!(
                "security declares {} roles; at most {MAX_ROLES} are allowed",
                security.roles.len()
            ),
            "Merge roles or manage the rest outside the spec.",
        ));
    }
    if security.principals.len() > MAX_PRINCIPALS {
        issues.push(error(
            "SPEC_LIMIT_EXCEEDED",
            "security.principals",
            format!(
                "security declares {} principals; at most {MAX_PRINCIPALS} are allowed",
                security.principals.len()
            ),
            "Manage the rest of the principals outside the spec.",
        ));
    }

    let mut role_names = BTreeSet::new();
    for (index, role) in security.roles.iter().enumerate() {
        let path = format!("security.roles[{index}]");
        check_role_name(&role.name, &format!("{path}.name"), issues);
        if !role_names.insert(role.name.as_str()) {
            issues.push(error(
                "SECURITY_DUPLICATE",
                format!("{path}.name"),
                format!("role {} is declared twice", role.name),
                "Declare every role once and merge its grants.",
            ));
        }
        if role.grants.len() > MAX_GRANTS_PER_ROLE {
            issues.push(error(
                "SPEC_LIMIT_EXCEEDED",
                format!("{path}.grants"),
                format!(
                    "role {} has {} grants; at most {MAX_GRANTS_PER_ROLE} are allowed",
                    role.name,
                    role.grants.len()
                ),
                "Grant on the database or split the role.",
            ));
        }
        check_grants(role, &path, &objects, issues);
    }

    let declared_roles = security
        .roles
        .iter()
        .map(|role| role.name.as_str())
        .collect::<Vec<_>>();
    let mut users = BTreeSet::new();
    for (index, principal) in security.principals.iter().enumerate() {
        let path = format!("security.principals[{index}]");
        check_principal_name(&principal.user, "user", &format!("{path}.user"), issues);
        if let Some(name) = &principal.principal {
            check_principal_name(name, "principal", &format!("{path}.principal"), issues);
        }
        if !users.insert(principal.user.as_str()) {
            issues.push(error(
                "SECURITY_DUPLICATE",
                format!("{path}.user"),
                format!("user {} is declared twice", principal.user),
                "Declare every user once.",
            ));
        }
        let mut assigned = BTreeSet::new();
        for (role_index, role) in principal.roles.iter().enumerate() {
            let role_path = format!("{path}.roles[{role_index}]");
            if RESERVED_ROLE_NAMES.contains(&role.as_str()) {
                issues.push(reserved(
                    &role_path,
                    format!(
                        "role {role} is the bootstrap administrator role, which a spec cannot \
                         assign"
                    ),
                ));
            } else if !declared_roles.contains(&role.as_str()) {
                issues.push(error(
                    "SECURITY_PRINCIPAL_UNKNOWN_ROLE",
                    role_path.clone(),
                    format!(
                        "user {} is assigned role {role}, which security.roles does not declare",
                        principal.user
                    ),
                    suggest::closest(role, declared_roles.iter().copied(), 2).map_or_else(
                        || "Declare the role in security.roles, or remove it.".to_owned(),
                        |candidate| format!("Did you mean {candidate}?"),
                    ),
                ));
            }
            if !assigned.insert(role.as_str()) {
                issues.push(error(
                    "SECURITY_DUPLICATE",
                    role_path,
                    format!("user {} is assigned role {role} twice", principal.user),
                    "List every role once.",
                ));
            }
        }
    }
}

fn check_grants(
    role: &RoleSpec,
    path: &str,
    objects: &BTreeMap<&str, &PlannedObject>,
    issues: &mut Vec<ValidationIssue>,
) {
    let mut targets = BTreeSet::new();
    for (index, grant) in role.grants.iter().enumerate() {
        let grant_path = format!("{path}.grants[{index}]");
        let target = match (&grant.object, grant.resource) {
            (Some(object), None) => object.clone(),
            (None, Some(GrantScope::Database)) => "resource: database".to_owned(),
            (Some(_), Some(_)) | (None, None) => {
                issues.push(error(
                    "SECURITY_GRANT_TARGET_INVALID",
                    grant_path,
                    format!(
                        "a grant of role {} must name exactly one of object or resource",
                        role.name
                    ),
                    "Set object to a collection or index name, or resource: database.",
                ));
                continue;
            }
        };
        if !targets.insert(target.clone()) {
            issues.push(error(
                "SECURITY_DUPLICATE",
                grant_path.clone(),
                format!("role {} grants on {target} twice", role.name),
                "Merge the permissions into one grant.",
            ));
        }
        let mut permissions = BTreeSet::new();
        for (permission_index, permission) in grant.permissions.iter().enumerate() {
            if !permissions.insert(*permission) {
                issues.push(error(
                    "SECURITY_DUPLICATE",
                    format!("{grant_path}.permissions[{permission_index}]"),
                    format!(
                        "a grant of role {} lists {} twice",
                        role.name,
                        permission_slug(*permission)
                    ),
                    "List every permission once.",
                ));
            }
        }

        match &grant.object {
            Some(object) if object.starts_with("__") => issues.push(error(
                "SECURITY_SYSTEM_GRANT_FORBIDDEN",
                format!("{grant_path}.object"),
                format!(
                    "role {} grants on {object}; system objects (__ prefix) and system \
                     administration cannot be granted from a spec",
                    role.name
                ),
                "Grant on a collection or index of the spec instead.",
            )),
            Some(object) => match objects.get(object.as_str()) {
                None => issues.push(error(
                    "SECURITY_GRANT_UNKNOWN_OBJECT",
                    format!("{grant_path}.object"),
                    format!(
                        "role {} grants on {object}, which is no collection, index or history \
                         table of the spec",
                        role.name
                    ),
                    suggest::closest(object, objects.keys().copied(), 2).map_or_else(
                        || {
                            "Name a collection, or a full-text, geo or history object it derives."
                                .to_owned()
                        },
                        |candidate| format!("Did you mean {candidate}?"),
                    ),
                )),
                Some(planned) => {
                    if permissions.contains(&PermissionSpec::Read) {
                        check_index_source_read(role, &grant_path, planned, issues);
                    }
                }
            },
            None => {
                if !permissions.is_empty() {
                    issues.push(broad_grant(role, grant_path, &permissions));
                }
            }
        }
    }
}

/// A grant on the whole database also covers the system resource in this build
/// (`Role::allows` falls back to the database grant for every resource): read covers SQL system
/// views and workload reports, admin covers provisioning, RBAC and audit administration.
fn broad_grant(
    role: &RoleSpec,
    grant_path: String,
    permissions: &BTreeSet<PermissionSpec>,
) -> ValidationIssue {
    let granted = permissions
        .iter()
        .map(|permission| permission_slug(*permission))
        .collect::<Vec<_>>()
        .join(", ");
    let covers = permissions
        .iter()
        .map(|permission| match permission {
            PermissionSpec::Read => "system reads (SQL system views and workload reports)",
            PermissionSpec::Write => "system writes",
            PermissionSpec::Admin => "system administration (provisioning, RBAC and audit)",
        })
        .collect::<Vec<_>>()
        .join(", ");
    validation_issue(
        "SECURITY_BROAD_GRANT",
        ValidationSeverity::Warning,
        grant_path,
        format!(
            "role {} gets {granted} on the whole database, which in this build also covers the \
             system resource: {covers}",
            role.name
        ),
        format!("Grant {granted} on the objects the role needs instead."),
        CertificationImpact::None,
    )
}

/// `match()` and `within_radius()` return the source documents' text, ids and points, so
/// searching a full-text or geo index needs Read on its source collection too.
fn check_index_source_read(
    role: &RoleSpec,
    grant_path: &str,
    planned: &PlannedObject,
    issues: &mut Vec<ValidationIssue>,
) {
    let (PlannedKind::FullText { source, .. } | PlannedKind::Geo { source, .. }) = &planned.kind
    else {
        return;
    };
    let reads_source = role.grants.iter().any(|grant| {
        grant.permissions.contains(&PermissionSpec::Read)
            && match (&grant.object, grant.resource) {
                (Some(object), None) => object == source,
                (None, Some(GrantScope::Database)) => true,
                _ => false,
            }
    });
    if !reads_source {
        issues.push(validation_issue(
            "SECURITY_INDEX_GRANT_WITHOUT_SOURCE_READ",
            ValidationSeverity::Warning,
            grant_path,
            format!(
                "role {} can read index {} but not its source collection {source}; match() \
                 and within_radius() return the source documents' text, ids and points, so \
                 searching the index also needs read on {source}",
                role.name, planned.name
            ),
            format!("Grant read on {source} to the role as well."),
            CertificationImpact::None,
        ));
    }
}

fn check_role_name(name: &str, path: &str, issues: &mut Vec<ValidationIssue>) {
    if RESERVED_ROLE_NAMES.contains(&name) {
        issues.push(reserved(
            path,
            format!("role {name} is the bootstrap administrator role and cannot be managed"),
        ));
        return;
    }
    let mut chars = name.chars();
    let valid = chars.next().is_some_and(|first| first.is_ascii_lowercase())
        && name.len() <= MAX_ROLE_NAME_BYTES
        && chars.all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == '_'
        });
    if !valid {
        issues.push(error(
            "SECURITY_NAME_INVALID",
            path,
            format!("role name {name:?} must match ^[a-z][a-z0-9_]{{0,63}}$"),
            "Use lowercase letters, digits and _, starting with a letter.",
        ));
    }
}

fn check_principal_name(name: &str, field: &str, path: &str, issues: &mut Vec<ValidationIssue>) {
    if RESERVED_PRINCIPAL_NAMES.contains(&name) {
        issues.push(reserved(
            path,
            format!("{field} {name} is a bootstrap administrator and cannot be managed"),
        ));
        return;
    }
    if name.is_empty()
        || name.len() > MAX_PRINCIPAL_NAME_BYTES
        || !name.chars().all(|character| character.is_ascii_graphic())
    {
        issues.push(error(
            "SECURITY_NAME_INVALID",
            path,
            format!(
                "{field} name {name:?} must be 1 to {MAX_PRINCIPAL_NAME_BYTES} printable ASCII \
                 characters without spaces"
            ),
            "Use a login name without spaces or control characters.",
        ));
    }
}

fn reserved(path: &str, message: String) -> ValidationIssue {
    error(
        "SECURITY_RESERVED_NAME",
        path,
        message,
        "The bootstrap administrator (role admin, principals admin and root) is managed by \
         multidb serve; choose another name.",
    )
}

fn error(
    code: &str,
    path: impl Into<String>,
    message: impl Into<String>,
    suggestion: impl Into<String>,
) -> ValidationIssue {
    validation_issue(
        code,
        ValidationSeverity::Error,
        path,
        message,
        suggestion,
        CertificationImpact::BlocksCertified,
    )
}

const fn permission_slug(permission: PermissionSpec) -> &'static str {
    match permission {
        PermissionSpec::Read => "read",
        PermissionSpec::Write => "write",
        PermissionSpec::Admin => "admin",
    }
}
