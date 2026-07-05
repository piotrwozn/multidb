use serde_json::{Value as JsonValue, json};

use super::DATABASE_SPEC_VERSION;

#[must_use]
pub fn database_spec_v1_schema() -> JsonValue {
    json!({
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "https://multidb.dev/schemas/database-spec-v1.schema.json",
        "title": "MultiDB DatabaseSpec v1",
        "type": "object",
        "additionalProperties": false,
        "required": [
            "version",
            "name",
            "profile",
            "deployment",
            "defaults",
            "guarantees",
            "domains",
            "collections",
            "extensions",
            "overrides",
            "operation_hints"
        ],
        "properties": {
            "version": { "const": DATABASE_SPEC_VERSION },
            "name": { "type": "string", "minLength": 1 },
            "profile": { "type": "string", "minLength": 1 },
            "deployment": { "$ref": "#/$defs/deployment" },
            "topology": { "$ref": "#/$defs/topology" },
            "defaults": { "$ref": "#/$defs/defaults" },
            "guarantees": { "$ref": "#/$defs/guarantees" },
            "domains": {
                "type": "array",
                "items": { "$ref": "#/$defs/consistency_domain" }
            },
            "collections": {
                "type": "array",
                "items": { "$ref": "#/$defs/collection_role" }
            },
            "extensions": {
                "type": "array",
                "items": { "$ref": "#/$defs/extension_ref" }
            },
            "overrides": { "$ref": "#/$defs/string_map" },
            "operation_hints": { "$ref": "#/$defs/string_map" }
        },
        "$defs": {
            "collection_role": {
                "type": "object",
                "additionalProperties": false,
                "required": ["name", "role", "domain", "indexes"],
                "properties": {
                    "name": { "type": "string", "minLength": 1 },
                    "role": {
                        "type": "string",
                        "enum": [
                            "document_entity",
                            "key_value",
                            "event_log",
                            "vector_memory",
                            "cache",
                            "audit",
                            "graph",
                            "analytics",
                            "time_series"
                        ]
                    },
                    "domain": { "type": "string", "minLength": 1 },
                    "indexes": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": [
                                "primary",
                                "document",
                                "vector",
                                "graph",
                                "full_text",
                                "columnar",
                                "time_series"
                            ]
                        }
                    }
                }
            },
            "consistency_domain": {
                "type": "object",
                "additionalProperties": false,
                "required": ["name", "mode"],
                "properties": {
                    "name": { "type": "string", "minLength": 1 },
                    "mode": {
                        "type": "string",
                        "enum": ["local_snapshot", "strong_cp", "eventual_ap"]
                    }
                }
            },
            "defaults": {
                "type": "object",
                "additionalProperties": false,
                "required": ["consistency_domain", "replication"],
                "properties": {
                    "consistency_domain": { "type": "string", "minLength": 1 },
                    "replication": {
                        "type": "string",
                        "enum": ["cp", "ap"]
                    }
                }
            },
            "guarantees": {
                "type": "object",
                "additionalProperties": false,
                "required": [
                    "write_ack",
                    "conflict_resolution",
                    "backup",
                    "encryption",
                    "audit",
                    "sensitive_data",
                    "strict_cross_domain_transactions"
                ],
                "properties": {
                    "write_ack": {
                        "type": "string",
                        "enum": ["local", "quorum", "all"]
                    },
                    "conflict_resolution": {
                        "type": "string",
                        "enum": [
                            "none",
                            "last_write_wins",
                            "vector_clock",
                            "crdt",
                            "custom"
                        ]
                    },
                    "backup": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["enabled", "pitr"],
                        "properties": {
                            "enabled": { "type": "boolean" },
                            "pitr": { "type": "boolean" }
                        }
                    },
                    "encryption": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["at_rest"],
                        "properties": {
                            "at_rest": { "type": "boolean" }
                        }
                    },
                    "audit": {
                        "type": "object",
                        "additionalProperties": false,
                        "required": ["enabled"],
                        "properties": {
                            "enabled": { "type": "boolean" }
                        }
                    },
                    "sensitive_data": { "type": "boolean" },
                    "strict_cross_domain_transactions": { "type": "boolean" }
                }
            },
            "deployment": {
                "type": "object",
                "additionalProperties": false,
                "required": ["mode", "storage_path"],
                "properties": {
                    "mode": {
                        "type": "string",
                        "enum": ["embedded", "single_node", "cluster"]
                    },
                    "storage_path": {
                        "type": ["string", "null"]
                    }
                }
            },
            "topology": {
                "type": "object",
                "additionalProperties": false,
                "required": ["replica_count", "shard_count"],
                "properties": {
                    "replica_count": { "type": "integer", "minimum": 1, "maximum": 65535 },
                    "shard_count": { "type": "integer", "minimum": 1, "maximum": 65535 }
                }
            },
            "extension_ref": {
                "type": "object",
                "additionalProperties": false,
                "required": ["name", "version", "stability"],
                "properties": {
                    "name": { "type": "string", "minLength": 1 },
                    "version": { "type": "string", "minLength": 1 },
                    "stability": {
                        "type": "string",
                        "enum": ["stable", "experimental"]
                    }
                }
            },
            "string_map": {
                "type": "object",
                "additionalProperties": { "type": "string" },
                "propertyNames": { "minLength": 1 }
            }
        }
    })
}
