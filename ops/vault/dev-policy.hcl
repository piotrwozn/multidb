path "secret/data/multidb/kek" {
  capabilities = ["create", "read", "update"]
}

path "secret/metadata/multidb/kek" {
  capabilities = ["read", "list"]
}

path "transit/keys/multidb-*" {
  capabilities = ["create", "read", "update"]
}

path "transit/encrypt/multidb-*" {
  capabilities = ["update"]
}

path "transit/decrypt/multidb-*" {
  capabilities = ["update"]
}

path "transit/rewrap/multidb-*" {
  capabilities = ["update"]
}
