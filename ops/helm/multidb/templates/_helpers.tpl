{{- define "multidb.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "multidb.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}
{{- end -}}

{{- define "multidb.validate" -}}
{{- $profile := lower (replace "-" "_" .Values.config.profile) -}}
{{- $memory := has $profile (list "inmemory" "in_memory" "memory") -}}
{{- $tensor := has $profile (list "tensor" "tensor_dataset" "deeplake_like") -}}
{{- if ne (int .Values.replicaCount) 1 -}}{{ fail "replicaCount must be1: this chart runs a single writer; extra pods are not live Raft/AP" }}{{- end -}}
{{- if and (eq .Values.runtime.mode "production") (not .Values.image.digest) -}}{{ fail "production requires image.digest pinned to a real verified sha256 digest" }}{{- end -}}
{{- if and (eq .Values.runtime.mode "production") (not .Values.audit.enabled) -}}{{ fail "production requires a real private audit key Secret; the development key is local-dev only" }}{{- end -}}
{{- if and .Values.audit.enabled (or (eq .Values.audit.anchorPath .Values.runtime.dbPath) (hasPrefix (printf "%s/" .Values.runtime.dbPath) .Values.audit.anchorPath) (hasPrefix (printf "%s/" .Values.audit.anchorPath) .Values.runtime.dbPath)) -}}{{ fail "audit anchor and database file paths must not overlap" }}{{- end -}}
{{- if and .Values.audit.enabled (not (regexMatch "^/var/lib/multidb/[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$" .Values.audit.anchorPath)) -}}{{ fail "audit anchor requires a private child directory on the data volume; the PVC root is not a private anchor parent" }}{{- end -}}
{{- if and (eq .Values.runtime.mode "production") (not .Values.runtime.allowVolatile) (or (not .Values.persistence.enabled) .Values.persistence.devEmptyDir) -}}{{ fail "durable production profiles require persistence; InMemory needs explicit runtime.allowVolatile" }}{{- end -}}
{{- if and (not .Values.spec.enabled) $memory (not .Values.runtime.allowVolatile) -}}{{ fail "InMemory requires runtime.allowVolatile=true; no durability/restart survival is promised" }}{{- end -}}
{{- if and .Values.runtime.allowVolatile (or (not $memory) .Values.spec.enabled .Values.persistence.enabled (not .Values.persistence.devEmptyDir) .Values.encryption.enabled) -}}{{ fail "allowVolatile requires flat InMemory, devEmptyDir, persistence=false and encryption=false; volatile serve specs are unsupported" }}{{- end -}}
{{- if and (eq .Values.runtime.mode "production") (not .Values.networkPolicy.enabled) -}}{{ fail "production requires default-deny networkPolicy.enabled" }}{{- end -}}
{{- if and (eq .Values.runtime.mode "production") (ne .Values.service.type "ClusterIP") -}}{{ fail "production requires ClusterIP; expose HTTP through an authenticated TLS proxy/Ingress" }}{{- end -}}
{{- if and (not .Values.persistence.enabled) (not .Values.persistence.devEmptyDir) -}}{{ fail "persistence.enabled=false requires explicit local-dev devEmptyDir=true" }}{{- end -}}
{{- if and .Values.persistence.devEmptyDir .Values.persistence.existingClaim -}}{{ fail "devEmptyDir and existingClaim conflict" }}{{- end -}}
{{- range $path := list .Values.runtime.dbPath .Values.runtime.studioDir .Values.secrets.pgTls.mountPath .Values.spec.mountPath .Values.encryption.mountPath .Values.audit.mountPath .Values.audit.anchorPath -}}
{{- if or (contains "//" $path) (has ".." (splitList "/" $path)) (has "." (splitList "/" $path)) -}}{{ fail "mount/runtime paths must be canonical, without repeated separators or dot traversal" }}{{- end -}}
{{- end -}}
{{- $studio := printf "%s/" (trimSuffix "/" .Values.runtime.studioDir) -}}
{{- range $path := list "/var/lib/multidb" "/etc/multidb/auth" "/tmp" .Values.secrets.pgTls.mountPath .Values.spec.mountPath .Values.encryption.mountPath .Values.audit.mountPath -}}
{{- $private := printf "%s/" (trimSuffix "/" $path) -}}
{{- if or (hasPrefix $studio $private) (hasPrefix $private $studio) -}}{{ fail "studio assets must not overlap database, credentials, spec, keys or writable temp mounts" }}{{- end -}}
{{- end -}}
{{- range $path := list .Values.secrets.pgTls.mountPath .Values.spec.mountPath .Values.encryption.mountPath .Values.audit.mountPath -}}
{{- $mount := printf "%s/" (trimSuffix "/" $path) -}}
{{- if or (hasPrefix "/var/lib/multidb/" $mount) (hasPrefix $mount "/var/lib/multidb/") -}}{{ fail "credential, spec and key mounts must not overlap the writable data volume" }}{{- end -}}
{{- end -}}
{{- if and (not .Values.spec.enabled) $tensor (not .Values.gpu.enabled) -}}{{ fail "Tensor profile requires explicit GPU deployment values and real supported hardware" }}{{- end -}}
{{- if .Values.gpu.enabled -}}
{{- $resources := dict "nvidia" "nvidia.com/gpu" "amd" "amd.com/gpu" "intel" "gpu.intel.com/i915" -}}
{{- $resource := default (get $resources .Values.gpu.vendor) .Values.gpu.resourceName -}}
{{- if or (not (hasKey $resources .Values.gpu.vendor)) (not $resource) (not (hasKey .Values.resources.limits $resource)) (not (hasKey .Values.resources.requests $resource)) -}}{{ fail "GPU deployment requires supported vendor and matching vendor extended resources in requests/limits" }}{{- end -}}
{{- if ne (printf "%v" (get .Values.resources.limits $resource)) (printf "%v" (get .Values.resources.requests $resource)) -}}{{ fail "GPU extended resource requests and limits must match" }}{{- end -}}
{{- end -}}
{{- if ne .Values.runtime.adminBind (printf "0.0.0.0:%v" .Values.service.adminPort) -}}{{ fail "runtime.adminBind must match service.adminPort on0.0.0.0" }}{{- end -}}
{{- if ne .Values.runtime.pgBind (printf "0.0.0.0:%v" .Values.service.pgwirePort) -}}{{ fail "runtime.pgBind must match service.pgwirePort on0.0.0.0" }}{{- end -}}
{{- if le (int .Values.runtime.terminationGracePeriodSeconds) (add (int .Values.runtime.preStopDelaySeconds) 20) -}}{{ fail "termination grace must exceed preStop delay plus20s drain budget" }}{{- end -}}
{{- range $key := list "checksum/config" "multidb.io/upgrade-policy" "multidb.io/downgrade-policy" "multidb.io/storage-durability" -}}
{{- if hasKey $.Values.podAnnotations $key -}}{{ fail (printf "podAnnotations cannot override reserved key%s" $key) }}{{- end -}}
{{- end -}}
{{- if and .Values.ingress.enabled (or (not .Values.ingress.principalHeaderTrustAcknowledged) (not .Values.ingress.tls) (not .Values.ingress.hosts) (not .Values.networkPolicy.proxyPeers)) -}}{{ fail "Ingress requires TLS hosts, explicit proxyPeers and principalHeaderTrustAcknowledged" }}{{- end -}}
{{- if .Values.ingress.enabled -}}
{{- $tlsHosts := list -}}
{{- range .Values.ingress.tls -}}{{- $tlsHosts = concat $tlsHosts .hosts -}}{{- end -}}
{{- range .Values.ingress.hosts -}}{{- if not (has . $tlsHosts) -}}{{ fail (printf "Ingress host%s has no declared TLS host entry" .) }}{{- end -}}{{- end -}}
{{- end -}}
{{- if and .Values.monitoring.serviceMonitor.enabled (or (not .Values.monitoring.enabled) (not .Values.secrets.adminToken.name) (not .Values.networkPolicy.metricsPeers)) -}}{{ fail "ServiceMonitor requires monitoring.enabled, adminToken secret and metricsPeers" }}{{- end -}}
{{- end -}}

{{- define "multidb.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "multidb.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
