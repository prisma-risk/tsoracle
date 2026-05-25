{{- define "tsoracle.name" -}}{{ .Release.Name }}{{- end -}}
{{- define "tsoracle.peerService" -}}{{ .Release.Name }}-peer{{- end -}}
{{- define "tsoracle.image" -}}{{ .Values.image.repository }}:{{ .Values.image.tag | default .Chart.AppVersion }}{{- end -}}
{{- define "tsoracle.replicas" -}}{{ if eq .Values.driver "file" }}1{{ else }}{{ .Values.replicas }}{{ end }}{{- end -}}
{{- define "tsoracle.labels" -}}
app.kubernetes.io/name: tsoracle
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
{{- define "tsoracle.validate" -}}
{{- if not (has .Values.driver (list "file" "openraft" "paxos")) }}{{ fail "driver must be file, openraft, or paxos" }}{{- end }}
{{- if and .Values.tls.enabled (not .Values.tls.secretName) }}{{ fail "tls.enabled requires tls.secretName" }}{{- end }}
{{- if and (ne .Values.driver "file") (not .Values.tls.enabled) (not .Values.tls.allowInsecurePeer) }}{{ fail "driver=openraft/paxos exposes a consensus peer port that runs unauthenticated plaintext when tls.enabled=false; set tls.enabled=true with tls.secretName to enable peer mTLS, or set tls.allowInsecurePeer=true to intentionally deploy plaintext consensus (e.g. dev/test behind a NetworkPolicy)" }}{{- end }}
{{- if and (eq .Values.driver "file") (gt (int .Values.replicas) 1) }}{{ fail "driver=file is single-node; set replicas: 1" }}{{- end }}
{{- end -}}
