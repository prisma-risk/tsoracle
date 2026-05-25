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
{{- if and (eq .Values.driver "file") (gt (int .Values.replicas) 1) }}{{ fail "driver=file is single-node; set replicas: 1" }}{{- end }}
{{- end -}}
