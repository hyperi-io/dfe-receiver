{{/*
Expand the name of the chart.
*/}}
{{- define "dfe-receiver.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
Truncated at 63 chars because some K8s name fields are limited.
*/}}
{{- define "dfe-receiver.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "dfe-receiver.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "dfe-receiver.labels" -}}
helm.sh/chart: {{ include "dfe-receiver.chart" . }}
{{ include "dfe-receiver.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "dfe-receiver.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dfe-receiver.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name.
*/}}
{{- define "dfe-receiver.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "dfe-receiver.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
kafka secret name -- use existing or generate from fullname.
*/}}
{{- define "dfe-receiver.kafkaSecretName" -}}
{{- if .Values.kafka.existingSecret }}
{{- .Values.kafka.existingSecret }}
{{- else }}
{{- printf "%s-kafka" (include "dfe-receiver.fullname" .) }}
{{- end }}
{{- end }}

{{/*
auth secret name -- use existing or generate from fullname.
*/}}
{{- define "dfe-receiver.authSecretName" -}}
{{- if .Values.auth.existingSecret }}
{{- .Values.auth.existingSecret }}
{{- else }}
{{- printf "%s-auth" (include "dfe-receiver.fullname" .) }}
{{- end }}
{{- end }}
