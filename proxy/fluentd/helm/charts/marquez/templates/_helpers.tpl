{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "marquez.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "marquez.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "marquez.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Return the proper Marquez image name
*/}}
{{- define "marquez.image" -}}
{{- include "common.images.image" (dict "imageRoot" .Values.marquez.image "global" .Values.global) -}}
{{- end -}}

{{/*
Return the proper Marquez web image name
*/}}
{{- define "web.image" -}}
{{- include "common.images.image" (dict "imageRoot" .Values.web.image "global" .Values.global) -}}
{{- end -}}

{{/*
Get the secret name
*/}}
{{- define "marquez.secretName" -}}
{{- if .Values.marquez.existingSecretName -}}
  {{- printf "%s" .Values.marquez.existingSecretName -}}
{{- else -}}
  {{- printf "%s" (include "common.names.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
Create a default fully qualified postgresql name.
*/}}
{{- define "marquez.postgresql.fullname" -}}
{{- $name := default "postgresql" .Values.postgresql.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Flexible Postgres database host, using an existing or newly created instance.
*/}}
{{- define "marquez.database.host" -}}
  {{- if eq .Values.postgresql.enabled true -}}
    {{- template "marquez.postgresql.fullname" . -}}
  {{- else -}}
    {{- .Values.marquez.db.host -}}
  {{- end -}}
{{- end -}}

{{/*
Flexible Postgres database port, using an existing or newly created instance.
*/}}
{{- define "marquez.database.port" -}}
  {{- if eq .Values.postgresql.enabled true -}}
    {{- printf "%s" "5432" -}}
  {{- else -}}
    {{- .Values.marquez.db.port -}}
  {{- end -}}
{{- end -}}

{{/*
Flexible Postgres database name, using an existing or newly created instance.
*/}}
{{- define "marquez.database.name" -}}
  {{- if eq .Values.postgresql.enabled true -}}
    {{- .Values.postgresql.auth.database -}}
  {{- else -}}
    {{- .Values.marquez.db.name -}}
  {{- end -}}
{{- end -}}

{{/*
Flexible Postgres database user, using an existing or newly created instance.
*/}}
{{- define "marquez.database.user" -}}
  {{- if eq .Values.postgresql.enabled true -}}
    {{- .Values.postgresql.auth.username -}}
  {{- else -}}
    {{- .Values.marquez.db.user -}}
  {{- end -}}
{{- end -}}

{{/*
Postgres helm chart expects a specific secret name, when an override is not provided.
*/}}
{{- define "marquez.postgresql.secretName" -}}
{{- if and (.Values.postgresql.enabled) (not .Values.postgresql.auth.existingSecret) -}}
    {{- printf "%s" (include "marquez.postgresql.fullname" .) -}}
{{- else if and (.Values.postgresql.enabled) (.Values.postgresql.auth.existingSecret) -}}
    {{- printf "%s" .Values.postgresql.auth.existingSecret -}}
{{- else -}}
    {{- include "marquez.secretName" . -}}
{{- end -}}
{{- end -}}

{{/*
Postgres helm chart expects the password to exist within a specific key.
*/}}
{{- define "marquez.database.existingsecret.key" -}}
{{- if .Values.postgresql.enabled -}}
    {{- printf "%s" "password" -}}
{{- else -}}
    {{- printf "%s" "marquez-db-password" -}}
{{- end -}}
{{- end -}}