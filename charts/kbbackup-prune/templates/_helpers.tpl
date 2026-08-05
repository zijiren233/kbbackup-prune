{{- define "kbbackup-prune.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "kbbackup-prune.fullname" -}}
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

{{- define "kbbackup-prune.jobName" -}}
{{- $suffix := printf "-%d" .Release.Revision }}
{{- $baseLimit := sub 63 (len $suffix) | int }}
{{- $base := include "kbbackup-prune.fullname" . | trunc $baseLimit | trimSuffix "-" }}
{{- printf "%s%s" $base $suffix }}
{{- end }}

{{- define "kbbackup-prune.clusterRoleName" -}}
{{- printf "%s-%s" (include "kbbackup-prune.fullname" .) .Release.Namespace | trunc 253 | trimSuffix "-" }}
{{- end }}

{{- define "kbbackup-prune.cronJobName" -}}
{{- include "kbbackup-prune.fullname" . | trunc 52 | trimSuffix "-" }}
{{- end }}

{{- define "kbbackup-prune.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "kbbackup-prune.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kbbackup-prune.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "kbbackup-prune.labels" -}}
helm.sh/chart: {{ include "kbbackup-prune.chart" . }}
{{ include "kbbackup-prune.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "kbbackup-prune.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "kbbackup-prune.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- required "serviceAccount.name is required when serviceAccount.create=false" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "kbbackup-prune.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) }}
{{- end }}

{{- define "kbbackup-prune.args" -}}
{{- $repo := required "config.backupRepo is required" .Values.config.backupRepo }}
{{- if and (eq .Values.config.command "prune") (not .Values.config.dryRun) }}
{{- $expected := "DELETE" }}
{{- if and .Values.config.includeRetained .Values.config.deleteRepositoryStray }}
{{- $expected = "DELETE-RETAINED-AND-STRAY" }}
{{- else if .Values.config.includeRetained }}
{{- $expected = "DELETE-RETAINED" }}
{{- else if .Values.config.deleteRepositoryStray }}
{{- $expected = "DELETE-STRAY" }}
{{- end }}
{{- if ne .Values.config.confirm $expected }}
{{- fail (printf "config.confirm must be %s for this live prune configuration" $expected) }}
{{- end }}
{{- end }}
- {{ .Values.config.command | quote }}
- {{ printf "--backup-repo=%s" $repo | quote }}
{{- if .Values.config.debug }}
- "--debug"
{{- end }}
- {{ printf "--kube-mode=%s" .Values.config.kubeMode | quote }}
- {{ printf "--output=%s" .Values.config.output | quote }}
- {{ printf "--request-timeout=%s" .Values.config.requestTimeout | quote }}
- {{ printf "--timeout=%s" .Values.config.timeout | quote }}
- {{ printf "--manifest-name=%s" .Values.config.manifestName | quote }}
- {{ printf "--min-age=%s" .Values.config.minAge | quote }}
- {{ printf "--path-style=%t" .Values.config.pathStyle | quote }}
- {{ printf "--bucket-versioning=%s" .Values.config.bucketVersioning | quote }}
{{- with .Values.config.kubeconfig }}
- {{ printf "--kubeconfig=%s" . | quote }}
{{- end }}
{{- with .Values.config.context }}
- {{ printf "--context=%s" . | quote }}
{{- end }}
{{- with .Values.config.namespace }}
- {{ printf "--namespace=%s" . | quote }}
{{- end }}
{{- with .Values.config.bucket }}
- {{ printf "--bucket=%s" . | quote }}
{{- end }}
{{- with .Values.config.endpoint }}
- {{ printf "--endpoint=%s" . | quote }}
{{- end }}
{{- with .Values.config.region }}
- {{ printf "--region=%s" . | quote }}
{{- end }}
{{- with .Values.config.prefix }}
- {{ printf "--prefix=%s" . | quote }}
{{- end }}
{{- with .Values.config.caFile }}
- {{ printf "--ca-file=%s" . | quote }}
{{- end }}
{{- if .Values.config.insecureSkipTLSVerify }}
- "--insecure-skip-tls-verify"
{{- end }}
- {{ printf "--use-backup-repo-credentials=%t" .Values.config.useBackupRepoCredentials | quote }}
{{- if .Values.config.includeRetained }}
- "--include-retained"
{{- end }}
{{- if .Values.config.deleteRepositoryStray }}
- "--delete-repository-stray"
{{- end }}
{{- if .Values.config.purgeVersions }}
- "--purge-versions"
{{- end }}
{{- if .Values.config.showAll }}
- "--show-all"
{{- end }}
{{- if and (eq .Values.config.command "plan") .Values.config.failOnOrphans }}
- "--fail-on-orphans"
{{- end }}
{{- if eq .Values.config.command "prune" }}
- {{ printf "--dry-run=%t" .Values.config.dryRun | quote }}
- {{ printf "--concurrency=%d" (int .Values.config.concurrency) | quote }}
{{- with .Values.config.confirm }}
- {{ printf "--confirm=%s" . | quote }}
{{- end }}
{{- end }}
{{- range .Values.config.extraArgs }}
- {{ . | quote }}
{{- end }}
{{- end }}

{{- define "kbbackup-prune.podSpec" -}}
serviceAccountName: {{ include "kbbackup-prune.serviceAccountName" . }}
automountServiceAccountToken: true
restartPolicy: Never
terminationGracePeriodSeconds: {{ .Values.terminationGracePeriodSeconds }}
securityContext:
{{- toYaml .Values.podSecurityContext | nindent 2 }}
{{- with .Values.imagePullSecrets }}
imagePullSecrets:
{{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.priorityClassName }}
priorityClassName: {{ . }}
{{- end }}
containers:
  - name: {{ .Chart.Name }}
    image: {{ include "kbbackup-prune.image" . | quote }}
    imagePullPolicy: {{ .Values.image.pullPolicy }}
    args:
{{- include "kbbackup-prune.args" . | nindent 6 }}
    securityContext:
{{- toYaml .Values.securityContext | nindent 6 }}
    resources:
{{- toYaml .Values.resources | nindent 6 }}
{{- with .Values.extraEnv }}
    env:
{{- toYaml . | nindent 6 }}
{{- end }}
{{- with .Values.envFrom }}
    envFrom:
{{- toYaml . | nindent 6 }}
{{- end }}
{{- with .Values.extraVolumeMounts }}
    volumeMounts:
{{- toYaml . | nindent 6 }}
{{- end }}
{{- with .Values.extraVolumes }}
volumes:
{{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.nodeSelector }}
nodeSelector:
{{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.affinity }}
affinity:
{{- toYaml . | nindent 2 }}
{{- end }}
{{- with .Values.tolerations }}
tolerations:
{{- toYaml . | nindent 2 }}
{{- end }}
{{- end }}
