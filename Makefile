SHELL := /bin/sh

BINARY ?= bin/kbbackup-prune
VERSION ?= dev
LDFLAGS := -s -w -X main.version=$(VERSION)
CHART ?= charts/kbbackup-prune
HELM_TEST_REPO ?= test-repo
HELM_LONG_NAME := aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
HELM_TEST_BASE := --set config.backupRepo=$(HELM_TEST_REPO) \
	--set config.useBackupRepoCredentials=false

.PHONY: build build-linux-amd64 build-linux-arm64 test test-integration coverage lint helm-lint verify

build:
	go build -trimpath -ldflags '$(LDFLAGS)' -o $(BINARY) ./cmd/kbbackup-prune

build-linux-amd64:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
		go build -trimpath -ldflags '$(LDFLAGS)' \
		-o bin/kbbackup-prune-linux-amd64 ./cmd/kbbackup-prune

build-linux-arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
		go build -trimpath -ldflags '$(LDFLAGS)' \
		-o bin/kbbackup-prune-linux-arm64 ./cmd/kbbackup-prune

test:
	go test -race ./...

test-integration:
	REQUIRE_TESTCONTAINERS=true go test -race -count=1 -run 'MinIO|RustFS' ./internal/objectstore

coverage:
	go test -covermode=atomic -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

lint:
	golangci-lint run ./...

helm-lint:
	helm lint $(CHART) $(HELM_TEST_BASE)
	helm template test $(CHART) --namespace kb-system $(HELM_TEST_BASE) >/dev/null
	helm template test $(CHART) --namespace kb-system \
		$(HELM_TEST_BASE) \
		--set workload.kind=CronJob >/dev/null
	helm template test $(CHART) --namespace kb-system \
		$(HELM_TEST_BASE) \
		--set config.debug=true >/dev/null
	@helm template test $(CHART) --namespace kb-system \
		$(HELM_TEST_BASE) \
		--set config.showAll=true | \
		grep -q -- '"--show-all"'
	@if helm template test $(CHART) --namespace kb-system \
		$(HELM_TEST_BASE) | \
		grep -q -- '"--show-all"'; then \
		echo "expected show-all flag to be disabled by default" >&2; \
		exit 1; \
	fi
	helm template test $(CHART) --namespace kb-system \
		$(HELM_TEST_BASE) \
		--set config.bucketVersioning=disabled >/dev/null
	helm template test $(CHART) --namespace kb-system \
		$(HELM_TEST_BASE) \
		--set config.command=prune \
		--set config.dryRun=false \
		--set-string config.confirm=DELETE >/dev/null
	helm template test $(CHART) --namespace kb-system \
		$(HELM_TEST_BASE) \
		--set config.command=prune \
		--set config.deleteRepositoryStray=true \
		--set config.dryRun=false \
		--set-string config.confirm=DELETE-STRAY >/dev/null
	@helm template test $(CHART) --namespace kb-system \
		$(HELM_TEST_BASE) \
		--set config.deleteRepositoryStray=true | \
		grep -q -- '"--delete-repository-stray"'
	@if helm template test $(CHART) --namespace kb-system \
		$(HELM_TEST_BASE) | \
		grep -q -- '"--delete-repository-stray"'; then \
		echo "expected repository-stray deletion flag to be disabled by default" >&2; \
		exit 1; \
	fi
	helm template test $(CHART) --namespace kb-system \
		--set config.backupRepo=$(HELM_TEST_REPO) \
		--set config.useBackupRepoCredentials=true \
		--set secretReader.create=true \
		--set secretReader.namespace=secrets \
		--set secretReader.secretName=repo-creds >/dev/null
	@if helm template test $(CHART) --namespace kb-system \
		--set config.backupRepo=$(HELM_TEST_REPO) >/dev/null 2>&1; then \
		echo "expected chart-managed repository credentials without a Secret reader to fail" >&2; \
		exit 1; \
	fi
	helm template test $(CHART) --namespace kb-system \
		--set config.backupRepo=$(HELM_TEST_REPO) \
		--set serviceAccount.create=false \
		--set serviceAccount.name=externally-authorized >/dev/null
	helm template test $(CHART) --namespace kb-system \
		--set config.backupRepo=$(HELM_TEST_REPO) \
		--set rbac.create=false >/dev/null
	@if helm template test $(CHART) --namespace kb-system \
		--set config.backupRepo=$(HELM_TEST_REPO) \
		--set secretReader.create=true >/dev/null 2>&1; then \
		echo "expected an incomplete Secret reader reference to fail" >&2; \
		exit 1; \
	fi
	@job_name=$$(helm template test $(CHART) \
		$(HELM_TEST_BASE) \
		--set fullnameOverride=$(HELM_LONG_NAME) \
		--show-only templates/workload.yaml | \
		awk '/^kind: Job$$/{job=1} job && /^  name:/{print $$2; exit}'); \
		test $${#job_name} -le 63; \
		case "$$job_name" in *-1) ;; *) exit 1 ;; esac
	@cron_name=$$(helm template test $(CHART) \
		$(HELM_TEST_BASE) \
		--set fullnameOverride=$(HELM_LONG_NAME) \
		--set workload.kind=CronJob \
		--show-only templates/workload.yaml | \
		awk '/^kind: CronJob$$/{cron=1} cron && /^  name:/{print $$2; exit}'); \
		test $${#cron_name} -le 52
	@if helm template test $(CHART) --namespace kb-system \
		$(HELM_TEST_BASE) \
		--set config.command=prune \
		--set config.dryRun=false >/dev/null 2>&1; then \
		echo "expected live prune without confirmation to fail" >&2; \
		exit 1; \
	fi
	@if helm template test $(CHART) --namespace kb-system \
		$(HELM_TEST_BASE) \
		--set config.command=prune \
		--set config.deleteRepositoryStray=true \
		--set config.dryRun=false \
		--set-string config.confirm=DELETE >/dev/null 2>&1; then \
		echo "expected repository-stray deletion with weak confirmation to fail" >&2; \
		exit 1; \
	fi
	@if helm template test $(CHART) --namespace kb-system \
		--set config.useBackupRepoCredentials=false >/dev/null 2>&1; then \
		echo "expected missing BackupRepo to fail" >&2; \
		exit 1; \
	fi

verify: lint test helm-lint build
