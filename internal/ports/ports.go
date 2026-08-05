package ports

import (
	"context"
	"io"

	"github.com/labring-sigs/kbbackup-prune/internal/domain"
)

type Kubernetes interface {
	Inventory(
		ctx context.Context,
		repoName, namespace string,
		useRepoCredentials bool,
	) (domain.Inventory, domain.S3Settings, error)
	BackupExists(ctx context.Context, key domain.BackupKey) (bool, error)
}

type ObjectStore interface {
	ListLevel(
		ctx context.Context,
		prefix, delimiter string,
		versions bool,
	) (domain.ObjectLevel, error)
	List(ctx context.Context, prefix string, versions bool) ([]domain.Object, error)
	Walk(
		ctx context.Context,
		prefix string,
		versions bool,
		visit func(domain.Object) error,
	) error
	Open(ctx context.Context, key string, maxBytes int64) (io.ReadCloser, error)
	Stat(ctx context.Context, key string) (domain.Object, error)
	Delete(ctx context.Context, objects []domain.Object) error
	Versioning(ctx context.Context) (string, error)
}
