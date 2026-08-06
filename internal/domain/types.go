package domain

import "time"

const (
	BackupAPIVersion               = "dataprotection.kubeblocks.io/v1alpha1"
	BackupKind                     = "Backup"
	BackupRepoLabel                = "dataprotection.kubeblocks.io/backup-repo-name"
	ClusterUIDLabel                = "dataprotection.kubeblocks.io/cluster-uid"
	DefaultManifest                = "kubeblocks-backup.json"
	DeletionPolicyDelete           = "Delete"
	DeletionPolicyRetain           = "Retain"
	S3CSIDriverYandex              = "ru.yandex.s3.csi"
	BucketVersioningDisabled       = "Disabled"
	BucketVersioningEnabled        = "Enabled"
	BucketVersioningSuspended      = "Suspended"
	BucketVersioningModeAuto       = "auto"
	BucketVersioningModeDisabled   = "disabled"
	BucketVersioningModeEnabled    = "enabled"
	BucketVersioningModeSuspended  = "suspended"
	BucketVersioningSourceDetected = "detected"
	BucketVersioningSourceOverride = "operator-override"
	S3AuthSourceSDKChain           = "aws-sdk-default-chain"
	S3AuthSourceGenerated          = "status.generatedCSIDriverSecret"
	S3AuthSourceSpec               = "spec.credential"
)

type BackupKey struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

func (k BackupKey) String() string {
	return k.Namespace + "/" + k.Name
}

type Backup struct {
	Key              BackupKey `json:"key"`
	UID              string    `json:"uid,omitempty"`
	ClusterUID       string    `json:"clusterUID,omitempty"`
	Repo             string    `json:"repo"`
	Path             string    `json:"path"`
	KopiaRepoPath    string    `json:"kopiaRepoPath,omitempty"`
	RawPath          string    `json:"-"`
	RawKopiaRepoPath string    `json:"-"`
	ParentBackupName string    `json:"parentBackupName,omitempty"`
	BaseBackupName   string    `json:"baseBackupName,omitempty"`
}

type Repository struct {
	Name                     string            `json:"name"`
	UID                      string            `json:"uid,omitempty"`
	Generation               int64             `json:"generation,omitempty"`
	PathPrefix               string            `json:"pathPrefix,omitempty"`
	ObjectPrefixes           map[string]string `json:"objectPrefixes,omitempty"`
	StorageClassName         string            `json:"storageClassName,omitempty"`
	BackupPVCName            string            `json:"backupPVCName,omitempty"`
	Config                   map[string]string `json:"-"`
	Credential               *SecretRef        `json:"-"`
	GeneratedCSIDriverSecret *SecretRef        `json:"-"`
}

type SecretRef struct {
	Namespace string
	Name      string
}

type S3Settings struct {
	Bucket           string
	Endpoint         string
	Region           string
	AccessKeyID      string
	SecretAccessKey  string
	SessionToken     string
	Insecure         bool
	CredentialSource string
	CredentialRef    *SecretRef
	CredentialKeys   []string
}

type Protection struct {
	Prefix   string `json:"prefix"`
	Kind     string `json:"kind"`
	Resource string `json:"resource"`
}

type VolumeRootKind string

const (
	VolumeRootRepository VolumeRootKind = "repository"
	VolumeRootUser       VolumeRootKind = "user"
)

type VolumeRoot struct {
	Prefix    string         `json:"prefix"`
	Kind      VolumeRootKind `json:"kind"`
	Namespace string         `json:"namespace,omitempty"`
	Resource  string         `json:"resource"`
	Current   bool           `json:"current,omitempty"`
}

type VolumeRootCounts struct {
	Total         int `json:"total"`
	Repository    int `json:"repository"`
	ProtectedUser int `json:"protectedUser"`
	Unowned       int `json:"unowned"`
	Other         int `json:"other"`
}

type Inventory struct {
	Backups          map[BackupKey]Backup  `json:"backups"`
	ProtectedBackups map[BackupKey]string  `json:"protectedBackups,omitempty"`
	Repo             Repository            `json:"repository"`
	Protections      []Protection          `json:"protections,omitempty"`
	VolumeRoots      map[string]VolumeRoot `json:"volumeRoots,omitempty"`
	BlockingReasons  []string              `json:"blockingReasons,omitempty"`
}

type Object struct {
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	LastModified time.Time `json:"lastModified"`
	ETag         string    `json:"etag,omitempty"`
	VersionID    string    `json:"versionId,omitempty"`
	// Generation is the Google Cloud Storage object generation. GCS exposes
	// this identity on ordinary object listings, while the S3 model calls the
	// same value VersionId only in its interoperability version listing.
	Generation   string `json:"generation,omitempty"`
	DeleteMarker bool   `json:"deleteMarker,omitempty"`
}

type ObjectLevel struct {
	Objects  []Object `json:"objects,omitempty"`
	Prefixes []string `json:"prefixes,omitempty"`
}

type BackupManifest struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Metadata   struct {
		Name              string            `json:"name"`
		Namespace         string            `json:"namespace"`
		UID               string            `json:"uid"`
		CreationTimestamp time.Time         `json:"creationTimestamp"`
		Labels            map[string]string `json:"labels"`
	} `json:"metadata"`
	Spec struct {
		DeletionPolicy   string `json:"deletionPolicy"`
		ParentBackupName string `json:"parentBackupName"`
	} `json:"spec"`
	Status struct {
		BackupRepoName string     `json:"backupRepoName"`
		Path           string     `json:"path"`
		KopiaRepoPath  string     `json:"kopiaRepoPath"`
		Phase          string     `json:"phase"`
		CompletionTime *time.Time `json:"completionTimestamp"`
		ParentBackup   string     `json:"parentBackupName"`
		BaseBackup     string     `json:"baseBackupName"`
	} `json:"status"`
}

func (m BackupManifest) Key() BackupKey {
	return BackupKey{Namespace: m.Metadata.Namespace, Name: m.Metadata.Name}
}

func (m BackupManifest) Repo() string {
	if m.Status.BackupRepoName != "" {
		return m.Status.BackupRepoName
	}
	return m.Metadata.Labels[BackupRepoLabel]
}

type CandidateState string

type CandidateKind string

const (
	CandidateBackup               CandidateKind = "backup"
	CandidateOrphanClusterRoot    CandidateKind = "orphan-cluster-root"
	CandidateOrphanRepositoryRoot CandidateKind = "orphan-repository-root"
	CandidateOrphanVolumeRoot     CandidateKind = "orphan-volume-root"
	CandidateRepositoryStray      CandidateKind = "repository-stray"
	CandidateProtectedUserVolume  CandidateKind = "protected-user-volume"
)

const (
	StateOrphan          CandidateState = "orphan"
	StateLive            CandidateState = "live"
	StateRetained        CandidateState = "retained"
	StateTooYoung        CandidateState = "too-young"
	StateProtected       CandidateState = "protected"
	StateDependency      CandidateState = "dependency"
	StateInvalidManifest CandidateState = "invalid-manifest"
)

type Candidate struct {
	Kind                 CandidateKind   `json:"type"`
	Backup               BackupKey       `json:"backup"`
	UID                  string          `json:"uid,omitempty"`
	Prefix               string          `json:"prefix"`
	ScopePrefix          string          `json:"scopePrefix,omitempty"`
	DeferredScan         bool            `json:"deferredScan,omitempty"`
	FullScopeSnapshot    bool            `json:"-"`
	ManifestKey          string          `json:"manifestKey"`
	ManifestETag         string          `json:"manifestETag,omitempty"`
	ManifestVersionID    string          `json:"manifestVersionId,omitempty"`
	ManifestGeneration   string          `json:"manifestGeneration,omitempty"`
	CreatedAt            time.Time       `json:"createdAt"`
	LastModified         time.Time       `json:"lastModified"`
	DeletionPolicy       string          `json:"deletionPolicy,omitempty"`
	ParentBackup         string          `json:"parentBackupName,omitempty"`
	BaseBackup           string          `json:"baseBackupName,omitempty"`
	State                CandidateState  `json:"state"`
	Reason               string          `json:"reason"`
	DeletionConfigurable bool            `json:"deletionConfigurable,omitempty"`
	Objects              []Object        `json:"-"`
	ScopeObjects         []Object        `json:"-"`
	ObjectCount          int             `json:"objectCount"`
	Bytes                int64           `json:"bytes"`
	Protection           *Protection     `json:"protection,omitempty"`
	Manifest             *BackupManifest `json:"-"`
}

type Plan struct {
	GeneratedAt           time.Time              `json:"generatedAt"`
	Repository            string                 `json:"repository"`
	RepositoryUID         string                 `json:"repositoryUID,omitempty"`
	RepositoryGeneration  int64                  `json:"repositoryGeneration,omitempty"`
	Bucket                string                 `json:"bucket"`
	Namespace             string                 `json:"namespace,omitempty"`
	Prefix                string                 `json:"prefix,omitempty"`
	Prefixes              []string               `json:"prefixes,omitempty"`
	ObjectPrefixes        map[string]string      `json:"objectPrefixes,omitempty"`
	Versioning            string                 `json:"versioning"`
	VersioningSource      string                 `json:"versioningSource"`
	DryRun                bool                   `json:"dryRun"`
	VolumeDiscovery       bool                   `json:"volumeDiscovery,omitempty"`
	VolumeRootCounts      *VolumeRootCounts      `json:"volumeRootCounts,omitempty"`
	DeleteRepositoryStray bool                   `json:"deleteRepositoryStray,omitempty"`
	Candidates            []Candidate            `json:"candidates"`
	StateCounts           map[CandidateState]int `json:"stateCounts"`
	ScannedObjects        int                    `json:"scannedObjects"`
	ScannedBytes          int64                  `json:"scannedBytes"`
	UnclassifiedObjects   int                    `json:"unclassifiedObjects"`
	UnclassifiedBytes     int64                  `json:"unclassifiedBytes"`
	DeleteObjects         int                    `json:"deleteObjects"`
	DeleteBytes           int64                  `json:"deleteBytes"`
	BlockingReasons       []string               `json:"blockingReasons,omitempty"`
	OrphanVolumeRoots     []string               `json:"orphanVolumeRoots,omitempty"`
}

type DeleteResult struct {
	Prefix         string `json:"prefix"`
	ObjectsDeleted int    `json:"objectsDeleted"`
	BytesDeleted   int64  `json:"bytesDeleted"`
	Error          string `json:"error,omitempty"`
}

type DeleteFailure struct {
	Object  Object `json:"object"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type DeleteReport struct {
	Deleted []Object        `json:"deleted,omitempty"`
	Failed  []DeleteFailure `json:"failed,omitempty"`
}

type Execution struct {
	DryRun  bool           `json:"dryRun"`
	Results []DeleteResult `json:"results"`
}
