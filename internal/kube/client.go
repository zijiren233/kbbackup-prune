package kube

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/labring-sigs/kbbackup-prune/internal/domain"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	backupGVR = schema.GroupVersionResource{
		Group: "dataprotection.kubeblocks.io", Version: "v1alpha1", Resource: "backups",
	}
	backupRepoGVR = schema.GroupVersionResource{
		Group: "dataprotection.kubeblocks.io", Version: "v1alpha1", Resource: "backuprepos",
	}
	restoreGVR = schema.GroupVersionResource{
		Group: "dataprotection.kubeblocks.io", Version: "v1alpha1", Resource: "restores",
	}
)

type ConfigOptions struct {
	Mode       string
	Kubeconfig string
	Context    string
	Timeout    time.Duration
	QPS        float32
	Burst      int
}

type Client struct {
	dynamic dynamic.Interface
	typed   kubernetes.Interface
}

func New(opts ConfigOptions) (*Client, error) {
	config, err := loadRESTConfig(opts)
	if err != nil {
		return nil, err
	}

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes dynamic client: %w", err)
	}

	typedClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client: %w", err)
	}

	return &Client{dynamic: dynamicClient, typed: typedClient}, nil
}

func NewForClients(dynamicClient dynamic.Interface, typedClient kubernetes.Interface) *Client {
	return &Client{dynamic: dynamicClient, typed: typedClient}
}

func loadRESTConfig(opts ConfigOptions) (*rest.Config, error) {
	mode := opts.Mode
	if mode == "" {
		mode = "auto"
	}

	var (
		config *rest.Config
		err    error
	)
	switch mode {
	case "in-cluster":
		config, err = rest.InClusterConfig()
	case "kubeconfig":
		config, err = loadKubeconfig(opts)
	case "auto":
		if os.Getenv("KUBERNETES_SERVICE_HOST") != "" &&
			os.Getenv("KUBERNETES_SERVICE_PORT") != "" &&
			opts.Kubeconfig == "" {
			config, err = rest.InClusterConfig()
		} else {
			config, err = loadKubeconfig(opts)
		}
	default:
		return nil, fmt.Errorf(
			"invalid Kubernetes mode %q; use auto, kubeconfig, or in-cluster",
			mode,
		)
	}

	if err != nil {
		return nil, fmt.Errorf("load Kubernetes configuration (%s): %w", mode, err)
	}

	config.UserAgent = "kbbackup-prune"
	if opts.Timeout > 0 {
		config.Timeout = opts.Timeout
	}

	if opts.QPS > 0 {
		config.QPS = opts.QPS
	}

	if opts.Burst > 0 {
		config.Burst = opts.Burst
	}

	return config, nil
}

func loadKubeconfig(opts ConfigOptions) (*rest.Config, error) {
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if opts.Kubeconfig != "" {
		rules.ExplicitPath = opts.Kubeconfig
	}

	overrides := &clientcmd.ConfigOverrides{CurrentContext: opts.Context}

	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, overrides).ClientConfig()
}

func (c *Client) Inventory(
	ctx context.Context,
	repoName string,
	namespace string,
	useRepoCredentials bool,
) (domain.Inventory, domain.S3Settings, error) {
	if repoName == "" {
		return domain.Inventory{}, domain.S3Settings{}, errors.New("BackupRepo name is required")
	}

	repoObject, err := c.dynamic.Resource(backupRepoGVR).Get(ctx, repoName, metav1.GetOptions{})
	if err != nil {
		return domain.Inventory{}, domain.S3Settings{}, fmt.Errorf(
			"get BackupRepo %q: %w",
			repoName,
			err,
		)
	}

	repo, settings, err := parseRepository(repoObject)
	if err != nil {
		return domain.Inventory{}, domain.S3Settings{}, err
	}

	settings.CredentialSource = domain.S3AuthSourceSDKChain

	if useRepoCredentials {
		credential := repo.GeneratedCSIDriverSecret

		referencePath := domain.S3AuthSourceGenerated
		if credential == nil {
			credential = repo.Credential
			referencePath = domain.S3AuthSourceSpec
		}

		if credential == nil {
			return domain.Inventory{}, domain.S3Settings{}, fmt.Errorf(
				"BackupRepo %q has no credential Secret reference",
				repoName,
			)
		}

		secret, secretErr := c.typed.CoreV1().Secrets(credential.Namespace).Get(
			ctx, credential.Name, metav1.GetOptions{},
		)
		if secretErr != nil {
			return domain.Inventory{}, domain.S3Settings{}, fmt.Errorf(
				"get BackupRepo %s Secret %s/%s: %w",
				referencePath,
				credential.Namespace,
				credential.Name,
				secretErr,
			)
		}

		settings.CredentialSource = referencePath
		settings.CredentialRef = &domain.SecretRef{
			Namespace: credential.Namespace,
			Name:      credential.Name,
		}

		settings.CredentialKeys = make([]string, 0, len(secret.Data))
		for key := range secret.Data {
			settings.CredentialKeys = append(settings.CredentialKeys, key)
		}

		sort.Strings(settings.CredentialKeys)

		settings.AccessKeyID = firstBytes(
			secret.Data,
			"accessKeyId",
			"accessKeyID",
			"access_key_id",
			"AWS_ACCESS_KEY_ID",
		)
		settings.SecretAccessKey = firstBytes(
			secret.Data,
			"secretAccessKey",
			"secret_access_key",
			"AWS_SECRET_ACCESS_KEY",
		)

		settings.SessionToken = firstBytes(
			secret.Data,
			"sessionToken",
			"session_token",
			"AWS_SESSION_TOKEN",
		)
		if endpoint := firstBytes(
			secret.Data,
			"endpoint",
			"AWS_ENDPOINT_URL_S3",
			"AWS_ENDPOINT_URL",
		); endpoint != "" {
			settings.Endpoint = endpoint
		}

		if region := firstBytes(
			secret.Data,
			"region",
			"AWS_REGION",
			"AWS_DEFAULT_REGION",
		); region != "" {
			settings.Region = region
		}

		if settings.AccessKeyID == "" || settings.SecretAccessKey == "" {
			return domain.Inventory{}, domain.S3Settings{}, errors.New(
				"BackupRepo credential Secret lacks accessKeyID or secretAccessKey",
			)
		}
	}

	backupList, err := c.dynamic.Resource(backupGVR).
		Namespace(metav1.NamespaceAll).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return domain.Inventory{}, domain.S3Settings{}, fmt.Errorf(
			"list Backup CRs across all namespaces: %w",
			err,
		)
	}

	inventory := domain.Inventory{
		Backups:          make(map[domain.BackupKey]domain.Backup),
		ProtectedBackups: make(map[domain.BackupKey]string),
		VolumeRoots:      make(map[string]domain.VolumeRoot),
		Repo:             repo,
	}
	for i := range backupList.Items {
		backup := parseBackup(&backupList.Items[i])
		if backup.Repo != repo.Name {
			continue
		}

		inventory.Backups[backup.Key] = backup
	}

	c.addRestoreProtections(ctx, &inventory)

	if repo.StorageClassName != "" {
		c.addPVCProtections(ctx, &inventory, &settings, namespace)
	}

	qualifyBackupPaths(&inventory)

	return inventory, settings, nil
}

func (c *Client) addRestoreProtections(ctx context.Context, inventory *domain.Inventory) {
	restores, err := c.dynamic.Resource(restoreGVR).
		Namespace(metav1.NamespaceAll).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		inventory.BlockingReasons = append(
			inventory.BlockingReasons,
			fmt.Sprintf("cannot list Restore CRs: %v", err),
		)

		return
	}

	for i := range restores.Items {
		restore := &restores.Items[i]

		phase, _, _ := unstructured.NestedString(restore.Object, "status", "phase")
		if phase == "Completed" || phase == "Failed" {
			continue
		}

		backupName, _, _ := unstructured.NestedString(restore.Object, "spec", "backup", "name")

		backupNamespace, _, _ := unstructured.NestedString(
			restore.Object,
			"spec",
			"backup",
			"namespace",
		)
		if backupName == "" || backupNamespace == "" {
			inventory.BlockingReasons = append(
				inventory.BlockingReasons,
				fmt.Sprintf(
					"active Restore %s/%s has an incomplete backup reference",
					restore.GetNamespace(),
					restore.GetName(),
				),
			)

			continue
		}

		state := phase
		if state == "" {
			state = "Pending"
		}

		key := domain.BackupKey{Namespace: backupNamespace, Name: backupName}
		inventory.ProtectedBackups[key] = fmt.Sprintf(
			"referenced by active Restore %s/%s (%s)",
			restore.GetNamespace(),
			restore.GetName(),
			state,
		)
	}
}

func parseRepository(
	object *unstructured.Unstructured,
) (domain.Repository, domain.S3Settings, error) {
	config, _, err := unstructured.NestedStringMap(object.Object, "spec", "config")
	if err != nil {
		return domain.Repository{}, domain.S3Settings{}, fmt.Errorf(
			"read BackupRepo spec.config: %w",
			err,
		)
	}

	pathPrefix, _, _ := unstructured.NestedString(object.Object, "spec", "pathPrefix")
	storageClass, _, _ := unstructured.NestedString(
		object.Object,
		"status",
		"generatedStorageClassName",
	)
	backupPVC, _, _ := unstructured.NestedString(object.Object, "status", "backupPVCName")
	repo := domain.Repository{
		Name: object.GetName(), UID: string(object.GetUID()), Generation: object.GetGeneration(),
		PathPrefix: pathPrefix, StorageClassName: storageClass,
		BackupPVCName: backupPVC, Config: config,
	}

	credential, found, nestedErr := unstructured.NestedStringMap(
		object.Object,
		"spec",
		"credential",
	)
	if nestedErr != nil {
		return domain.Repository{}, domain.S3Settings{}, fmt.Errorf(
			"read BackupRepo spec.credential: %w",
			nestedErr,
		)
	}

	if found {
		repo.Credential = &domain.SecretRef{
			Namespace: credential["namespace"],
			Name:      credential["name"],
		}
	}

	generatedCredential, found, nestedErr := unstructured.NestedStringMap(
		object.Object,
		"status",
		"generatedCSIDriverSecret",
	)
	if nestedErr != nil {
		return domain.Repository{}, domain.S3Settings{}, fmt.Errorf(
			"read BackupRepo status.generatedCSIDriverSecret: %w",
			nestedErr,
		)
	}

	if found {
		repo.GeneratedCSIDriverSecret = &domain.SecretRef{
			Namespace: generatedCredential["namespace"],
			Name:      generatedCredential["name"],
		}
	}

	insecure, _ := strconv.ParseBool(first(config, "insecure", "no_check_certificate"))
	settings := domain.S3Settings{
		Bucket: first(config, "bucket", "root"), Endpoint: first(config, "endpoint"),
		Region: first(config, "region"), Insecure: insecure,
	}

	return repo, settings, nil
}

func parseBackup(object *unstructured.Unstructured) domain.Backup {
	repo, _, _ := unstructured.NestedString(object.Object, "status", "backupRepoName")
	if repo == "" {
		repo = object.GetLabels()[domain.BackupRepoLabel]
	}

	backupPath, _, _ := unstructured.NestedString(object.Object, "status", "path")
	kopiaPath, _, _ := unstructured.NestedString(object.Object, "status", "kopiaRepoPath")

	parent, _, _ := unstructured.NestedString(object.Object, "status", "parentBackupName")
	if parent == "" {
		parent, _, _ = unstructured.NestedString(object.Object, "spec", "parentBackupName")
	}

	base, _, _ := unstructured.NestedString(object.Object, "status", "baseBackupName")

	return domain.Backup{
		Key: domain.BackupKey{Namespace: object.GetNamespace(), Name: object.GetName()},
		UID: string(
			object.GetUID(),
		),
		ClusterUID:       object.GetLabels()[domain.ClusterUIDLabel],
		Repo:             repo,
		Path:             cleanPrefix(backupPath),
		KopiaRepoPath:    cleanPrefix(kopiaPath),
		RawPath:          cleanPrefix(backupPath),
		RawKopiaRepoPath: cleanPrefix(kopiaPath),
		ParentBackupName: parent,
		BaseBackupName:   base,
	}
}

func (c *Client) addPVCProtections(
	ctx context.Context,
	inventory *domain.Inventory,
	settings *domain.S3Settings,
	namespace string,
) {
	storageClass, err := c.typed.StorageV1().
		StorageClasses().
		Get(ctx, inventory.Repo.StorageClassName, metav1.GetOptions{})
	if err != nil {
		inventory.BlockingReasons = append(
			inventory.BlockingReasons,
			fmt.Sprintf(
				"cannot inspect generated StorageClass %q: %v",
				inventory.Repo.StorageClassName,
				err,
			),
		)

		return
	}

	if settings.Bucket == "" {
		settings.Bucket = storageClass.Parameters["bucket"]
	}

	if repoBucket := first(
		inventory.Repo.Config,
		"bucket",
		"root",
	); repoBucket != "" && storageClass.Parameters["bucket"] != "" &&
		repoBucket != storageClass.Parameters["bucket"] {
		inventory.BlockingReasons = append(
			inventory.BlockingReasons,
			fmt.Sprintf(
				"BackupRepo bucket %q differs from generated StorageClass bucket %q",
				repoBucket,
				storageClass.Parameters["bucket"],
			),
		)
	}

	pvcs, err := c.typed.CoreV1().
		PersistentVolumeClaims(metav1.NamespaceAll).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		inventory.BlockingReasons = append(
			inventory.BlockingReasons,
			fmt.Sprintf("cannot list PVCs: %v", err),
		)

		return
	}

	pvs, err := c.typed.CoreV1().PersistentVolumes().List(ctx, metav1.ListOptions{})
	if err != nil {
		inventory.BlockingReasons = append(
			inventory.BlockingReasons,
			fmt.Sprintf("cannot list PVs: %v", err),
		)

		return
	}

	pvByName := make(map[string]*corev1.PersistentVolume, len(pvs.Items))
	for i := range pvs.Items {
		pvByName[pvs.Items[i].Name] = &pvs.Items[i]
	}

	indexVolumeRoots(
		inventory,
		pvcs.Items,
		pvs.Items,
		storageClass.Provisioner,
		settings.Bucket,
	)
	indexRepositoryPVRoots(
		inventory,
		pvs.Items,
		storageClass.Provisioner,
		settings.Bucket,
	)

	inventory.Repo.ObjectPrefixes = make(map[string]string)
	foundRepoPVC := false

	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		if pvc.Spec.StorageClassName == nil ||
			*pvc.Spec.StorageClassName != inventory.Repo.StorageClassName {
			continue
		}

		isRepoPVC := pvc.Name == inventory.Repo.BackupPVCName
		if isRepoPVC {
			if namespace != "" && pvc.Namespace != namespace {
				continue
			}

			if labeledRepo := pvc.Labels[domain.BackupRepoLabel]; labeledRepo != "" &&
				labeledRepo != inventory.Repo.Name {
				inventory.BlockingReasons = append(
					inventory.BlockingReasons,
					fmt.Sprintf(
						"BackupRepo PVC %s/%s is labeled for repository %q",
						pvc.Namespace,
						pvc.Name,
						labeledRepo,
					),
				)

				continue
			}

			foundRepoPVC = true

			c.addRepositoryObjectPrefix(
				inventory,
				pvc,
				pvByName,
				storageClass.Provisioner,
				settings.Bucket,
			)

			continue
		}

		resource := "PVC " + pvc.Namespace + "/" + pvc.Name

		prefixes := make([]string, 1, 5)

		prefixes[0] = string(pvc.UID)
		if pvc.Spec.VolumeName == "" {
			if pvc.Status.Phase == corev1.ClaimBound {
				inventory.BlockingReasons = append(
					inventory.BlockingReasons,
					resource+" is bound without spec.volumeName",
				)
			}

			addProtections(inventory, prefixes, resource)

			continue
		}

		pv := pvByName[pvc.Spec.VolumeName]
		if pv == nil {
			inventory.BlockingReasons = append(
				inventory.BlockingReasons,
				resource+" references an unreadable PV "+pvc.Spec.VolumeName,
			)

			continue
		}

		prefixes = append(prefixes, pv.Name)
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != storageClass.Provisioner {
			inventory.BlockingReasons = append(
				inventory.BlockingReasons,
				resource+" has a PV whose CSI source cannot be mapped safely",
			)

			continue
		}

		csiPrefixes, supported := extractCSIPrefixes(pv.Spec.CSI, settings.Bucket)
		if !supported {
			inventory.BlockingReasons = append(
				inventory.BlockingReasons,
				fmt.Sprintf(
					"%s uses unsupported CSI provisioner %q; object prefix mapping is unavailable",
					resource,
					pv.Spec.CSI.Driver,
				),
			)

			continue
		}

		if len(csiPrefixes) == 0 {
			inventory.BlockingReasons = append(
				inventory.BlockingReasons,
				resource+" has no safely mappable CSI object prefix",
			)

			continue
		}

		prefixes = append(prefixes, csiPrefixes...)
		addProtections(inventory, prefixes, resource)
	}

	if inventory.Repo.BackupPVCName != "" && !foundRepoPVC {
		resource := fmt.Sprintf("BackupRepo backup PVC %q", inventory.Repo.BackupPVCName)
		if namespace != "" {
			resource += " in namespace " + namespace
		}

		inventory.BlockingReasons = append(
			inventory.BlockingReasons,
			resource+" was not found",
		)
	}

	validateRepositoryObjectPrefixes(inventory)
}

func (c *Client) addRepositoryObjectPrefix(
	inventory *domain.Inventory,
	pvc *corev1.PersistentVolumeClaim,
	pvByName map[string]*corev1.PersistentVolume,
	provisioner string,
	bucket string,
) {
	resource := "BackupRepo PVC " + pvc.Namespace + "/" + pvc.Name
	if pvc.Spec.VolumeName == "" {
		inventory.BlockingReasons = append(
			inventory.BlockingReasons,
			resource+" is not bound to a PV",
		)

		return
	}

	pv := pvByName[pvc.Spec.VolumeName]
	if pv == nil {
		inventory.BlockingReasons = append(
			inventory.BlockingReasons,
			resource+" references an unreadable PV "+pvc.Spec.VolumeName,
		)

		return
	}

	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != provisioner {
		inventory.BlockingReasons = append(
			inventory.BlockingReasons,
			resource+" has a PV whose CSI source does not match the generated StorageClass",
		)

		return
	}

	if pv.Spec.CSI.Driver != domain.S3CSIDriverYandex {
		inventory.BlockingReasons = append(
			inventory.BlockingReasons,
			fmt.Sprintf(
				"%s uses unsupported CSI provisioner %q; repository root mapping is unavailable",
				resource,
				pv.Spec.CSI.Driver,
			),
		)

		return
	}

	prefix, ok := normalizeVolumePrefix(pv.Spec.CSI.VolumeHandle, bucket)
	if !ok || prefix == "" {
		inventory.BlockingReasons = append(
			inventory.BlockingReasons,
			resource+" has no safely mappable non-empty CSI volumeHandle prefix",
		)

		return
	}

	if existing, duplicate := inventory.Repo.ObjectPrefixes[pvc.Namespace]; duplicate {
		inventory.BlockingReasons = append(
			inventory.BlockingReasons,
			fmt.Sprintf(
				"namespace %s has multiple BackupRepo object prefixes %q and %q",
				pvc.Namespace,
				existing,
				prefix,
			),
		)

		return
	}

	inventory.Repo.ObjectPrefixes[pvc.Namespace] = prefix
	addVolumeRoot(inventory, domain.VolumeRoot{
		Prefix: prefix, Kind: domain.VolumeRootRepository, Namespace: pvc.Namespace,
		Resource: resource, Current: true,
	})
}

func indexVolumeRoots(
	inventory *domain.Inventory,
	pvcs []corev1.PersistentVolumeClaim,
	pvs []corev1.PersistentVolume,
	provisioner string,
	bucket string,
) {
	pvByName := make(map[string]*corev1.PersistentVolume, len(pvs))
	for i := range pvs {
		pv := &pvs[i]

		pvByName[pv.Name] = pv
		if isRepositoryPV(inventory.Repo, pv, provisioner) {
			continue
		}

		indexPVVolumeRoots(inventory, pv, "PV "+pv.Name, "", bucket)
	}

	for i := range pvcs {
		pvc := &pvcs[i]

		pv := pvByName[pvc.Spec.VolumeName]
		if pv != nil && isRepositoryPV(inventory.Repo, pv, provisioner) {
			continue
		}

		resource := "PVC " + pvc.Namespace + "/" + pvc.Name
		if root, ok := canonicalPVCRoot("pvc-" + string(pvc.UID)); ok {
			addVolumeRoot(inventory, domain.VolumeRoot{
				Prefix: root, Kind: domain.VolumeRootUser, Namespace: pvc.Namespace,
				Resource: resource, Current: true,
			})
		}

		if pv != nil {
			indexPVVolumeRoots(inventory, pv, resource, pvc.Namespace, bucket, true)
		}
	}
}

func indexRepositoryPVRoots(
	inventory *domain.Inventory,
	pvs []corev1.PersistentVolume,
	provisioner string,
	bucket string,
) {
	for i := range pvs {
		pv := &pvs[i]

		if !isRepositoryPV(inventory.Repo, pv, provisioner) {
			continue
		}

		claim := pv.Spec.ClaimRef

		prefix, ok := normalizeVolumePrefix(pv.Spec.CSI.VolumeHandle, bucket)
		if !ok {
			continue
		}

		root, ok := canonicalPVCRoot(prefix)
		if !ok {
			continue
		}

		addVolumeRoot(inventory, domain.VolumeRoot{
			Prefix: root, Kind: domain.VolumeRootRepository, Namespace: claim.Namespace,
			Resource: "BackupRepo PV " + pv.Name,
			Current:  pv.Status.Phase == corev1.VolumeBound,
		})
	}
}

func isRepositoryPV(
	repo domain.Repository,
	pv *corev1.PersistentVolume,
	provisioner string,
) bool {
	claim := pv.Spec.ClaimRef

	return claim != nil && isBackupRepoPVClaim(repo, claim.Name) &&
		pv.Spec.StorageClassName == repo.StorageClassName &&
		pv.Spec.CSI != nil && pv.Spec.CSI.Driver == provisioner &&
		pv.Spec.CSI.Driver == domain.S3CSIDriverYandex
}

func isBackupRepoPVClaim(repo domain.Repository, claimName string) bool {
	if claimName == repo.BackupPVCName {
		return true
	}

	uid := repo.UID
	if len(uid) < 8 {
		return false
	}

	name := fmt.Sprintf("pre-check-%s-%s", uid[:8], repo.Name)
	if len(name) > validation.DNS1123LabelMaxLength {
		name = strings.TrimSuffix(name[:validation.DNS1123LabelMaxLength], "-")
	}

	return claimName == name
}

func indexPVVolumeRoots(
	inventory *domain.Inventory,
	pv *corev1.PersistentVolume,
	resource string,
	namespace string,
	bucket string,
	current ...bool,
) {
	isCurrent := len(current) > 0 && current[0]

	values := []string{pv.Name}
	if pv.Spec.CSI != nil {
		if value, ok := normalizeVolumePrefix(pv.Spec.CSI.VolumeHandle, bucket); ok {
			values = append(values, value)
		}
	}

	for _, value := range values {
		root, ok := canonicalPVCRoot(value)
		if !ok {
			continue
		}

		addVolumeRoot(inventory, domain.VolumeRoot{
			Prefix: root, Kind: domain.VolumeRootUser, Namespace: namespace, Resource: resource,
			Current: isCurrent,
		})
	}
}

func canonicalPVCRoot(value string) (string, bool) {
	value = cleanPrefix(value)
	if strings.Contains(value, "/") || !strings.HasPrefix(value, "pvc-") {
		return "", false
	}

	parsed, err := uuid.Parse(strings.TrimPrefix(value, "pvc-"))
	if err != nil {
		return "", false
	}

	canonical := "pvc-" + parsed.String()

	return canonical, value == canonical
}

func addVolumeRoot(inventory *domain.Inventory, root domain.VolumeRoot) {
	root.Prefix = cleanPrefix(root.Prefix)
	if root.Prefix == "" {
		return
	}

	if inventory.VolumeRoots == nil {
		inventory.VolumeRoots = make(map[string]domain.VolumeRoot)
	}

	existing, found := inventory.VolumeRoots[root.Prefix]
	if found && existing.Kind != root.Kind {
		if existing.Kind == domain.VolumeRootUser {
			return
		}

		inventory.VolumeRoots[root.Prefix] = root

		return
	}

	if found && existing.Resource != "" {
		switch {
		case root.Current && !existing.Current:
			inventory.VolumeRoots[root.Prefix] = root
		case existing.Namespace == "" && root.Namespace != "":
			inventory.VolumeRoots[root.Prefix] = root
		}

		return
	}

	inventory.VolumeRoots[root.Prefix] = root
}

func validateRepositoryObjectPrefixes(inventory *domain.Inventory) {
	type entry struct {
		namespace string
		prefix    string
	}

	entries := make([]entry, 0, len(inventory.Repo.ObjectPrefixes))
	for namespace, prefix := range inventory.Repo.ObjectPrefixes {
		entries = append(entries, entry{namespace: namespace, prefix: prefix})
	}

	sort.Slice(entries, func(i, j int) bool { return entries[i].prefix < entries[j].prefix })

	for i := 1; i < len(entries); i++ {
		previous := entries[i-1]

		current := entries[i]
		if prefixContains(previous.prefix, current.prefix) {
			inventory.BlockingReasons = append(
				inventory.BlockingReasons,
				fmt.Sprintf(
					"BackupRepo object prefixes overlap: namespace %s %q and namespace %s %q",
					previous.namespace,
					previous.prefix,
					current.namespace,
					current.prefix,
				),
			)
		}
	}
}

func qualifyBackupPaths(inventory *domain.Inventory) {
	for key, backup := range inventory.Backups {
		root := inventory.Repo.ObjectPrefixes[key.Namespace]
		if root == "" {
			continue
		}

		backup.Path = qualifyObjectPath(root, backup.Path)
		backup.KopiaRepoPath = qualifyObjectPath(root, backup.KopiaRepoPath)
		inventory.Backups[key] = backup
	}
}

func qualifyObjectPath(root, value string) string {
	if value == "" || prefixContains(root, value) {
		return value
	}

	return path.Join(root, value)
}

func prefixContains(prefix, value string) bool {
	prefix = cleanPrefix(prefix)
	value = cleanPrefix(value)

	return prefix == "" || value == prefix || strings.HasPrefix(value, prefix+"/")
}

func extractCSIPrefixes(
	source *corev1.CSIPersistentVolumeSource,
	bucket string,
) ([]string, bool) {
	if source.Driver != domain.S3CSIDriverYandex {
		return nil, false
	}

	values := []string{source.VolumeHandle}
	for _, key := range []string{"prefix", "path", "subPath", "subdir", "root"} {
		values = append(values, source.VolumeAttributes[key])
	}

	for _, field := range []string{"options", "mountOptions"} {
		parts := strings.Fields(source.VolumeAttributes[field])
		for i, part := range parts {
			for _, flag := range []string{"--prefix", "--subdir", "--path"} {
				if part == flag && i+1 < len(parts) {
					values = append(values, parts[i+1])
				}

				if value, ok := strings.CutPrefix(part, flag+"="); ok {
					values = append(values, value)
				}
			}
		}
	}

	result := make([]string, 0, len(values))
	for _, value := range values {
		if prefix, ok := normalizeVolumePrefix(value, bucket); ok {
			result = append(result, prefix)
		}
	}

	return result, true
}

func normalizeVolumePrefix(value, bucket string) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", false
	}

	if parsed, err := url.Parse(value); err == nil && parsed.Scheme != "" {
		if parsed.Scheme == "s3" {
			if bucket != "" && parsed.Host != bucket {
				return "", false
			}
			return cleanPrefix(parsed.Path), true
		}

		return "", false
	}

	value = strings.TrimPrefix(value, bucket+"/")
	if value == bucket {
		return "", true
	}

	return cleanPrefix(value), true
}

func addProtections(inventory *domain.Inventory, prefixes []string, resource string) {
	seen := make(map[string]struct{})
	for _, existing := range inventory.Protections {
		seen[existing.Prefix+"\x00"+existing.Resource] = struct{}{}
	}

	for _, prefix := range prefixes {
		prefix = cleanPrefix(prefix)

		key := prefix + "\x00" + resource
		if _, ok := seen[key]; ok {
			continue
		}

		seen[key] = struct{}{}

		inventory.Protections = append(inventory.Protections, domain.Protection{
			Prefix: prefix, Kind: "pvc", Resource: resource,
		})
	}

	sort.Slice(inventory.Protections, func(i, j int) bool {
		if inventory.Protections[i].Prefix == inventory.Protections[j].Prefix {
			return inventory.Protections[i].Resource < inventory.Protections[j].Resource
		}
		return inventory.Protections[i].Prefix < inventory.Protections[j].Prefix
	})
}

func (c *Client) BackupExists(ctx context.Context, key domain.BackupKey) (bool, error) {
	_, err := c.dynamic.Resource(backupGVR).
		Namespace(key.Namespace).
		Get(ctx, key.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func cleanPrefix(value string) string {
	if value == "" || value == "/" {
		return ""
	}
	return strings.TrimPrefix(path.Clean("/"+value), "/")
}

func first(values map[string]string, keys ...string) string {
	for _, key := range keys {
		if values[key] != "" {
			return values[key]
		}
	}

	return ""
}

func firstBytes(values map[string][]byte, keys ...string) string {
	for _, key := range keys {
		if len(values[key]) > 0 {
			return string(values[key])
		}
	}

	return ""
}
