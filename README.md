# kbbackup-prune

`kbbackup-prune` 识别并清理 S3 中已经没有对应 KubeBlocks Backup CR 的备份目录和孤儿卷根。程序默认生成 dry-run 结果，真实删除需要显式确认。

## 安全边界

Mount 模式采用“所选 BackupRepo Bucket 归当前 Kubernetes 集群独占”的部署约束。部署前需保证其他集群和应用使用各自独立的 Bucket。

单个 `type=backup,state=orphan` 候选必须同时满足以下条件：

1. Mount 模式下目录符合 `pvc-<UUID>/<namespace>/<clusterName>-<clusterUUID>/<component>/<backupName>/` 布局；`backupName` 直接作为 Backup CR 名称，备份发现不依赖 `kubeblocks-backup.json`。
2. 目录存在清单时，清单的 namespace/name/UID 必须完整；使用 namespace 对应 backup PVC/PV 的对象根拼接 `status.path`，结果必须与 S3 目录完全一致；`status.backupRepoName` 必须与所选 BackupRepo 一致。
3. 集群中不存在同 namespace/name 的 Backup CR，也不存在重叠的现存 Backup 或 Kopia 路径。
4. 没有活跃 Restore 引用该备份。
5. 清单和 S3 对象都超过 `--min-age`，默认 7 天。
6. 目录存在清单时，只接受 `deletionPolicy: Delete` 和 `Retain`；`Retain` 默认受保护，未知值归类为无效清单。缺失清单时使用目录布局、Backup CR inventory、对象年龄和完整快照作为删除依据。
7. Mount 模式先使用 S3 delimiter 发现严格的 `pvc-<canonical UUID>/` 根，再使用 PVC UID、PV 名称和 CSI `volumeHandle` 建立当前卷根所有权索引。当前用户 PVC/PV 根直接保护并跳过内部列表。
8. 目录没有被仍需保留的增量/差异备份作为 parent 或 base 引用。
9. 执行计划使用第二次对象遍历生成最终快照，重新计算删除汇总并复核最小年龄；执行开始时刷新一次 Kubernetes inventory。首个候选完成最终 S3 快照后再统一刷新 BackupRepo、Backup、Restore、PVC、PV 和 StorageClass，并精确比较 namespace 到对象根的映射；同一次执行的候选共享该最终 inventory。真实 Kubernetes inventory 的 Backup map 覆盖单 Backup 候选复核，轻量端口实现可使用 `BackupExists` 回退。
10. `--bucket-versioning=auto` 会在删除前核对 Bucket versioning 状态；显式状态会作为运维声明写入计划。两种模式都会复核目录完整对象快照；存在清单时额外复核清单 ETag 和 VersionID/GCS generation，时间字段用于展示和年龄判断，版本化 Bucket 按 VersionID 精确删除。
11. 无版本 Bucket 在删除前执行完整对象快照复核。GCS 额外保存每个对象的 generation，并把 generation 作为 `VersionId` 提交给 Multi-Object Delete，实现对象身份约束。

当前 PVC 或 `Bound` PV 对应的 repository 根保持当前状态。claim 名匹配 BackupRepo backup PVC 或 pre-check PVC、同时处于 Released 等历史状态的 PV 根会进入历史 repository 判定。程序只列出根下 namespace 和 `clusterName-clusterUUID` 两层 topology，并用 Backup CR `status.path`、`status.kopiaRepoPath`、cluster UID、Restore、增量依赖和存储保护做引用审计。整个历史根没有引用时生成唯一的 `orphan-repository-root`；其内部 backup、`orphan-cluster-root` 和 `repository-stray` 候选全部省略。

历史根包含引用时继续使用细粒度流程。Mount 模式在 `pvc-<UUID>/<namespace>/` 下发现名称以 canonical UUID 结尾的 cluster 目录后，先查询内存中的 Backup CR inventory。相同 namespace 下存在 cluster UID 标签匹配、`status.path`/`status.kopiaRepoPath` 重叠、定位信息不完整、活跃 Restore 引用或 live backup 依赖时，程序继续深入扫描并按单个备份分类。没有任何引用时，程序扫描整个 `orphan-cluster-root`，按 `component/backupName` 拆分 Backup 候选，逐个应用最小年龄、manifest、依赖和 Kubernetes 保护检查；只覆盖 root 一部分的 `--prefix` 或存在存储保护时保留 root 级 protected 候选。

真实 prune 会枚举 `orphan-repository-root` 的全部对象，以及 `orphan-cluster-root` 下每个 Backup 候选的全部对象，复核 `--min-age`，读取其中的当前 manifest 并继续执行 Retain、清单有效性和 Kubernetes 保护检查，然后生成精确删除快照。首个最终对象快照完成后统一刷新 inventory；新出现的 Backup 路径、cluster UID、Restore、依赖、当前 PVC/PV 或存储保护都会使候选失效。Kubernetes 复核与 S3 删除属于两个独立系统，两次操作之间存在竞态窗口。只覆盖整根候选一部分的 `--prefix` 会把候选置为 protected。

`repository-stray` 表示仓库根下的松散对象、无效 namespace 目录、无效 cluster 目录，以及 cluster 内未形成规范 `component/backupName/` 边界的共享备份数据。规范备份目录统一使用 `type=backup`，由 `state` 区分 `live`、`orphan`、`retained` 和其他安全状态。`Size=0` 且 Key 以 `/` 结尾的 S3 CSI 目录标记属于结构元数据，程序会忽略标记对象并继续扫描其子目录。该类型默认保护；`--delete-repository-stray` 开启后才进入最小年龄和精确对象快照删除流程。非 `pvc-<UUID>/` 顶层对象保持在扫描范围外。PVC 到 S3 前缀的映射缺失、StorageClass/PV/PVC 权限不足或资源状态异常会产生 execution blocker，真实删除会停止。

CLI 的 table 和 JSON 只展示程序存在清理路径的候选：当前可清理的 `orphan`，以及可通过 `--include-retained`、`--min-age`、`--delete-repository-stray`、调整 `--prefix/--namespace` 解锁的候选。`state=live`、`protected-user-volume`、活动 Restore、live backup 依赖、无效 manifest 和其他硬保护候选仍在内部发现和对象归属阶段参与保护，并从候选输出隐藏。Kubernetes 权限、对象根映射和资源状态等 execution blocker 继续单独显示。

## 构建

要求 Go 1.26：

```bash
make verify
make build-linux-amd64
make build-linux-arm64
bin/kbbackup-prune version
```

项目使用当前稳定版依赖和 golangci-lint v2，规则来自用户目录的 `~/.golangci.yml`。

## Kubernetes 连接

默认 `--kube-mode=auto`：Pod 内使用 service account，其他环境使用 client-go 标准 kubeconfig 加载链。

```bash
# 标准 KUBECONFIG
export KUBECONFIG=/path/to/config
kbbackup-prune plan --backup-repo backup-io-gcp

# 显式文件和 context
kbbackup-prune plan \
  --kube-mode=kubeconfig \
  --kubeconfig=/path/to/config \
  --context=production \
  --backup-repo=backup-io-gcp

# Pod 内
kbbackup-prune plan \
  --kube-mode=in-cluster \
  --backup-repo=backup-io-gcp
```

Helm Chart 默认创建基础最小只读权限：

| API 资源 | 权限 | 用途 |
| --- | --- | --- |
| `backups.dataprotection.kubeblocks.io` | `get,list` | 建立现存备份集合和执行前复查 |
| `backuprepos.dataprotection.kubeblocks.io` | `get` | 读取仓库、路径前缀和生成资源名 |
| `restores.dataprotection.kubeblocks.io` | `list` | 保护活跃恢复任务正在读取的备份 |
| `persistentvolumeclaims` | `list` | 建立所有当前 PVC 的卷根所有权和 BackupRepo PVC 映射 |
| `persistentvolumes` | `list` | 将绑定 PVC 映射到 CSI S3 对象根 |
| `storageclasses.storage.k8s.io` | `get` | 验证生成 StorageClass 和 Bucket |
| `secrets` | 资源级 `get` | 读取 `status.generatedCSIDriverSecret` 或回退的 `spec.credential` |

程序默认读取 `BackupRepo.status.generatedCSIDriverSecret`，采用 StorageProvider 已标准化的 endpoint、region 和 S3 凭据；状态引用为空时读取 `spec.credential`。使用 `--use-backup-repo-credentials=false` 可切换到 AWS SDK 凭据链，包括环境变量、shared config、Web Identity/IRSA 和实例角色。Helm 的 `secretReader` values 可以为实际选中的单个 Secret 创建资源级 `get` 权限。

S3 读取计划需要 `s3:ListBucket` 和 `s3:GetObject`。默认 `--bucket-versioning=auto` 还需要 `s3:GetBucketVersioning`；显式声明版本状态会跳过该请求。启用 `--purge-versions` 时还需要 `s3:ListBucketVersions`。真实清理增加 `s3:DeleteObject`，永久删除历史版本增加 `s3:DeleteObjectVersion`；Bucket 级动作绑定 Bucket ARN，对象级动作可限制到 BackupRepo PVC/PV 映射出的对象根。

GCS XML API 的 `?versioning` 属于 Bucket metadata 请求，HMAC Service Account 需要 `storage.buckets.get`。可以在目标 Bucket 上授予 `roles/storage.legacyBucketReader`，或使用包含该权限的自定义角色。已经通过独立证据确认关闭版本管理时，可以使用 `--bucket-versioning=disabled` 跳过该权限和请求。

GCS Soft Delete 与 XML API object versioning 是两项独立能力。Bucket 启用 Soft Delete 时，清理请求删除 live generation，Cloud Storage 按 Bucket 的 Soft Delete retention 保存可恢复副本；`--bucket-versioning` 只描述 object versioning 状态。

程序会精确识别 `googleapis.com` 下的 Cloud Storage endpoint，并自动启用 AWS SDK Go v2 的 GCS SigV4 兼容层：签名前移除 GCS canonicalization 不接受的 `Accept-Encoding`、`Amz-Sdk-Invocation-Id` 和 `Amz-Sdk-Request`，签名后恢复 `Accept-Encoding: identity`。该行为同时覆盖列表、分页、对象读取和删除请求，其他 S3-compatible endpoint 保持标准 AWS SDK 行为。

## Helm 部署

Chart 位于 `charts/kbbackup-prune`。默认工作负载是执行一次后结束的 `Job`，默认命令是只读 `plan`，安装过程不会创建定时任务：

```bash
helm upgrade --install kbbackup-prune ./charts/kbbackup-prune \
  --namespace=kb-system \
  --create-namespace \
  --set config.backupRepo=backup-io-gcp \
  --set secretReader.create=true \
  --set secretReader.namespace=sealos \
  --set secretReader.secretName=secret-backup-io-gcp-rwlfhn \
  --set image.tag=latest

kubectl logs \
  --namespace=kb-system \
  --selector=app.kubernetes.io/instance=kbbackup-prune \
  --all-containers=true
```

`secretReader.namespace` 和 `secretReader.secretName` 对应 BackupRepo
`status.generatedCSIDriverSecret`；该状态引用为空时填写 `spec.credential`。使用已有授权
ServiceAccount 时设置 `serviceAccount.create=false` 并提供 `serviceAccount.name`。

Job 名称包含 Helm release revision。需要重新执行时升级同一个 release：

```bash
helm upgrade kbbackup-prune ./charts/kbbackup-prune \
  --namespace=kb-system \
  --reuse-values
```

执行真实清理时，Chart 会在渲染阶段校验确认词：

```bash
helm upgrade kbbackup-prune ./charts/kbbackup-prune \
  --namespace=kb-system \
  --reuse-values \
  --set config.command=prune \
  --set config.dryRun=false \
  --set config.purgeVersions=true \
  --set-string config.confirm=DELETE
```

默认读取 BackupRepo Secret。使用 Chart 管理 RBAC 时开启资源级 Secret 权限；下面的名称对应 `status.generatedCSIDriverSecret`，状态引用为空时填写 `spec.credential`：

```bash
helm upgrade --install kbbackup-prune ./charts/kbbackup-prune \
  --namespace=kb-system \
  --create-namespace \
  --set config.backupRepo=backup-io-gcp \
  --set config.bucketVersioning=disabled \
  --set secretReader.create=true \
  --set secretReader.namespace=backup-secret-namespace \
  --set secretReader.secretName=secret-backup-io-gcp-rwlfhn
```

定期审计需要显式选择 `CronJob`：

```bash
helm upgrade --install kbbackup-prune ./charts/kbbackup-prune \
  --namespace=kb-system \
  --create-namespace \
  --set config.backupRepo=backup-io-gcp \
  --set secretReader.create=true \
  --set secretReader.namespace=sealos \
  --set secretReader.secretName=secret-backup-io-gcp-rwlfhn \
  --set workload.kind=CronJob \
  --set-string 'workload.schedule=17 2 * * *'
```

完整配置和 Pod 调度、IRSA 注解、额外环境变量、Volume 挂载选项见 `charts/kbbackup-prune/values.yaml`。

## 使用

BackupRepo 的 `spec.config` 会提供 bucket、endpoint、region 和 TLS 设置。CLI 参数用于覆盖连接端点或补齐配置；`--bucket` 与 BackupRepo Bucket 不一致时程序会终止。

S3 默认使用 virtual-hosted-style 地址。需要兼容 path-style 的 MinIO、RustFS 或私有端点时使用 `--path-style=true`；布尔参数采用 `--flag=false` 形式显式关闭。

```bash
# 只读计划
kbbackup-prune plan \
  --backup-repo=backup-io-gcp \
  --output=table

# JSON 审计结果；发现可清理对象时返回退出码 2
kbbackup-prune plan \
  --backup-repo=backup-io-gcp \
  --output=json \
  --fail-on-orphans

# prune 默认仍为 dry-run
kbbackup-prune prune \
  --backup-repo=backup-io-gcp

# 已确认 Bucket 关闭 versioning 后执行删除
kbbackup-prune prune \
  --backup-repo=backup-io-gcp \
  --bucket-versioning=disabled \
  --dry-run=false \
  --confirm=DELETE

# 显式允许清理 repository-stray
kbbackup-prune prune \
  --backup-repo=backup-io-gcp \
  --bucket-versioning=disabled \
  --delete-repository-stray \
  --dry-run=false \
  --confirm=DELETE-STRAY
```

常用安全选项：

```text
--min-age=168h                 最小年龄
--namespace=dataflow-system   只扫描该 namespace 的 BackupRepo PVC 对象根
--prefix=pvc-uuid/ns/cluster  进一步缩小完整对象前缀，必须位于选定 PVC 对象根内
--concurrency=4               备份目录并发数
--timeout=30m                 整个命令的最长执行时间；大批量清理可显式提高
--delete-repository-stray     允许删除 repository-stray，默认保护
--show-all                    展示 live 和硬保护资源；默认只展示存在清理路径的候选
--bucket-versioning=auto      自动查询版本状态；可显式声明 disabled/enabled/suspended
--ca-file=/path/to/ca.pem     私有 S3 CA
--debug                       向 stderr 输出脱敏连接诊断
```

已枚举候选的对象数量和字节数直接来自 `ListObjectsV2` 或 `ListObjectVersions` 响应，用于审计展示和执行快照一致性校验。`orphan-repository-root` 在只读 plan 中显示 `OBJECTS=deferred`；`orphan-cluster-root` 会在发现阶段展开为单个 Backup 候选并显示对象数量，真实 prune 再为可清理候选生成精确快照。manifest 安全检查会读取每个当前 `kubeblocks-backup.json`，普通数据对象不会增加 HEAD/Get 请求。

Table 输出中的 `Discovered PVC roots` 统计 bucket 根部发现的全部 canonical `pvc-UUID` 前缀，并按 BackupRepo repository 根、受保护用户卷、无主卷和其他根分类。当前 PVC、历史 Released BackupRepo PV 和当前 BackupRepo pre-check PV 都会保留 repository 身份；claim 名不同的用户卷保持硬保护，程序跳过其内部对象枚举。

### Debug 诊断

`--debug` 在首次 S3 请求之前向 stderr 输出凭据来源、Secret 引用和 key 名、脱敏 Access Key、凭据 SHA-256 短指纹、endpoint、region、bucket、path-style、版本状态模式、显式扫描前缀和 BackupRepo PVC 对象根数量。选择单一 namespace 时也会输出其对象根。后续请求还会输出 AWS SDK 的 Canonical String 与 String-to-Sign，用于比较签名兼容性；这些内容不包含 Access Key、Secret、Session Token 或最终 Signature。stdout 继续保持 table/JSON 业务输出。原始 Secret Access Key、Session Token、endpoint userinfo 和查询参数保留在进程内。

```bash
kbbackup-prune plan \
  --backup-repo=backup-io-gcp \
  --debug
```

示例 stderr：

```text
debug: credential_source="status.generatedCSIDriverSecret"
debug: credential_ref="sealos/secret-backup-io-gcp-rwlfhn"
debug: credential_keys="accessKeyID,endpoint,secretAccessKey"
debug: access_key_id masked="GOOG...ABCD" bytes=61 sha256=0123456789ab
debug: secret_access_key present=true bytes=40 sha256=abcdef012345
debug: session_token present=false bytes=0 sha256=<empty>
debug: s3 bucket="sealos-cloud" endpoint="https://storage.googleapis.com" region="asia-southeast1" path_style=false insecure_tls=false
debug: bucket_versioning_mode="auto"
debug: scan_prefix=""
debug: repository_object_prefixes=1
debug: repository_object_prefix_namespace="dataflow-system"
debug: repository_object_prefix="pvc-8fd207da-088a-416f-8860-6a9f73116334"
```

Helm 使用 `--set config.debug=true` 开启相同行为。

### Retain 数据

`deletionPolicy: Retain` 表达保留意图。显式纳入时使用加强确认词：

```bash
kbbackup-prune prune \
  --backup-repo=backup-io-gcp \
  --include-retained \
  --dry-run=false \
  --confirm=DELETE-RETAINED
```

同时启用 Retain 和 repository-stray 删除时使用 `DELETE-RETAINED-AND-STRAY`。

### Bucket 版本控制与条件删除

`--bucket-versioning` 支持以下模式：

| 值 | 行为 |
| --- | --- |
| `auto` | 默认值；调用 `GetBucketVersioning`，计划记录 `versioningSource: detected`，执行前再次查询并比较状态 |
| `disabled` | 跳过版本查询，计划记录 `Disabled` 和 `versioningSource: operator-override` |
| `enabled` | 跳过版本查询并按已启用版本管理处理 |
| `suspended` | 跳过版本查询并按已暂停版本管理处理 |

显式模式是运维者对 Bucket 当前状态的声明。`disabled` 会按无版本对象执行删除；`enabled` 和 `suspended` 的真实清理要求 `--purge-versions`。

Bucket 开启或暂停过版本控制时，程序默认阻断真实删除；确认需要永久删除全部版本后加入 `--purge-versions`。删除请求携带精确 VersionID，因此扫描后出现的新版本会保留。对象锁、legal hold 和 Bucket policy 仍由 S3 服务端执行，失败会记录到每个前缀的结果中。

AWS S3 支持在 `DeleteObjects` 的每个 ObjectIdentifier 中携带 ETag 条件。当前 MinIO 和 RustFS 会忽略这个 XML 字段。程序会在删除前复核完整对象快照，最终检查到服务端执行之间仍存在覆盖竞态：

```bash
kbbackup-prune prune \
  --backup-repo=backup-io-gcp \
  --bucket-versioning=disabled \
  --dry-run=false \
  --confirm=DELETE
```

该模式仍会在删除前核对完整对象快照，并把 List/Delete 之间的剩余覆盖竞态作为显式运维风险。

## 环境变量

CLI 支持 `KBBACKUP_PRUNE_BACKUP_REPO`、`KBBACKUP_PRUNE_NAMESPACE`、`KBBACKUP_PRUNE_BUCKET`、`KBBACKUP_PRUNE_ENDPOINT`、`KBBACKUP_PRUNE_PREFIX` 和 `KBBACKUP_PRUNE_KUBE_MODE`。AWS 凭据和 region 使用 AWS SDK 标准环境变量。

## 测试

```bash
go test -race ./...
go test -covermode=atomic -coverprofile=coverage.out ./...
golangci-lint run ./...
make helm-lint
```

`internal/objectstore` 的集成测试通过 testcontainers 覆盖以下 S3-compatible 服务：

| 服务 | 镜像版本 | 原生 `DeleteObjects` 校验行为 | ETag 条件删除 |
| --- | --- | --- | --- |
| MinIO | `RELEASE.2024-01-16T16-07-38Z` | 强制 `Content-MD5` | 忽略 ObjectIdentifier ETag |
| MinIO | `RELEASE.2025-09-07T16-13-09Z` | 接受 AWS SDK 默认 CRC32 | 忽略 ObjectIdentifier ETag |
| RustFS | `1.0.0-beta.12` | 接受 AWS SDK 默认 CRC32 | 忽略 ObjectIdentifier ETag |

AWS SDK v2 的 `DeleteObjectsInput.ChecksumAlgorithm=MD5` 当前会在客户端校验阶段返回 `unknown checksum algorithm, MD5`。适配器移除该操作的 flexible-checksum middleware，再通过 Smithy middleware 为每个批量删除请求计算标准 `Content-MD5`，同时兼容以上三种服务，并保留每批最多 1000 个对象的批量删除。测试覆盖 MD5 与 flexible checksum 的排他性、ETag 条件行为、对象读取、分页接口、普通删除、Bucket 版本控制、历史版本永久删除，以及 GCS endpoint、generation、版本列表互操作请求头和 SigV4 signed headers。Docker 不可用时测试会 skip；CI 可设置 `REQUIRE_TESTCONTAINERS=true` 强制执行。

GCS XML API 的 Multi-Object Delete 请求体使用 `Key` 和可选 `VersionId`。适配器从普通对象列表的 `<Generation>` 保存对象 generation，版本列表请求携带并签名 `x-goog-interop-list-objects-format: enabled`，删除时把 generation 映射为 `VersionId`。缺少 generation 或 VersionID 的 GCS 对象会停止删除并报告身份缺失。其他 S3 兼容服务使用带 ETag 的批量删除；`MalformedMultiObjectDeleteRequest` 会原样返回，便于按服务能力显式处理。`Quiet=false` 让执行汇总准确记录批量请求中的部分成功。大批量任务可提高 `--timeout` 和 `--concurrency`，例如 `--timeout=2h --concurrency=16`。

## 架构

```text
cmd/kbbackup-prune        进程、信号和退出码
internal/cli              参数、配置合并和审计输出
internal/domain           Backup、候选状态、计划和执行结果
internal/ports            Kubernetes 与对象存储端口
internal/kube             client-go 资源发现和 PVC 保护
internal/objectstore      AWS SDK v2 S3 适配器
internal/prune            清单校验、规划、安全门和删除执行
charts/kbbackup-prune     Job-first Helm 部署、RBAC 和可选 CronJob
```

清理器会从已引用 cluster 和无当前 Backup CR 引用的 `orphan-cluster-root` 中发现规范的 `component/backupName/` 目录；缺少 `kubeblocks-backup.json` 且没有 Backup CR 的目录归类为 `type=backup,state=orphan`。现存 Backup CR 对应的目录归类为 `type=backup,state=live`。cluster 内缺少单个备份边界的共享数据和无效仓库目录归类为 `repository-stray`，并默认保护。没有任何引用的历史 BackupRepo PV 根归类为单个 `orphan-repository-root` 并省略内层候选。孤儿卷根具备严格的 `pvc-<UUID>/` 形状且当前没有 PVC/PV 映射时归类为 `orphan-volume-root`。
