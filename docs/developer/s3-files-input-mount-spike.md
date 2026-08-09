# Spike Brief: Amazon S3 Files as the demand execution input path

**Status:** Spike — investigation only, no implementation
**Created:** 2026-08-07
**Repos in scope:** `aibs-informatics-aws-lambda`, `aibs-informatics-cdk-lib`
**Related:** OCSDV-452, OCSDV-453, aws-lambda#39, cdk-lib#61

> This is a working design brief, not published documentation. It is deliberately
> not in the mkdocs nav.

---

## 1. Why this is on the table

Concurrent demand executions moving 200 GB–1 TB exhaust a single bursting-mode EFS
file system's throughput. The open PRs (aws-lambda#39, cdk-lib#61) address this by
letting each execution land on one of N candidate file systems.

That is the right fix for today, but it treats a symptom. In bursting mode, baseline
throughput is **50 KiB/s per GiB stored**, and scratch file systems are near-empty by
design — so every individual file system has a near-zero baseline and survives only on
burst credits. N file systems multiplies the credit pool by N; it does not change any
single file system's baseline.

Alternatives already considered and **rejected**:

| Option | Verdict |
|---|---|
| EFS Elastic throughput | **Rejected on cost.** ~$0.03/GB read, ~$0.06/GB write — roughly $90–150 per TB round-tripped. Converts a reliability problem into a line item that scales with data volume. |
| EFS Provisioned throughput | Not evaluated in depth; fixed $/MBps-month is wasteful given bursty load. |
| FSx for Lustre | Batch has supported it since 2020, but **only via EC2 launch templates**, which are a static asset in our system. Same rigidity problem as Mountpoint for S3. |
| AWS DataSync | ~$0.0125/GB, and it is a migration tool, not a per-job data path. Wrong shape. |
| Folding stage-in into the compute job | **Rejected.** EFS exists specifically to decouple phases so I/O-bound transfer runs on cheap instances while CPU/GPU-bound science runs on expensive ones. Folding transfer in means paying expensive-instance rates to wait on S3. |

**Amazon S3 Files** (GA 2026-04-08) is new and changes the calculus: NFS v4.1+ file
system access directly over S3 buckets, built on EFS technology, and — critically —
configurable **at the AWS Batch job definition level with no launch template**.

---

## 2. Scope decision (already made — do not relitigate)

**Input mount only. Outputs remain on the existing copy path.**

Rationale: S3 Files synchronization is asynchronous. Filesystem writes appear in S3
within *minutes*; S3 changes appear in the filesystem within seconds to a minute. The
current upload path gives an explicit "outputs are durable in S3 now" moment that
chained executions depend on. Under an S3 Files mount, "the job finished" and "the
outputs are in S3" become different moments.

Do not put outputs on an S3 Files mount.

---

## 3. What S3 Files gives us

Job definition shape (ECS properties):

```json
"volumes": [{
  "name": "myS3FilesVolume",
  "s3filesVolumeConfiguration": {
    "fileSystemArn": "arn:aws:s3files:<region>:<account>:file-system/<fs-id>",
    "accessPointArn": "arn:aws:s3files:<region>:<account>:file-system/<fs-id>/access-point/<fsap-id>"
  }
}]
```

This mirrors the per-job EFS volume config we already use. **No launch template.**

S3 Files access points enforce a POSIX user identity and root directory, mapping
closely onto our existing `MountPointConfiguration` / EFS access point model. The
`get_efs_path` / `get_local_path` abstraction should gain a variant rather than
needing a rewrite.

### Cost profile (verify against the AWS pricing page before relying on this)

| | EFS Elastic | S3 Files |
|---|---|---|
| Storage | $0.30/GB-mo | $0.023/GB-mo (underlying S3) |
| Read | $0.03/GB always | **$0 for large sequential reads** |
| Write | $0.06/GB always | $0.06/GB (cache tier only) |
| Throughput tier | $6/MBps-mo | none |

The mechanism that matters: **files >= 1 MiB read sequentially bypass the cache tier
and stream directly from S3 via byte-range reads**, incurring no S3 Files charge. BAM
files are hundreds of MB to tens of GB, read sequentially. Our workload sits squarely
in the free path.

Storage/cache rates above come from third-party analysis, not the AWS pricing page.
**Confirming these is a spike deliverable.**

---

## 4. Blocker zero — resolve before anything else

> **From the AWS Batch docs:** "S3 Files are not supported on the Amazon EC2 launch
> type at this time. If you configure an S3 file system in a job definition and
> attempt to run it on the Amazon EC2 launch type, the job will fail at launch.
> Amazon EC2 launch type support is planned for a future release."

Our compute environments are EC2 (spot, specific instance types, GPU). So:

- **ECS on EC2** — closed today. No published date for support.
- **Fargate** — supported, but caps at 16 vCPU / 120 GB. Almost certainly too small
  for the science jobs. Verify against actual job resource requirements.
- **Batch on EKS** — **supported today** via `persistentVolumeClaim` in
  `eksProperties`. EKS nodes are EC2: spot, GPU, arbitrary instance types. The EC2
  restriction applies to the ECS launch type, not to EKS.

**Spike question 0:** Is Batch-on-EKS an acceptable target, or do we wait for
ECS-on-EC2 support? This gates everything else.

Note the Step Functions layer is unaffected either way — job definitions gain
`eksProperties` instead of `containerProperties`; the orchestration shape does not
change.

Also open a support/TAM thread on ECS-EC2 timing. If it is near, waiting may beat
migrating.

---

## 5. Open question A — permissions and the sandbox

Our Batch jobs are deliberately constrained to limit blast radius (pseudo-sandbox).
S3 Files complicates this in a specific, non-obvious way.

There are **two** IAM roles involved:

1. **File system role** — assumed by the S3 Files service (trust principal is
   `elasticfilesystem.amazonaws.com`) to synchronize between bucket and file system.
   Needs broad `s3:GetObject*` / `s3:PutObject*` / `s3:List*` on the bucket, KMS
   permissions, and EventBridge rule management (`DO-NOT-DELETE-S3-Files*`).
2. **Compute role** (our Batch job role) — needs S3 Files client permissions:
   - `s3files:ClientMount` — **read-only access**
   - `s3files:ClientWrite` — write (we do **not** want this)
   - `s3files:ClientRootAccess` — root user (we do **not** want this)

   Managed policy `AmazonS3FilesClientReadOnlyAccess` covers the read-only case.

### The sandbox problem

AWS also requires an **inline policy on the compute role granting direct
`s3:GetObject`, `s3:GetObjectVersion`, and `s3:ListBucket` on the source bucket**,
because the mount helper reads file data straight from S3 to optimize throughput.

This punches through access-point isolation. An access point constrains the *NFS* view
to a root directory, but a job holding direct `s3:GetObject` on the bucket can read
anything in that grant via the S3 API, mount or no mount.

**The lever:** per AWS, *"Without this policy, the mount helper cannot read directly
from S3 and all reads go through the file system."* The direct grant is a
**performance optimization, not a functional requirement**. That gives a real dial:

| | Direct S3 grant | No direct grant |
|---|---|---|
| Large reads | Stream free from S3 | Route through the file system (cache-tier rates) |
| Isolation | Job can read the entire granted scope via S3 API | POSIX + access-point root directory enforced |

**Mitigations to evaluate:**

- Scope the **file system itself to a prefix** rather than a whole bucket. AWS
  explicitly recommends this: "clients can only access data within that prefix."
- Scope the compute role's inline policy to bucket **plus prefix**, not `bucket/*`.
- One file system per data domain, with a matching narrowly-scoped role, so the blast
  radius is a domain rather than a bucket.
- Use file system policies with the `s3files:AccessPointArn` condition key to require
  mounting through a specific access point.
- Consider per-execution scoping via session policies if static prefix scoping proves
  too coarse — but note our job roles are currently static, so this is a real design
  change, not a config tweak.

**Spike deliverable:** a concrete permissions model that preserves current isolation
guarantees, with the cost consequence of the chosen point on that dial quantified.

---

## 6. Open question B — cross-account inputs (**answered: not viable**)

**Question:** for S3 paths in another account that we can already read today, do we
need extra parameters or permissions to attach them as an S3 Files volume?

**Answer: you cannot create an S3 Files file system over a bucket you do not own.**
The file system is a sub-resource of the bucket and lives in the bucket owner's
account. The supported cross-account model is the inverse of what we want:

> Account A owns the S3 file system; Account B owns the compute that connects to it.

To use a collaborator's bucket, **they** would have to:

- Enable **S3 Versioning** on the bucket (a hard S3 Files prerequisite)
- Use SSE-S3 or SSE-KMS encryption
- Create the file system and mount targets in their VPC
- Add a file system policy granting our Batch job role `s3files:ClientMount`
- Permit the EventBridge sync rules S3 Files creates

And **we** would need:

- VPC peering (or TGW) with **non-overlapping CIDRs**
- Route table entries in both accounts
- Security group rules for TCP 2049 in both directions
- **Manually created Route 53 private hosted zones per AZ ID**, with A records
  pointing at mount target IPs (`{az_id}.{fs_id}.s3files.{region}.on.aws`)
- Compute role with S3 Files client permissions plus inline `s3:GetObject` on
  Account A's bucket

That is a per-collaborator infrastructure project, not a parameter.

### Consequence for the design

**Foreign-bucket inputs must stay on the existing copy path, permanently.** The system
therefore needs **both** paths — mount and copy — chosen per input.

This also means **OCSDV-452's include/exclude filtering is not obsoleted** by S3 Files.
Filtering remains the mechanism for reducing transfer cost on the copy path, which
foreign-bucket inputs will always use.

---

## 7. Design sketch (starting point, not a decision)

The natural insertion point is `DemandExecutionContextManager`. Today
`pre_execution_data_sync_requests` produces `list[PrepareBatchDataSyncRequest]`,
consumed by the "Transfer Inputs TO Batch Job" Map state in the SFN fragment.

For an input marked **mount**:

- Emit **no** data sync request.
- Add an S3 Files volume + mount point to the `BatchJobBuilder`, alongside the existing
  `BatchEFSConfiguration`-derived volumes. An `S3FilesConfiguration` analog producing
  `mount_point` / `volume` would mirror the existing pattern closely.
- Set the job param's local path to `{mount_path}/{relative_key}` instead of the
  sha256 shared-cache path or the working-dir path.
- Mount **read-only** (`readOnly: true` on the mount point, plus
  `AmazonS3FilesClientReadOnlyAccess` rather than full access).
- Skip the input in `post_execution_remove_data_paths_requests` — nothing was copied,
  so there is nothing to clean up.
- `isolate_inputs` becomes irrelevant for mounted inputs; they are read-only and
  shared by nature.

**Where is mount-vs-copy expressed?** This interlocks with OCSDV-453, which is already
adding fields to `ResolvableBase`. A `mode: copy | mount` field on the same model is
the obvious candidate — but note the serialization trap documented in OCSDV-453:
`sanitize_serialized_params` collapses a `Resolvable` to its `"remote @ local"` string
form, so any new field needs the same conditional-dict treatment, and
`update_demand_execution_parameter_inputs` rebuilds `Resolvable` from scratch and will
drop it otherwise.

**Selection rule** should probably be automatic rather than caller-specified: mount if
the bucket is ours and has a file system; copy otherwise. Caller override is a
secondary concern.

---

## 8. Known hazards to verify during the spike

- **`PathLock` will not work.** Advisory locks (`flock`, `fcntl`) do not work across
  clients on S3 Files. We use `PathLock` with `require_lock=True` on the input sync
  path. Mounted inputs are read-only so the lock may simply be unnecessary — confirm.
- **POSIX gaps:** no hard links; `rename()` on directories is not atomic across the
  NFS boundary; Unix sockets and named pipes unsupported; file-based databases
  (SQLite, LevelDB, RocksDB) corrupt under concurrent access. Audit what the science
  tools actually do — some genomics tooling uses file locking and index sidecar files.
- **Bucket prerequisites:** S3 Versioning **required**; SSE-S3 or SSE-KMS **required**.
  Check our input buckets. Enabling versioning on a large existing bucket has storage
  cost implications.
- **Client version:** `amazon-efs-utils` >= 3.0.0 on the host for direct EC2 mounts.
  For ECS/EKS the agent handles mounting, but confirm for the EKS path.
- **Availability zone affinity:** mount targets are per-AZ. Confirm behavior when a
  Batch job lands in an AZ without a mount target.

---

## 9. Spike deliverables / exit criteria

1. **Go/no-go on the compute platform** — Batch-on-EKS viable, or wait for ECS-EC2?
   Include a TAM answer on ECS-EC2 timing if obtainable.
2. **Measured throughput** for a representative BAM set versus the current EFS
   baseline, with confirmation that the >= 1 MiB free-read path actually triggers
   (validate via cost/usage data, not just docs).
3. **Confirmed pricing** from the official AWS pricing page.
4. **A permissions model** that preserves current isolation, with the cost of the
   chosen point on the direct-grant dial quantified.
5. **Hazard audit results** — especially `PathLock` and the POSIX gaps against real
   tooling.
6. **A recommendation**: adopt / adopt-with-constraints / defer, with the specific
   trigger that would change a "defer".

**Non-goals:** implementing the context manager change; touching outputs; migrating
off EFS for shared/cache scenarios; any change to the OCSDV-452/453 plan.

---

## 10. Sources

- [Amazon S3 Files volumes — AWS Batch](https://docs.aws.amazon.com/batch/latest/userguide/s3files-volumes.html)
- [Prerequisites for S3 Files](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-prereq-policies.html)
- [How S3 Files works with IAM](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-security-iam.html)
- [Tutorial: Mount an S3 file system across accounts](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-cross-account.html)
- [Launching S3 Files — AWS News Blog](https://aws.amazon.com/blogs/aws/launching-s3-files-making-s3-buckets-accessible-as-file-systems)
- [S3 Files best practices](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-best-practices.html)
