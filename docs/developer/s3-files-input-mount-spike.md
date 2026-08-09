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

---
---

# Findings

**Investigated:** 2026-08-09
**Method:** AWS documentation (fetched live on the date above) plus a read of
`aibs-informatics-aws-lambda`, `aibs-informatics-cdk-lib`, and `aibs-informatics-core`
at the commits noted below. No live AWS test was performed; every claim that would
require one is called out explicitly in section F6.

**Code state at time of investigation:** aws-lambda `main` @ `90ab548`, cdk-lib
`feature/ecr-mirroring` @ HEAD, aibs-informatics-core `main`.

**Bottom line:** **Defer.** The cost and throughput case is real and better than the
brief estimated, and the permissions problem is more tractable than the brief
estimated. But the compute-platform blocker is unchanged, and the only open door —
Batch-on-EKS — costs more than the problem it solves. See section F5.

---

## F1. Blocker zero — compute platform (exit criterion 1)

### The EC2 restriction is still live. The brief is not stale.

Both AWS pages still carry the restriction verbatim as of 2026-08-09:

> S3 Files are not supported on the Amazon EC2 launch type at this time. If you
> configure an S3 file system in a job definition and attempt to run it on the Amazon
> EC2 launch type, the job will fail at launch. Amazon EC2 launch type support is
> planned for a future release.

— identical text on both [AWS Batch S3 Files volumes][b-s3f] and
[Configuring S3 Files for Amazon ECS][e-s3f]. No what's-new announcement, release
note, or doc revision changing this was found. **No published date for EC2 support.**

### New information the brief did not have: ECS Managed Instances

The ECS page lists availability by launch type, and there are **three** supported
targets, not two:

> S3 Files support in Amazon ECS is available for the following launch types at
> General Availability:
> + **Fargate** — Fully supported.
> + **Amazon ECS Managed Instances** — Fully supported.

ECS Managed Instances is EC2-backed capacity with AWS-managed instances — on its face
exactly the escape hatch we want. **It does not help us.** AWS Batch compute
environments accept only `EC2 | SPOT | FARGATE | FARGATE_SPOT` for ECS orchestration,
plus EKS. There is no Batch compute environment type that targets ECS Managed
Instances. Confirmed against [CreateComputeEnvironment][b-cce]; no Batch
documentation or announcement references Managed Instances.

This is worth re-checking periodically: if Batch ever adds a Managed Instances
compute environment type, it would unblock S3 Files **without** an EKS migration.
That is a specific, plausible trigger — see section F5.

### Fargate: the brief's ceiling is stale, but the verdict is right

The brief says 16 vCPU / 120 GB. Current limit is **32 vCPU / 244 GB**
([Batch job definition parameters][b-jdp]: "The supported values are 0.25, 0.5, 1, 2,
4, 8, 16, and 32"; [ECS task CPU/memory table][e-cpu]: 32768 (32 vCPU) → "60 GB,
120 GB, 244 GB").

The corrected number does not change the answer. Fargate is ruled out by our own
code, three independent ways:

| Fargate constraint | Our code | Verdict |
|---|---|---|
| Compute environment must be Fargate | Every config in `constructs_/batch/defaults.py:13-73` and `constructs_/service/compute.py:250,262,309,326,338,351` is `use_fargate=False`. `BatchCompute` does build one `use_fargate=True` environment (`compute.py:272-280`), but `fargate_batch_environment` is assigned and never referenced again anywhere in `src/`, and `primary_batch_environment` returns the **on-demand** environment | No Fargate demand path is wired |
| Max 32 vCPU / 244 GB | `constructs_/batch/instance_types.py` provisions `m7a.48xlarge`, `m7a.metal-48xl`, `m6a.metal` (192 vCPU) and `r7a.32xlarge`, `r6a.32xlarge`, `r6i.metal` (~1 TiB RAM) | Platform is provisioned 4–6× past the ceiling |
| `privileged` must be false or absent | `handlers/demand/context_manager.py:820` sets **`privileged=True`** unconditionally (with a `# TODO: need to make this configurable`) | Hard reject at job registration |

Additionally Fargate supports no GPU (`resourceRequirements` type GPU is rejected)
while `resource_requirements.gpu` is a live field at `context_manager.py:814`, and
`inf2.8xlarge` (Inferentia) appears in both the on-demand and spot instance lists.

**Fargate is not a viable target and will not become one by AWS raising limits.**

### Batch-on-EKS: technically viable, organizationally expensive

The technical shape works, and better than the brief assumed:

- **Supported today.** Batch-on-EKS takes S3 Files via `persistentVolumeClaim` in
  `eksProperties.podProperties.volumes` ([Batch S3 Files volumes][b-s3f]).
- **EC2 On-Demand and Spot both supported.** "AWS Batch on Amazon EKS supports Amazon
  EC2 instances (On-Demand and Spot) as compute resources" ([EKS compute
  environments][b-eks]). Arbitrary instance types and GPU are available.
- **`privileged` is available.** [`EksContainerSecurityContext`][b-esc] exposes
  `privileged`, so our `privileged=True` requirement survives the move.
- **No in-cluster Batch components.** "there are no Kubernetes components (for
  example, Operators or Custom Resources) to install or manage in your cluster." Batch
  manages node lifecycle, scale-to-zero, labels and taints.
- **Mounting is a cluster add-on, not per-job.** The [EFS CSI driver serves both EFS
  and S3 Files][s-eks] ("install the efs-csi-driver, which is the CSI driver for both
  Amazon EFS and S3 Files"), with the `AmazonS3FilesCSIDriverPolicy` managed policy,
  and supports **both dynamic and static provisioning**.

The cost is that we would be standing up a Kubernetes platform from zero:

```
$ grep -rni "eks|kubernetes" src/   # aibs-informatics-aws-lambda  → 0 hits
$ grep -rni "eks|kubernetes" src/   # aibs-informatics-cdk-lib     → 0 hits
```

**There is no EKS anywhere in either repo.** Adopting Batch-on-EKS means: an EKS
cluster and its CDK surface; managed node groups; cluster RBAC / access entries; the
EFS CSI driver add-on and its IAM (Pod Identity or IRSA + OIDC provider); PV/PVC
lifecycle management; and a permanent Kubernetes version-upgrade obligation (AWS
documents [supported Kubernetes versions][b-eks] and a separate procedure for
updating a compute environment's Kubernetes version). It also splits our Batch
platform in two — the demand path on EKS, the data-sync and lambda-batch paths on
ECS — or forces migrating all of them.

One advantage claimed in section 4 partly evaporates on this path: "no launch
template" is true, but EKS substitutes a **pre-created PersistentVolume and
PersistentVolumeClaim per file system**, which is the same class of static,
out-of-band asset that made FSx for Lustre and Mountpoint unattractive. It is a
better static asset (one per file system, not one per compute environment, and
dynamic provisioning may remove even that) but it is not zero.

**Answer to spike question 0:** Batch-on-EKS is *viable* but is not an acceptable
cost for this benefit alone. It would be a reasonable target if EKS were already on
the roadmap for other reasons; it is not justified by the input-mount use case by
itself.

---

## F2. Pricing (exit criterion 3)

### The load-bearing claim is confirmed, and it is AWS's own

Verbatim from the [official S3 pricing page][aws-pricing]:

> For large file reads of 1 MiB or larger, data is streamed directly from your S3
> bucket even if it resides on the file system's high-performance storage, since S3 is
> optimized for high throughput while the file system's high-performance storage is
> optimized for low-latency small-file access. **These reads incur only S3 GET request
> charges with no S3 Files data access charges.**

> Reads of data not on the file system's high-performance storage are served directly
> from your S3 bucket at S3 GET request rates with no S3 Files data access charges.

AWS's own worked example bills the large-read portion at **$0.0 FREE**. Section 3's
central mechanism is real and no longer rests on third-party analysis.

### Confirmed rates, and two corrections to section 3's table

AWS's worked example (verbatim, [S3 pricing page][aws-pricing]) — 100 GB bucket,
10 GB read of which 94% large / 6% small, 1 GB written:

```
S3 Files High-performance storage = 0.60 GB small data + 0.25 GB writes = 0.85 GB x $0.30 = $0.255
S3 Files storage writes                = 1 GB    x $0.06 = $0.06
S3 Files storage writes sync           = 0.25 GB x $0.03 = $0.0075
S3 Files small file reads from file system = 0.60 GB x $0.03 = $0.018
S3 Files small file reads sync             = 0.60 GB x $0.06 = $0.036
S3 Files reads directly from S3 bucket = 10 GB x (1 - 6%) x $0.0 FREE
S3 Files Monthly total = $0.3765
```

**Correction 1 — the brief omits the high-performance tier storage charge.**
Section 3 lists S3 Files storage as "$0.023/GB-mo (underlying S3)". That is the
ordinary S3 storage we already pay for the source data. There is a *separate*
**$0.30/GB-month** charge on data resident in the high-performance tier — the same
rate as EFS Standard storage. Section 3's table reads as though S3 Files storage is
13× cheaper than EFS; for any data that lands in the cache tier it is **identical**.

**Correction 2 — the threshold is on read size, not file size.** Section 3 says
"files >= 1 MiB read sequentially". The [performance specification][s-perf] says
"for large reads >= 1 MiB" — the I/O request, not the file. This matters because
indexed BAM access via a `.bai` sidecar is *random* access, not sequential streaming:
a tool seeking to one region of a 30 GB BAM may issue 64 KiB reads, which would not
clear a 1 MiB read-size bar.

**Both corrections land in our favour anyway**, via a mechanism the brief did not
identify. From [best practices][s-bp]:

> By default, S3 Files caches data for files smaller than 128 KB when you first access
> a directory. **Files larger than this threshold are read directly from S3.**

and from [performance][s-perf], S3 Files streams directly from S3 "when the file's
data is not stored in the file system's high-performance storage" — a condition
independent of read size. So for our workload:

- BAMs are far larger than the 128 KiB import threshold → never auto-imported into the
  high-performance tier → **no $0.30/GB-mo cache storage charge**, and
- reads of data not in the cache tier stream from S3 → **no data access charge**,
  *regardless of read size*, so the random-access concern is neutralised.

Also confirmed: the mount helper's **default NFS read/write buffer is 1 MB**
("When using the mount helper, the default NFS read and write buffer sizes are set to
1 MB for optimal performance"), so sequential streaming clears the 1 MiB bar on its
own merits. And S3 Files "meters each read and write operation at a minimum of 32 KB",
which is the penalty for small-IO workloads.

One residual caveat: [troubleshooting][s-ts] notes reads are served directly from S3
only "when the file has not been modified through the file system." For a **read-only
input mount** nothing is ever modified through the file system, so this condition
holds permanently. This is a further argument for the section 2 input-only scope.

### What this actually costs us, versus what we do today

The brief compares against **EFS Elastic**, which we do not use.
`constructs_/efs/file_system.py:235` hardcodes `throughput_mode=efs.ThroughputMode.BURSTING`
(and the constructor default at line 64 is also `ThroughputMode.BURSTING`). Under
bursting, throughput is *free* — that is exactly why we have a burst-credit
reliability problem rather than a bill. The honest comparison per TB of input:

| | Today (EFS bursting + copy) | S3 Files input mount |
|---|---|---|
| Throughput charge | $0 (but credit-limited — the actual problem) | $0 |
| Storage | $0.30/GB-mo on EFS, prorated for residency (~$2.50/TB for a 6-hour execution) | $0 — nothing is resident |
| Read charge | $0 | $0 (large-file / uncached path) |
| S3 GET requests | ~$0.42/TB (copy reads the same bytes) | ~$0.42/TB (est., 1 MiB reads @ $0.0004/1k) |
| Transfer-stage compute | Real — the "Transfer Inputs TO Batch Job" Map state and its instances | **Eliminated** |

**Conclusion:** the input mount is modestly cheaper than today and meaningfully
simpler (it deletes the input transfer stage), but section 3 overstates the savings
by benchmarking against an EFS mode we do not run. **The real case for S3 Files is
throughput, not cost.** On that axis it is decisive: [performance
specifications][s-perf] give **3 GiB/s per client**, **250,000 read IOPS per file
system**, and "up to terabytes per second" aggregate, with **no burst credit concept
at all**. That is a categorical fix for the problem in section 1, where the multi-EFS
work in aws-lambda#39 / cdk-lib#61 is a multiplier on a near-zero baseline.

---

## F3. Permissions model (exit criterion 4)

### The direct-grant dial is real — confirmed, with a caveat about where AWS says it

Section 5's lever quote is accurate, but it does **not** appear on the prerequisites
or IAM pages. It is on the [troubleshooting page][s-ts], under "Intelligent read
routing is not working":

> **Missing S3 inline policy on compute role** – The IAM role attached to your compute
> resource must include an inline policy granting `s3:GetObject` and
> `s3:GetObjectVersion` on the linked S3 bucket. **Without this policy, the mount
> helper cannot read directly from S3 and all reads go through the file system.**

Meanwhile the [prerequisites page][s-prereq] is prescriptive — "Add the following two
policies to the IAM role" — and the [IAM page][s-iam] says "you **must** add an inline
policy". The reconciliation: it is *functionally* optional (reads still succeed, via
the file system) and *practically* mandatory for the free-read path. The dial exists.
Section 5's framing is correct.

### Correction: `s3files:ClientWrite` is NOT required for read-only

The [Batch S3 Files page][b-s3f] states:

> The job role (equivalent to the Amazon ECS task role) must have `s3files:ClientMount`
> and `s3files:ClientWrite` permissions on the file system.

The [S3 prerequisites page][s-prereq] directly contradicts this:

> You can also provide these permissions by adding individual IAM permissions such as
> `s3files:ClientMount` or `s3files:ClientWrite` **(not required for read-only
> connections)** to the IAM role of your compute resource.

and the [IAM action table][s-iam] defines `s3files:ClientMount` as "Provides read-only
access to a file system." The S3 User Guide is the service-owning documentation and
is almost certainly correct; the Batch page appears to be describing the read-write
case loosely. **This is a documentation conflict that needs a TAM answer or a live
test** (see F6) — but it is not load-bearing for our design, since a read-only mount
should need only `ClientMount`.

### Correction: our job role is already per-execution, not static

Section 5 says "our job roles are currently static, so this is a real design change,
not a config tweak." **That is not accurate.** The job role is already a
caller-supplied, per-execution field:

- `aibs_informatics_core/models/demand_execution/platform.py:7` —
  `job_role: IAMRoleArn | str | None` on the Batch execution platform
- `handlers/demand/context_manager.py:823` —
  `job_role_arn=demand_execution.execution_platform.aws_batch.job_role`
- → `handlers/demand/scaffolding.py:137` → `handlers/batch/create.py:123`

A per-data-domain job role can therefore be selected **per execution today**, with no
model change and no new plumbing. This materially improves the isolation story: the
"one file system per data domain, with a matching narrowly-scoped role" mitigation in
section 5 is available immediately rather than being a design change.

### Recommended model

The controlling insight: **the inline S3 grant is enforced at the S3 API layer, where
access points do not apply.** An access point constrains the NFS view; it does nothing
to a `GetObject` call. So the real blast radius of a mounted input is *the scope of
the inline grant*, and the design job is to make that scope small and meaningful
rather than to try to claw privilege back with access points.

Four controls, in order of effectiveness:

1. **Scope the file system to a prefix, not the bucket.** AWS recommends this
   explicitly for exactly this reason ([best practices][s-bp]): "consider creating
   your file system scoped to a specific prefix rather than the entire bucket, so that
   clients can only access data within that prefix." It also independently helps
   throughput ("create multiple file systems scoped to different specific prefixes
   within the same bucket … to scale horizontally") and limits rename blast radius.
2. **Scope the inline grant to the same prefix.** The prerequisites page's own policy
   template invites this: "Replace {{bucket}} with your S3 bucket name **or bucket
   name with prefix**." Set the grant's `Resource` to `arn:aws:s3:::{bucket}/{prefix}/*`
   and the `ListBucket` statement to the bucket with a matching
   `s3:prefix` condition.
3. **One file system + one job role per data domain**, selected per execution via the
   existing `execution_platform.aws_batch.job_role` field. Blast radius becomes a data
   domain rather than a bucket — and, per the point above, this is available today.
4. **Access point + file system policy**, for POSIX/NFS-layer defence in depth. The
   `s3files:AccessPointArn` condition key exists and works as section 5 describes
   ([IAM page][s-iam] carries a working example). Grant `s3files:ClientMount` only —
   omit `ClientWrite` and `ClientRootAccess`. Note the read-only file system policy
   example in the AWS docs grants `ClientMount` alone, corroborating F3's correction
   above.

Net effect: if the file system prefix, the inline grant prefix, and the access point
root directory are all aligned to the same data domain, then **the direct S3 grant
adds no privilege beyond what the mount already exposes** — because everything inside
the file system's prefix is already readable through the mount by design. The
isolation loss the brief worried about only materialises when the grant is broader
than the file system, which is a configuration choice we control.

### Cost of the isolation choice, quantified

If a data domain cannot be expressed as a prefix and we must drop the direct grant to
preserve isolation, reads route through the file system instead. Extrapolating the
line-item rates from AWS's worked example (small-file reads from the file system at
$0.03/GB plus read sync at $0.06/GB), routing 1 TB of input through the file system
costs on the order of **$90/TB**, plus $0.30/GB-month on whatever remains resident in
the cache tier.

That is squarely in the range the brief quotes for EFS Elastic ($90–150/TB) — i.e.
**giving up the direct grant gives up the entire cost advantage** and leaves only the
throughput advantage. This figure is an extrapolation from AWS's example rather than a
quoted rate for large reads routed through the file system; treat it as
order-of-magnitude and confirm by test before relying on it.

---

## F4. Hazard audit (exit criterion 5)

| # | Brief's claim | Finding | Status |
|---|---|---|---|
| 1 | `PathLock` will not work; advisory locks do not work across clients | **Contradicted by AWS docs** — see below | ✅ Not a hazard |
| 2 | No hard links | Confirmed | ⚠️ Real, not applicable |
| 3 | Directory `rename()` not atomic | Confirmed, and worse than stated | ⚠️ Real, outputs only |
| 4 | Unix sockets / named pipes unsupported | **Not supported by the cited docs** | ❓ Unverified |
| 5 | SQLite / LevelDB / RocksDB corrupt | Rests on #1; likely overstated | ❓ Needs tool audit |
| 6 | S3 Versioning required | Confirmed — **and we do not have it** | 🔴 Real blocker |
| 7 | SSE-S3 / SSE-KMS required | Confirmed; likely already satisfied | ✅ Low risk |
| 8 | `amazon-efs-utils` >= 3.0.0 | Confirmed | ✅ N/A on EKS |
| 9 | AZ affinity | Confirmed; straightforward mitigation | ✅ Manageable |
| — | *(not in brief)* Archival storage classes unreadable | New hazard | ⚠️ Needs bucket audit |
| — | *(not in brief)* `efs-utils` logs S3 key names | New, minor | ℹ️ Note |

**1 — `PathLock`: the brief's headline hazard appears to be wrong.**
The [quotas page][s-quotas] lists, under NFS features *not* supported, "**Mandatory
locking (all locks are advisory)**" — i.e. advisory locking is the supported mode, not
the broken one. It then publishes explicit cross-client lock quotas:

| Resource | Quota |
|---|---|
| Maximum locks per file | 512 **across all connected instances** |
| Maximum locks per mount | 8,192 across up to 256 file-process pairs |

A per-file lock quota counted "across all connected instances" only makes sense if
locks are coordinated server-side across clients. The [troubleshooting page][s-ts]
agrees: S3 Files routes reads intelligently "while maintaining full file system
semantics including consistency, **locking**, and POSIX permissions." The brief's
claim traces to third-party writing about *Lambda*, not to AWS documentation about
NFS mounts.

`PathLock` (`aibs_informatics_core/utils/file_operations.py:444-518`) uses
`fcntl.flock` on a sidecar `.lock` file, which is exactly advisory locking. It should
work. **It is moot regardless**: the design in section 7 emits no data sync request
for mounted inputs, so the `require_lock=True` path at
`handlers/demand/context_manager.py:360` is never reached for them. Two independent
reasons this is not a blocker. *(Worth a live test if we ever want locks on a mount:
`PathLock` writes a lock file, which a read-only mount would reject and which would
otherwise sync to S3 as an object.)*

**3 — rename is worse than "not atomic".** Per [performance][s-perf]: "when you rename
or move a directory containing tens of millions of files, your S3 request costs and
the synchronization time increase significantly. A directory rename of 100,000 files
takes a few minutes to fully reflect in the S3 bucket, though the rename is instant on
the file system." This is an outputs-path hazard and **reinforces the section 2
decision** to keep outputs on the copy path.

**4 and 5 — not supported by the documentation.** The [quotas page][s-quotas] lists
under unsupported NFS features "Block devices, character devices, attribute
directories, and named attributes" — it does **not** mention Unix sockets or FIFOs,
and says nothing about file-based databases. The brief's claims here appear to come
from third-party analysis. They may still be true; they are not documented. Since the
input mount is read-only, neither matters for this design — but do not carry these
assertions forward as fact. Additional documented limits worth knowing: no path
component over 255 bytes, no key over 1,024 bytes, no keys with `//`, `/./`, `/../`,
or null bytes (such objects are simply **not imported** and would silently go missing
from the file system view), and no `nconnect` mount option.

**6 — the one genuinely blocking prerequisite. Our buckets are not versioned.**
"Your S3 bucket has versioning enabled. S3 Files **requires** S3 Versioning"
([prerequisites][s-prereq]). Searching cdk-lib:

```
$ grep -rn "versioned" src/    # aibs-informatics-cdk-lib → 0 hits
```

`EnvBaseBucket` (`constructs_/s3/bucket.py:13-50`) never passes `versioned`, so it
takes the CDK default of `false`, and neither of its two instantiations
(`constructs_/service/storage.py:24`, `aibs_informatics_core_app/stacks/core.py:22`)
overrides it. Enabling versioning on existing multi-TB data buckets is a real project
with ongoing storage cost implications (every overwrite retains a prior version, and
lifecycle rules to expire noncurrent versions must be designed). **This is a
prerequisite in its own right, independent of the compute-platform blocker.**

**7 — encryption is likely already satisfied.** S3 has applied SSE-S3 by default to
all new buckets since January 2023, and our buckets set no conflicting encryption
configuration (`encryption` appears nowhere in cdk-lib's S3 constructs). Verify on the
real buckets rather than assuming.

**9 — AZ affinity is manageable.** Failure mode is explicit: "'Failed to resolve file
system DNS name' – There is no mount target in the Availability Zone where your EC2
instance is running" ([troubleshooting][s-ts]). Mitigation is a documented best
practice — "Create a mount target in every Availability Zone" — and the console does
it automatically. Note the hard quota of **1 mount target per AZ** and **1 VPC per
file system** ([quotas][s-quotas]).

**New hazard — archival storage classes are invisible to the file system.** Objects in
S3 Glacier Flexible Retrieval, Glacier Deep Archive, or the Intelligent-Tiering
Archive Access / Deep Archive Access tiers "cannot be accessed through the file
system. You must first restore these objects using the S3 API" ([quotas][s-quotas]).
cdk-lib has a lifecycle rule generator that transitions to
`s3.StorageClass.INTELLIGENT_TIERING` (`constructs_/s3/lifecycle_rules.py:52`). A
plain Intelligent-Tiering transition does not by itself enable the archive tiers, so
this is probably fine — but **the actual input buckets need auditing** for archive
tiering and Glacier lifecycle rules before any mount is attempted. Under the current
copy path such objects fail loudly at copy time; under a mount they would appear
simply absent.

**New note — key names leak into host logs.** "`efs-utils` writes S3 object key names
directly in logs which it stores in the directory `/var/log/amazon/efs`"
([best practices][s-bp]). If our S3 key names encode anything sensitive (subject IDs,
specimen identifiers), this is a disclosure surface on the host. AWS suggests
`chmod 700` on that directory.

---

## F5. Recommendation (exit criterion 6)

### **Defer.**

Not "no" — the underlying technology is a good fit and the analysis mostly came out
*better* than the brief expected:

- The free-read mechanism is confirmed, from AWS, and applies to our workload by two
  independent mechanisms (files > 128 KiB are never cached; uncached data streams
  from S3 free at any read size).
- Throughput is a categorical fix, not a multiplier: 3 GiB/s per client and 250k IOPS
  with no burst credits, against a bursting baseline of 50 KiB/s per GiB stored.
- The sandbox problem is smaller than feared. Prefix-scoped file systems plus
  prefix-scoped grants plus per-domain job roles — the last of which **works today**
  with no model change — reduce the direct grant to something that adds no privilege
  beyond the mount itself.
- The scariest hazard (`PathLock`) is contradicted by AWS's own lock quotas, and is
  moot for mounted inputs anyway.

We defer because the **only** path to running this on our compute is a Kubernetes
platform migration, and the benefit does not pay for it. Standing up EKS — cluster,
node groups, RBAC, CSI driver and its IAM, PV/PVC lifecycle, version-upgrade
treadmill, and a split Batch platform — to fix input throughput is disproportionate
when aws-lambda#39 / cdk-lib#61 already address the immediate pain, and when the
prerequisite work (S3 Versioning on the data buckets) is unfinished regardless.

### Triggers that would change this to adopt-with-constraints

Any **one** of these flips the recommendation. In rough order of likelihood:

1. **AWS ships ECS-on-EC2 support for S3 Files.** The direct unblock. Job definitions
   gain a `s3filesVolumeConfiguration` volume alongside the existing EFS volumes, the
   `BatchEFSConfiguration` pattern gains an `S3FilesConfiguration` sibling, and the
   Step Functions layer is untouched. This is the trigger to watch. *Action: open a
   TAM/support thread for timing — see F6.*
2. **AWS Batch adds an ECS Managed Instances compute environment type.** Managed
   Instances already supports S3 Files at the ECS layer; only Batch's compute
   environment enumeration blocks it. This would unblock us without EKS and is
   plausible given AWS's direction. Not currently announced.
3. **EKS arrives for independent reasons.** If the organisation adopts EKS on its own
   merits, the marginal cost of this work drops to near zero and it becomes clearly
   worth doing.
4. **Multi-EFS proves insufficient in production.** If aws-lambda#39 / cdk-lib#61
   ship and executions still exhaust burst credits, the calculus changes: the
   alternative is then EFS Elastic at $90–150/TB, against which an EKS migration
   starts to look like the cheaper answer. *This is the trigger that would justify
   paying the EKS cost.*

### Work worth doing now, regardless

None of this depends on the blocker resolving, and all of it shortens the path later:

1. **Enable S3 Versioning on the input data buckets**, with a noncurrent-version
   expiration lifecycle rule. Hard prerequisite, long lead time, independently
   defensible for data protection.
2. **Audit input buckets for archival storage classes** — Glacier lifecycle rules and
   Intelligent-Tiering archive tiers. Cheap, and objects in those classes would
   silently vanish from a mounted view.
3. **Confirm SSE-S3/SSE-KMS** on the same buckets. Almost certainly already true.
4. **Watch the two AWS surfaces** in triggers 1 and 2 — the Batch S3 Files page and
   Batch compute environment types — on a periodic basis.

Explicitly **not** recommended: any change to the OCSDV-452 / OCSDV-453 plan. Section
6's conclusion stands — foreign-bucket inputs must stay on the copy path permanently,
so OCSDV-452's include/exclude filtering keeps its full value, and this deferral only
strengthens that.

---

## F6. Open items — not determinable from docs or code

Flagged rather than guessed. Each needs a TAM answer or a live test.

**Needs a TAM / AWS Support answer:**

1. **ECS-on-EC2 launch type support for S3 Files — timing.** The single most
   decision-relevant unknown; "planned for a future release" with no date. If it is
   near, waiting clearly beats migrating. *(Carried forward from section 4 — still
   open, not attempted here.)*
2. **Is an ECS Managed Instances compute environment type planned for AWS Batch?**
   Would unblock without EKS. No public signal either way.
3. **Documentation conflict on `s3files:ClientWrite`.** The Batch page says the job
   role must have `ClientMount` *and* `ClientWrite`; the S3 User Guide says
   `ClientWrite` is "not required for read-only connections". Which governs a Batch
   job with a read-only mount?

**Needs a live test:**

4. **Do large-file reads elevate data into the high-performance tier?** The docs say
   files over the 128 KiB import threshold are read directly from S3, and AWS's
   pricing example bills no cache storage for the 94% large-read portion — but no page
   states outright that a large read never populates the cache. This is the difference
   between $0 and $0.30/GB-month on the full working set. **Validate via Cost and
   Usage Report line items, not just docs**, as section 9.2 requires.
5. **Measured throughput for a representative BAM set versus the EFS baseline.**
   Exit criterion 2 remains entirely unaddressed — it needs a deployed file system,
   which needs the blocker resolved. Deferred with the recommendation.
6. **Does the free path hold for indexed/random BAM access?** Sequential streaming
   clears the 1 MiB bar via the mount helper's 1 MB default buffer. Random `.bai`-driven
   access should also be free (uncached data streams from S3 at any read size), but
   this is inference from two separate doc statements, not a documented guarantee.
   Worth measuring alongside item 4.
7. **Extrapolated $90/TB cost of dropping the direct grant.** Derived from AWS's
   worked-example line items, not from a quoted rate for large reads routed through the
   file system. Confirm before using it to justify a design decision.
8. **Do the science tools use Unix sockets, FIFOs, or embedded databases on the input
   path?** The brief asserts these break; AWS documents neither way. Read-only inputs
   make it moot for this design, but the assertion should not be carried forward as
   fact without a tooling audit.

---

## F7. Additional sources consulted

- [Configuring S3 Files for Amazon ECS][e-s3f]
- [Amazon EKS compute environments — AWS Batch][b-eks]
- [EksContainerSecurityContext — AWS Batch API Reference][b-esc]
- [Job definitions on Fargate — AWS Batch](https://docs.aws.amazon.com/batch/latest/userguide/fargate-job-definitions.html)
- [Job definition parameters — AWS Batch][b-jdp]
- [Troubleshoot Amazon ECS task definition invalid CPU or memory errors][e-cpu]
- [CreateComputeEnvironment — AWS Batch API Reference][b-cce]
- [Mounting S3 file systems on Amazon EKS][s-eks]
- [Performance specifications — S3 Files][s-perf]
- [Unsupported features, limits, and quotas — S3 Files][s-quotas]
- [Troubleshooting S3 Files][s-ts]
- [Amazon S3 pricing][aws-pricing]

[b-s3f]: https://docs.aws.amazon.com/batch/latest/userguide/s3files-volumes.html
[b-eks]: https://docs.aws.amazon.com/batch/latest/userguide/eks.html
[b-esc]: https://docs.aws.amazon.com/batch/latest/APIReference/API_EksContainerSecurityContext.html
[b-jdp]: https://docs.aws.amazon.com/batch/latest/userguide/job_definition_parameters.html
[b-cce]: https://docs.aws.amazon.com/batch/latest/APIReference/API_CreateComputeEnvironment.html
[e-s3f]: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/s3files-volumes.html
[e-cpu]: https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-cpu-memory-error.html
[s-prereq]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-prereq-policies.html
[s-iam]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-security-iam.html
[s-bp]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-best-practices.html
[s-perf]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-performance.html
[s-quotas]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-quotas.html
[s-ts]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-troubleshooting.html
[s-eks]: https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-files-mounting-eks.html
[aws-pricing]: https://aws.amazon.com/s3/pricing/
