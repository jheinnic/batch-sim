# BSIM-E24 — Scheduler-Managed Job-Provisioned Storage Volumes

BSIM-E18 built a storage cost model (single thin-pool for Batch, generational
thin-pool for K8S/K8S+) but deliberately left storage outside the admission path —
`_batch_fits` / `_k8s_fits` check RAM and vCPU only. `STORAGE_EXHAUSTED` is emitted
when a node's pool hits its `max_ebs_volumes` ceiling, but nothing prevents the job
from being placed there anyway; it has zero scheduling consequence today.

This was a reasonable simplification at the time — E18's own framing deferred the
question of whether the generational mitigation (and storage cost generally) was
"worth the added scheduling complexity" pending an AUC-style finding. That finding
has now landed: across this session's measured runs, storage ran 14.3%–27.5% of
K8S+'s total cost, and in one comparison it ate 96% of K8S+'s compute advantage over
Batch. Storage is not a rounding error in the K8S+ cost story — it is sometimes the
whole story — and the model currently can't express *why* (admission isn't
storage-aware) or test mitigations the way it already does for RAM/CPU admission.

Real Kubernetes does not have this gap: the default scheduler's `NodeVolumeLimits`
predicate (backed by CSI `CSIStorageCapacity` / per-node attach-count tracking)
rejects placement when a node's EBS attachment ceiling would be exceeded, exactly
the way RAM/CPU requests are checked today. This epic closes that gap for
K8S/K8S+, and separately investigates whether AWS Batch — which may not have any
real analog to dynamic CSI-driven volume attachment — can or should receive
equivalent treatment, rather than assuming symmetry either way.

Out of scope: an admission-controller-driven mechanism that dynamically rewrites a
job's volume spec (dedicated → pool-leased) in response to live density pressure.
That's a materially heavier, two-part mechanism (mutating webhook + a
volume-capacity-aware scheduler extender; no off-the-shelf CSI driver does this
natively) with no demonstrated need beyond what BSIM-127/128 already cover.
Candidate future spike, not a story in this epic.

Depends on: BSIM-E18 (BSIM-91–94, storage pool mechanics being extended)

---

## BSIM-126 — Spike: AWS Batch storage/volume-attachment capability assessment

**Type:** Spike | **Priority:** High | **Status:** Done

**Description:**
Research whether AWS Batch (EC2 launch type — what this simulator models, not
Fargate) has any realistic mechanism analogous to Kubernetes' CSI-driven dynamic
volume attachment, or whether storage capacity for Batch-managed instances is
necessarily static — fixed at compute-environment / launch-template definition
time, with no per-job dynamic attach/detach response to live demand.

This determines the disposition of BSIM-129. Do not assume symmetry with K8S+
(Batch may not have an equivalent capability worth modeling) or assume asymmetry
(Batch's looser packing might still hit real EBS ceilings often enough to matter,
or a degenerate form of the same admission check — "don't place a job on a node
already at its static volume ceiling" — may be realistic without requiring any
dynamic-attach capability at all).

**Acceptance Criteria:**
- Written finding citing actual AWS Batch / ECS / EC2 instance storage
  provisioning mechanics (launch template `BlockDeviceMappings`, job-definition
  `mountPoints`/`volumes`, compute environment instance configuration)
- Explicit recommendation: build BSIM-129 as a symmetric admission check, build it
  in a modified/degenerate form, or replace it with a documented-and-justified
  asymmetry
- No code changes — this is a research/documentation deliverable only

**Finding (resolved):**
AWS Batch (EC2 launch type) has no analog to CSI-driven dynamic volume attachment.
EBS volumes are declared once via the launch template's `BlockDeviceMappings` and
attached at instance boot — identical shape for every instance of that type, no
per-job variation. Job-definition `volumes`/`mountPoints` reference storage that
already exists on the host; there is no PVC-equivalent, and nothing in Batch's
control plane calls an attach/detach API in response to a specific job's demand.
EBS Elastic Volumes / Multi-Attach exist at the raw EC2 API level, but Batch never
invokes them automatically — using them would require the same kind of bespoke
controller this epic already excluded as out of scope for K8S+.

This argues *for* BSIM-129, not against it. Karpenter has a partial mitigation
when storage pressure is high — it can select a different instance type with more
volume slots for its *next* node. Batch's compute environment has no equivalent
recourse: once an instance's static volume ceiling is hit, that is a hard wall.
Modeling "don't place a job past that static ceiling" does not require granting
Batch a capability it lacks; it enforces the one constraint Batch is *more*
rigidly subject to than K8S+, which the model currently lets it ignore for free.

**Recommendation:** build BSIM-129 as a symmetric admission check, same mechanics
as BSIM-127.

---

## BSIM-127 — Storage capacity as a real admission constraint for K8S/K8S+

**Type:** Task | **Priority:** High | **Status:** Done
**Depends on:** BSIM-91–94 (E18)

**Description:**
Extend `_k8s_fits` (and the joint-tier provisioner's placement scoring) to reject
a candidate node when the incoming job's `workspace_gb` would exceed that node's
remaining storage ceiling (`max_ebs_volumes` / current pool or generation
capacity) — mirroring real K8s' `NodeVolumeLimits` predicate and the same way
RAM/CPU-unfit nodes are already skipped today.

`STORAGE_EXHAUSTED` changes from a pure observability event to one with real
admission consequence: a node in this state is no longer eligible for new
placements (existing jobs already running on it are unaffected) until enough
jobs complete to free room, or the provisioner scales out.

**Acceptance Criteria:**
- A job that would exceed a node's storage ceiling is never placed on that node;
  placement falls through to the next candidate, or triggers scale-out when no
  node qualifies
- Existing RAM/CPU admission tests pass unchanged
- New test: a storage-exhausted node is correctly skipped in `_best_fit_node` /
  `_k8s_fits` in favor of a node with room
- New test: no node has room → scale-out path triggers (or job queues, matching
  existing RAM/CPU-driven scale-out behavior)
- Storage-driven admission rejection is observable via the existing
  `ADMISSION_REJECTED`-style event machinery (BSIM-104–108), not a new event type

---

## BSIM-128 — Per-job dedicated ephemeral storage volumes (selectable alternative model)

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-127

**Description:**
Add a second storage strategy, selectable via `storage.model: dedicated`
(default remains the current behavior — `generational` for K8S/K8S+, single-pool
for Batch). Under the dedicated model, each job gets its own volume sized to
`workspace_gb`, attached at JOB_START and detached at JOB_COMPLETE/CRASH; node
concurrency is bounded directly by `max_ebs_volumes` with no thin-pool or
generation/overlap bookkeeping required.

This is a genuine alternative to `GenerationalStoragePool`, not just a
simplification of it — many real EBS-backed K8s storage patterns (dynamically
provisioned per-pod PVCs) work exactly this way. Keeping both models selectable
allows head-to-head comparison on the same workload, which is itself useful given
how much this session's findings turned on storage-model mechanics.

**Acceptance Criteria:**
- `storage.model` schema field validates `generational` (default) and `dedicated`
- Dedicated-model nodes never run more concurrent jobs than `max_ebs_volumes`
  permits (enforced via BSIM-127's admission path)
- Cost accrual verified against a hand-computed example
  (Σ job volume-GB × residency time × price)
- Existing generational-model tests pass unchanged (default behavior preserved)
- Same workload + scheduler config runnable under both models for side-by-side
  cost comparison

---

## BSIM-129 — Apply storage admission constraint to Batch

**Type:** Task | **Priority:** Medium | **Status:** Done
**Depends on:** BSIM-126 (resolved — build symmetric check), BSIM-127

**Description:**
Per BSIM-126's finding: Batch has no dynamic volume-attachment capability, but
that argues for this story rather than against it — Batch's static per-instance
volume ceiling is a *harder* constraint than K8S+'s (no Karpenter-style
next-node-type mitigation), and the model currently lets Batch ignore it for
free. Extend `_batch_fits` with the same admission check added in BSIM-127, so
Batch and K8S/K8S+ are compared on a level playing field.

**Acceptance Criteria:**
- A job that would exceed a node's storage ceiling is never placed on that node
  in `_batch_fits` / `_best_fit_node`
- Existing RAM/CPU admission tests pass unchanged
- New test: a storage-exhausted Batch node is skipped in favor of a node with
  room, or triggers a new node launch when no existing node qualifies
- Mirrors BSIM-127's mechanics closely enough to share test patterns/fixtures

---

## Out of scope (candidate future spike)

Admission-controller-driven dynamic volume rewriting: a mutating webhook that
switches a job's volume from dedicated to pool-leased storage in response to live
node density, paired with a volume-capacity-aware scheduler extender (no
off-the-shelf CSI driver supports this natively). Materially heavier than
BSIM-127/128, with no demonstrated need yet. Revisit only if BSIM-127/128's
results show a gap those two don't cover.
