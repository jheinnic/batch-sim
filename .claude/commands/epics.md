# /epics — Structured Epic Brainstorming Session

You are facilitating a focused design session whose terminal output is one or more
new epic files written to `docs/jira/` and an updated `docs/jira/README.md`.

The user has provided a starting topic: **$ARGUMENTS**

---

## Session structure

Work through the following phases conversationally. Ask questions, propose options,
surface tradeoffs. Do not rush to decomposition — understanding the scope and motivation
first produces better stories.

### 1. Framing
- What problem does this solve, or what capability does it add?
- What is the strategic motivation (from the project's migration/optimization goals)?
- What is explicitly out of scope?
- Are there dependencies on existing epics or open tickets? Read `docs/jira/README.md`
  and relevant epic files as needed to establish context.

### 2. Boundary decisions
- How many epics does this split into? (One epic if coherent; split if separable concerns
  have different owners, timelines, or could ship independently.)
- What does "done" look like at the epic level for each?

### 3. Design exploration
- What are the main design options or open questions?
- What are the tradeoffs (cost of correctness, implementation complexity, reversibility)?
- What must be decided now vs. deferred to story-level?

### 4. Story decomposition
For each epic, identify the minimal set of stories that delivers the epic's outcome.
For each story:
- One-line summary (becomes the `## BSIM-N — Title` heading)
- Type: Task | Bug | Spike
- Priority: Highest | High | Medium | Low
- Dependencies on other stories in this session or existing tickets
- Description: what and why (not how, unless the how is the point)
- Acceptance Criteria: concrete, testable, 3–6 bullets

### 5. Number assignment
Before writing files, read `docs/jira/README.md` to find the current highest BSIM
ticket number. Assign sequential numbers starting from N+1.

---

## Terminal output

When the brainstorm reaches a stable point and the user confirms they are ready to
commit the output:

1. **Write one epic file per epic** to `docs/jira/epic_E{N}_{slug}.md`, following
   the format of existing files (epic header, then `## BSIM-N — Title` sections).

2. **Update `docs/jira/README.md`**:
   - Add a row to the Epic Structure table
   - Add a link entry under Files

3. **Confirm** what was written — epic count, ticket range, filenames.

Do not write files until the user signals the session is ready to close out.
Until then, keep the conversation open for refinement.
