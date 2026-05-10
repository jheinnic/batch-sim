# BSIM-E9 — Presentation

---

## BSIM-42 — Presentation narrative and slide outline

**Type:** Task | **Priority:** Medium | **Status:** To Do

**Description:**
Draft the narrative arc and slide-by-slide outline for the non-technical audience
presentation. The presentation must tell a clear story: the problem, the proposed
alternative, the simulation methodology, the results, and the recommendation.

**Slide structure (≤12 slides):**
1. Title: "Is There a Better Way to Run Batch Compute?"
2. The Problem: what AWS Batch costs us and why
3. The Proposed Alternative: OKD/K8S on AWS
4. Why K8S Might Be Cheaper: the key mechanical insight (peak vs soft limits)
5. How We Tested It: the simulation approach (no code required to understand)
6. Our Simulated Workload: four job types, four hours, 242 jobs
7. Result: Cost Comparison (Chart 4 — cost over time)
8. Result: Service Quality (Chart 5 — per-centroid wait times)
9. The Tradeoff Curve (Chart 1 — Pareto frontier)
10. The Safety Net: how K8S handles memory spikes (crash-and-replay)
11. Recommendation: proceed to prototype
12. Appendix: Glossary

**Acceptance Criteria:**
- Outline reviewed and approved before slides built
- Every chart included is from the actual reference run (real data)
- Jargon glossary slide included
- No more than 12 slides

---

## BSIM-43 — Presentation build

**Type:** Task | **Priority:** Medium | **Status:** To Do
**Depends on:** BSIM-42

**Description:**
Build the presentation as a self-contained interactive HTML artifact using real
simulation output charts. Must render in-browser without external dependencies.

**Acceptance Criteria:**
- Presentation renders in browser without dependencies
- All charts embedded (not linked)
- Speaker notes included per slide
- Keyboard navigation (arrow keys) between slides
- Saved to `docs/presentation.html`

---

## BSIM-44 — Executive summary document

**Type:** Task | **Priority:** Low | **Status:** To Do
**Depends on:** BSIM-43

**Description:**
One-page written summary suitable for a decision-maker who will not attend the
presentation. States the question, the method, the result, and the recommendation
in plain language.

**Acceptance Criteria:**
- Under 500 words
- References specific cost figures from the reference run
- States confidence level and key assumptions explicitly
- Saved to `docs/executive_summary.md`
