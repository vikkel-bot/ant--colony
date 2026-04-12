# ANT COLONY — Claude Code Operating Rules

## Purpose
This file is the canonical instruction source for Claude Code work in this repo.

## Working mode
- Execute tasks end-to-end when requested: inspect -> implement -> test -> regression-check -> report
- Prefer meaningful complete steps over many tiny fragmented steps
- Keep solutions minimal, safe, deterministic, and explainable
- Preserve existing behavior unless the task explicitly asks for behavior change
- Fail-closed over fail-open
- Observability before new logic
- Avoid unnecessary complexity
- Keep work reproducible through tests

## Engineering rules
- No manual redesign unless explicitly requested
- No scope creep
- No cosmetic refactors unless they are required to complete the task safely
- Reuse existing structures, naming, and module boundaries
- Leave non-scope issues untouched and report them briefly
- If something is intentionally left hardcoded because it is outside scope, say so explicitly
- Prefer one canonical source per config/policy surface
- Do not silently introduce a second source of truth

## Prompt execution template
When given a task, execute it in this structure:

1. Inspect only the relevant files/modules
2. Implement the requested change minimally and safely
3. Run the minimum relevant tests
4. Run regression checks if the touched area requires it
5. Report briefly:
   - what changed
   - which tests ran and results
   - what was intentionally left untouched
   - final status: OK / PARTIAL / BLOCKED

## Default constraints
- No redesign
- No new abstraction unless strictly needed
- No extra features
- No execution-impacting changes unless explicitly requested
- Maintain fail-closed behavior
- Maintain deterministic outputs where applicable

## Quality bar
A task is considered complete only if:
- the requested change is actually implemented
- relevant tests pass
- regressions are not knowingly introduced
- the report is concise and concrete

## Repository context
Project: ANT COLONY
Goal: modular, adaptive trading architecture
Priority order:
1. stability
2. observability
3. determinism
4. controlled evolution
5. only then more complexity

---

## Current project phase (as of AC-144)

The development phase has shifted from:
- research / simulation expansion

To:
- controlled live/test transition
- real feedback integration
- gradual colony expansion across markets

EDGE3 is already running live outside the colony and collecting real runtime data.
The colony itself is still too simulation/paper-oriented. The next build phase corrects this.

---

## Core principle — Reality before refinement

**Hard rule:** Do not build new intelligence or abstraction layers unless they directly contribute to:
- testability
- live-readiness
- operator safety
- real feedback integration

One working colony core is more valuable than multiple new simulation layers.
Refinement only after reality is confirmed working.

---

## Consultant-derived project rules (binding)

These rules are permanent constraints derived from external review. They apply to all future work:

- Do not widen the AC-143 pipeline without demonstrated necessity
- Live lane and test lane must be built in isolation — no shared state between them
- Operator visibility is mandatory before any capital activation
- Macro freeze and external risk override are required before live capital is activated
- Feedback schema must be defined before any execution coupling is built
- Paper lane and live lane must never share state

---

## Build priority order (next phases)

Work must proceed in this order. Do not skip ahead:

1. AC-145 watchdog / alerting — keep ready and maintained
2. Live lane isolation / controlled test lane (AC-144/next)
3. Macro freeze + external risk override
4. Broker execution integration
5. Real feedback loop
6. Expansion to multiple markets and strategies

---

## Colony growth model

The colony does not expand broadly in one step. Enforced sequence:

1. First: 1 working ant / 1 lane / 1 controlled path
2. Then: stable test phase with real feedback
3. Then: expansion to additional markets and strategies
4. Then: optimization and adaptive growth

Do not proceed to a later stage without confirming the prior stage is stable and observable.

---

## What Claude Code must and must not do in this phase

**Do:**
- Take small, safe, additive steps
- Explicitly state how each step contributes to real colony operation
- Preserve all existing tests
- Maintain fail-closed behavior

**Do not:**
- Build additional research or simulation layers without operational necessity
- Expand architecture because it is conceptually elegant
- Refine paper/simulation components without a clear live/test goal

---

## Git discipline (mandatory)

Every AC step ends with a commit and push. No exceptions.

**GitHub main branch is the only source of truth.**
Claude Code and future sessions must always operate from GitHub state, not local state.

### Procedure — after successful implementation and tests

```powershell
git add .
git commit -m "AC-XXX: short description"
git push origin main
```

### Rules

- `git add`, `git commit`, and `git push` are part of the definition of done
- A task is NOT complete until its changes exist on GitHub main
- Commit message format: `AC-XXX: short description` (matches existing repo history)
- Push immediately after commit — do not batch multiple ACs into one push
- If a push fails, resolve before starting the next AC step
- Local-only state is considered unfinished work
