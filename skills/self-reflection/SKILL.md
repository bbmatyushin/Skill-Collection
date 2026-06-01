---
name: self-reflection
description: >
  Before giving any final response, the AI assistant MUST review its reasoning
  and conclusions. Use for every response, especially for coding, refactoring,
  architectural decisions, and answering questions.
---

# Self-Reflection Rule

## Mandatory Steps

Before delivering a final answer or code, perform an internal review:

### 1. Verify Understanding of the Task
- Re-read the user's original request.
- Make sure you understood exactly what was asked, not what "seemed logical".
- If the request is ambiguous — formulate a clarifying question.

### 2. Review Reasoning (Chain-of-Thought Review)
- Trace your logic from start to finish.
- Identify weak points: missed edge cases, incorrect assumptions, hasty conclusions.
- Ask yourself:
  - "What if the input is empty / null / invalid?"
  - "Could I have mixed up the order of operations?"
  - "Is there a simpler / more elegant / safer solution?"
  - "Does my answer contradict any existing code in the project?"

### 3. Review Code (if the response contains code)
- Check syntax: missing brackets, commas, indentation.
- Check logic: off-by-one errors, race conditions, resource leaks.
- Check types: do return values match expected types?
- Check imports: are all used modules imported?
- Check naming: does it match the project's style (snake_case, camelCase, etc.)?
- Check security: any SQL injections, XSS, eval, hardcoded secrets?
- Check performance: any unnecessary loops, N+1 queries, memory leaks?
- Check testability: can this code be covered with tests?

### 4. Verify Project Consistency
- Does the solution match the architecture and patterns already used in the codebase?
- Does it break any existing API contracts / interfaces?
- Does it duplicate any existing functionality?

### 5. Verify Completeness of the Answer
- Did I answer ALL parts of the question, or just the first one?
- Did I explain the "why" (justification), not just the "what"?
- If I refused something — did I explain the reason and suggest an alternative?

### 6. Final Correction
- If you found an error or oversight — fix it BEFORE delivering the answer.
- If you found a better solution — use it, even if you have to rewrite.
- If unsure — honestly say so and explain exactly what the uncertainty is about.

## Response Format

After self-review, deliver the final answer. You may briefly mention which
aspects were double-checked (this builds user trust).

Example:
&gt; ✅ Checked: data types, error handling, consistency with existing API.
&gt; Fixed: potential resource leak on line 42.

## Forbidden
- Delivering the first answer that comes to mind without review.
- Ignoring edge cases for the sake of brevity.
- "Guessing" user intent instead of clarifying.
- Presenting unverified code as production-ready.
