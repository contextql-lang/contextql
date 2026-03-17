# What This Starter Pack Gives You

## 1. A practical grammar scaffold

`grammar/contextql.lark` includes starter support for:

- `SELECT`
- `CREATE / ALTER / DROP CONTEXT`
- `CREATE / ALTER / DROP EVENT LOG`
- `CREATE / DROP PROCESS MODEL`
- `SHOW / DESCRIBE / REFRESH / VALIDATE`
- `CONTEXT IN`
- `CONTEXT ON`
- `WEIGHT`
- `THEN`
- `AT / BETWEEN`
- `MCP(...)`
- `REMOTE(...)`
- provider registration
- namespace / grant / set basics

It is intentionally a **tooling grammar first**, not yet your final normative grammar.

---

## 2. A parser scaffold

`contextql/parser.py` provides:

- stable parse API
- normalized syntax errors
- file parsing
- parse tree dumping for debugging

This is the right boundary for:

- CLI
- tests
- future LSP
- future semantic analyzer

---

## 3. A linter scaffold

`contextql/linter.py` provides a minimal semantic pass with starter rules:

- `CTX001` undefined context
- `CTX002` `ORDER BY CONTEXT` without `WHERE CONTEXT IN`
- `CTX003` `CONTEXT_SCORE()` outside context query
- `CTX004` `CONTEXT WINDOW` without scores
- `CTX005` temporal qualifier on non-temporal context
- `CTX006` negative weight
- `CTX007` joined query missing explicit `CONTEXT ON`

This is enough to begin building the editor experience before the full engine exists.

---

## 4. Language server and tooling specs

You asked for the minimal architecture earlier.  
The two docs included are now ready to become repo artifacts:

- `docs/LANGUAGE_SERVER_SPEC.md`
- `docs/TOOLING.md`

These define the expected shape of:

- LSP features
- diagnostics contract
- completion sources
- parser/linter boundaries
- tooling stability expectations

---

## 5. Why this is the right next step

This sequencing is strong:

```text
whitepaper
→ grammar
→ parser
→ linter
→ language server
→ engine
```

Because the grammar/linter/language server become the **developer-facing contract**.

That contract should stabilize before deep executor work.