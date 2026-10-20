# Policies

A policy records one error the error-detection agent found, and the fix it proposes. There is one file per error signature:

```
policies/<instance>/<resource-slug>/<sig8>.yaml
```

- `<instance>` is the instance's name in `.platform-agents/tools.yaml`.
- `<resource-slug>` is what failed, e.g. `<pipeline>-<task>`.
- `<sig8>` is the first 8 hex characters of the signature hash. The same error always gets the same `sig8`, which is how the agent avoids opening duplicate PRs.

The `platform-agents-checks` workflow validates each file against the product's policy schema. The agent opens a PR containing the policy, plus the code fix when the fix is in this repo. A person reviews and merges it.

When the fix can't be made in this repo (for example a missing connection), `remediation.type` is `manual`. `manual_steps` then says exactly what the reviewer must do.

`context.checks` lists the metadata checks the agent ran and what each showed. The agent can never read table rows, file contents, secret values or anything inside the orchestrator, so checks only ever show names, schemas, hashes, roles and states.

If the same error comes back more than 24 hours after its fix was merged, the agent opens a new PR on a `-recur-<date>` branch, adds an entry to the policy's `history`, and says plainly that the earlier fix didn't hold.
