# Pipelines

One folder per pipeline the pipeline builder has built: `pipelines/<id>/pipeline.yaml`, the **spec**. The spec says what the pipeline reads, does and writes, and why it exists. Its code sits where its orchestrator expects code: the orchestrator's build guide in the product says exactly where, and the spec's `artifacts` list every file.

The `platform-agents-checks` workflow checks every spec:
- It follows the product's pipeline schema.
- Its instances exist in `.platform-agents/tools.yaml` and use `pipeline-builder`.
- Its sources and targets are in places those instances allow.
- Its files exist and stay inside the pipeline's own paths.

It also checks the orchestrator's code:
- It reads and writes only allowed places.
- It holds no credentials or public-access grants.
- It compiles, imports on the orchestrator's version, and its SQL parses.

To ask for a pipeline, or a change to one, open a **Pipeline request** issue. To ask about a pipeline PR, or change it, comment `@claude <question or change>` on it.

Spec fields, in short:

| Field | Content |
|---|---|
| `id`, `title`, `description`, `owner`, `request`, `created_at` | What it is, who owns it, and the issue that asked for it |
| `orchestrator`, `schedule`, `timezone` | The instance that runs it, and when |
| `sources`, `targets` | Instance plus object, written the way that instance's tool writes it. Targets have a `write_mode` (`append`, `merge` or `replace`), and `merge_keys` for merges. |
| `steps` | In run order, each with its id, kind, the instance it runs on, its file if any, and its `inputs` and `outputs`: the pipeline's lineage |
| `artifacts` | Every file the pipeline consists of |
| `manual_steps`, `assumptions` | What a person must do first, and what the builder assumed |
| `context` | Versions, docs read, and metadata checks run while designing it |
| `history` | One entry per change after it was first built |
