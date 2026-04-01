# Agent Guide for Building Guest Modules

This directory contains documentation optimized for AI agents building wruntime WASM guest modules. These docs are structured for minimal ambiguity and maximum copy-paste correctness.

| Document | Purpose |
|----------|---------|
| [module_template.md](module_template.md) | Fill-in-the-blank skeleton for new modules |
| [api_reference.md](api_reference.md) | Exact function signatures for all guest-callable APIs |
| [constraints.md](constraints.md) | Hard rules, gotchas, and common mistakes |
| [decision_matrix.md](decision_matrix.md) | Choose the right pattern for the task |
| [examples.md](examples.md) | Index of real code in the repo |
| [codegen.md](codegen.md) | Proto-to-Rust code generation mapping |

## Quick start

1. Read [decision_matrix.md](decision_matrix.md) to pick handler vs. runner vs. combined
2. Copy the skeleton from [module_template.md](module_template.md)
3. Use [api_reference.md](api_reference.md) as the source of truth for function signatures
4. Check [constraints.md](constraints.md) before building to avoid known pitfalls
5. Refer to [codegen.md](codegen.md) to understand what `wr-build` generates from your proto
