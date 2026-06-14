# Contributing to STRATA

STRATA is the reproducibility artifact for a database research paper, so the bar
for the code is production-grade hygiene. Three gates run in CI on every push and
pull request (see `.github/workflows/ci.yml`) and must pass before merge.

## The gates

Run all three locally before pushing:

```sh
# 1. Formatting: not negotiable.
cargo fmt --all -- --check

# 2. Lints: zero warnings, treated as errors.
cargo clippy --all-targets -- -D warnings

# 3. Tests: unit, integration, and doc tests.
cargo test
```

To auto-apply formatting before committing:

```sh
cargo fmt --all
```

## Toolchain

The toolchain is pinned in `rust-toolchain.toml` (stable `1.93.1` with `rustfmt`
and `clippy`). `rustup` reads this automatically, so local and CI run the same
compiler. Bump that file and the version exercised in CI together.

## Lint policy

The crate sets `#![warn(clippy::all)]` in `src/lib.rs`, and CI promotes warnings
to errors via `-D warnings`. `clippy::pedantic` is intentionally not enabled: it
surfaces several hundred findings and would gate the build without a cleanup of
validated, paper-backing code. If you add a targeted
`#[allow(...)]`, justify it with a one-line comment on the same line or directly
above it.

## What not to touch

The existence-bitset and routing logic (`src/routing/hash.rs`, the bitset code in
`src/types.rs`, and the segment-filtering paths) is validated and backs the
paper's results. Changes there must come with tests demonstrating the behavior is
preserved.
