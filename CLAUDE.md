# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Single Image is a local-first Windows desktop app for photo organization and deduplication. It scans directories, detects exact duplicates (SHA-256) and visually similar photos (perceptual hashing + SSIM), and moves unwanted files to the Windows Recycle Bin.

**Stack:** React 19 + TypeScript + Vite (frontend) · Tauri 2 (desktop shell) · Rust + SQLite (backend)

## Commands

```bash
# Install dependencies
pnpm install --ignore-scripts

# Development
pnpm dev              # Vite dev server only (port 1420)
pnpm desktop:dev      # Full Tauri app with live reload (use this for most dev work)

# Build
pnpm build            # Frontend only
pnpm desktop:build --debug  # Full desktop app → src-tauri/target/debug/single-image.exe

# Tests (Rust only — no frontend tests exist)
cargo test --target-dir target-check --manifest-path src-tauri/Cargo.toml
cargo test --target-dir target-check --manifest-path src-tauri/Cargo.toml -- test_name  # single test

# TypeScript type-check (no emit)
pnpm exec tsc --noEmit
```

Tauri CLI is invoked on-demand via `pnpm dlx`—do not add it as a persistent dependency.

## Architecture

### Data Model
A dual-layer model separates content identity from file location:
- **`content_assets`** — one row per unique content (keyed by SHA-256); stores analysis version
- **`file_instances`** — one row per physical file on disk; stores path, Windows file ID, phash/dhash, quality score, thumbnail; references a `content_asset`

This allows tracking a file through moves, renames, and cross-disk migrations using Windows volume serial + file index (`fs_id.rs`).

### Deduplication Groups
**`match_groups`** + **`group_members`** represent clusters of related files:
- `exact` — same SHA-256
- `similar` — phash/dhash + SSIM ≥ 95%
- `raw_jpeg_set` — same filename stem, different extension (e.g., `.rw2` + `.jpg`)

Each group has a `ReviewStatus` (pending → approved/skipped/applied) and a recommended keep instance.

### IPC Boundary
All frontend↔backend communication goes through 8 Tauri commands registered in `lib.rs`. The frontend wrapper is `src/api.ts`. Types are defined in both `src/types.ts` (TypeScript) and `src-tauri/src/models.rs` (Rust)—keep them in sync when changing the API.

Commands: `app_snapshot`, `scan_start`, `scan_list_unknown_formats`, `review_list_groups`, `review_get_group`, `review_apply_decision`, `file_lookup_history`, `history_list_actions`.

### Key Files
| File | Role |
|---|---|
| `src-tauri/src/app.rs` | `AppService` — all DB queries, scan logic, group detection, decision application |
| `src-tauri/src/image_tools.rs` | SHA-256, BLAKE3 quick hash, phash/dhash, SSIM, thumbnail generation |
| `src-tauri/src/models.rs` | Serializable structs shared across the IPC boundary |
| `src-tauri/src/fs_id.rs` | Windows file identity (volume serial + file index) |
| `src/App.tsx` | Entire frontend UI (scan, review tabs, group detail, history) |
| `src/api.ts` | Typed wrappers around `invoke()` |

### Scan Flow
1. Walk directories, filter by supported extensions
2. Quick-hash each file (BLAKE3 on file start/middle/end) to detect unchanged files
3. For new/changed files: compute SHA-256, phash, dhash, quality score, thumbnail
4. Upsert into `content_assets` and `file_instances`
5. Build `match_groups` from duplicate/similarity/raw-jpeg heuristics
6. Store recommendations (which instance to keep)

## Supported Formats
- **Full analysis** (hash + phash + quality + thumbnail): jpg, jpeg, png, webp
- **Hash only** (no preview): heic, heif, rw2
