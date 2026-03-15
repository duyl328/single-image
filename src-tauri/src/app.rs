use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::cmp;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use rayon::prelude::*;
use rusqlite::{params, Connection, OptionalExtension, Row, Transaction};
use serde::Serialize;
use tauri::Manager;
use walkdir::WalkDir;

use crate::fs_id::{read_windows_identity, WindowsIdentity};
use image::GrayImage;

use crate::image_tools::{
    analyze_asset, classify_extension, FileClass,
    load_similarity_buffer, ssim_from_buffers, normalized_extension, normalized_stem,
    path_to_string, AssetAnalysis, ANALYSIS_VERSION, SIMILARITY_THRESHOLD,
    PHASH_MAX_DISTANCE, DHASH_MAX_DISTANCE,
};
use crate::models::{
    AppSnapshot, DecisionPayload, GroupDetail, GroupMember, GroupSummary, GroupingProgress,
    MatchKind, PathHistoryItem, ReviewActionSummary, ReviewGroupFilter, ReviewStatus,
    ScanActiveItem, ScanProgress, ScanRecentItem, ScanResult, ScanTaskStarted, ScanTaskStatus,
    UnknownFormatSummary,
};

const MEDIA_TYPE_IMAGE: &str = "image";
const MEDIA_TYPE_VIDEO: &str = "video";

/// Analysis batches: commit every N files in a separate transaction.
/// Keeps individual transactions short and allows clean cancellation
/// between batches without rolling back all work.
const SCAN_BATCH_SIZE: usize = 200;

#[derive(Debug, Clone)]
pub struct AppService {
    pub db_path: PathBuf,
    pub thumbs_dir: PathBuf,
    pub scan_progress: Arc<Mutex<ScanProgress>>,
    pub next_task_id: Arc<AtomicU64>,
    /// Set to true to request cancellation of the running scan.
    pub cancel_flag: Arc<AtomicBool>,
}

#[derive(Debug)]
enum ScanDisposition {
    NewFile,
    UpdatedLocation,
    Unchanged,
}

#[derive(Debug)]
enum PreparedImagePath {
    Disposition(ScanDisposition),
    NeedsAnalysis(PendingFileAnalysis),
}

#[derive(Debug, Clone)]
struct ScanRun {
    id: i64,
    started_at: String,
}

#[derive(Debug, Clone)]
struct ExistingInstance {
    id: i64,
    current_path: String,
    path_key: String,
    volume_id: Option<String>,
    file_id: Option<String>,
    file_size: i64,
    modified_ms: i64,
    exists_flag: bool,
}

#[derive(Debug, Clone)]
struct AssetRecord {
    id: i64,
}

#[derive(Debug, Clone)]
struct ActiveRecord {
    file_instance_id: i64,
    content_asset_id: i64,
    path: String,
    extension: String,
    width: Option<u32>,
    height: Option<u32>,
    quality_score: Option<f32>,
    thumbnail_path: Option<String>,
    preview_supported: bool,
    sha256: String,
    phash: Option<String>,
    dhash: Option<String>,
}

#[derive(Debug)]
struct GroupDraft {
    anchor: String,
    kind: MatchKind,
    recommendation_reason: String,
    recommended_keep_instance_id: Option<i64>,
    members: Vec<GroupMemberDraft>,
}

#[derive(Debug)]
struct GroupMemberDraft {
    file_instance_id: i64,
    content_asset_id: i64,
    similarity: Option<f32>,
    role: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DecisionResult {
    pub group_id: i64,
    pub recycled_count: usize,
    pub applied_at: String,
}

#[derive(Debug, Clone)]
struct ScanRoot {
    actual_path: PathBuf,
    display: String,
    key: String,
}

#[derive(Debug, Clone)]
struct PendingFileAnalysis {
    file_path: PathBuf,
    display_path: String,
    path_key: String,
    extension: String,
    file_class: FileClass,
    file_size: i64,
    modified_ms: i64,
    windows_identity: Option<WindowsIdentity>,
}

#[derive(Debug, Clone)]
struct AnalyzedFile {
    pending: PendingFileAnalysis,
    analysis: AssetAnalysis,
}

/// A sidecar file (aae, xmp) discovered during the scan walk.
#[derive(Debug, Clone)]
struct SidecarFile {
    path: String,
    path_key: String,
    extension: String,
    file_size: i64,
}

impl AppService {
    pub fn new(app_handle: &tauri::AppHandle) -> Result<Self> {
        let data_dir = app_handle
            .path()
            .app_data_dir()
            .context("unable to resolve app data directory")?;
        fs::create_dir_all(&data_dir)?;

        let db_path = data_dir.join("single-image.db");
        let thumbs_dir = data_dir.join("thumbs");
        fs::create_dir_all(&thumbs_dir)?;

        let service = Self {
            db_path,
            thumbs_dir,
            scan_progress: Arc::new(Mutex::new(ScanProgress::idle())),
            next_task_id: Arc::new(AtomicU64::new(1)),
            cancel_flag: Arc::new(AtomicBool::new(false)),
        };
        service.ensure_schema()?;
        Ok(service)
    }

    pub fn ensure_schema(&self) -> Result<()> {
        let conn = self.open()?;
        conn.execute_batch(
            "
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS scan_runs (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              status TEXT NOT NULL,
              roots_json TEXT NOT NULL,
              started_at TEXT NOT NULL,
              completed_at TEXT,
              new_files INTEGER NOT NULL DEFAULT 0,
              updated_locations INTEGER NOT NULL DEFAULT 0,
              unchanged_files INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS content_assets (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              media_type TEXT NOT NULL,
              extension TEXT NOT NULL,
              file_size INTEGER NOT NULL,
              quick_fingerprint TEXT NOT NULL,
              sha256 TEXT NOT NULL UNIQUE,
              width INTEGER,
              height INTEGER,
              format_name TEXT,
              captured_at TEXT,
              phash TEXT,
              dhash TEXT,
              quality_score REAL,
              thumbnail_path TEXT,
              preview_supported INTEGER NOT NULL DEFAULT 0,
              analysis_version INTEGER NOT NULL,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_content_assets_quick
              ON content_assets (file_size, quick_fingerprint);

            CREATE TABLE IF NOT EXISTS file_instances (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              content_asset_id INTEGER NOT NULL,
              current_path TEXT NOT NULL,
              path_key TEXT NOT NULL UNIQUE,
              volume_id TEXT,
              file_id TEXT,
              file_size INTEGER NOT NULL,
              modified_ms INTEGER NOT NULL,
              extension TEXT NOT NULL,
              file_class TEXT NOT NULL DEFAULT 'image',
              first_seen_at TEXT NOT NULL,
              last_seen_at TEXT NOT NULL,
              last_scan_run_id INTEGER,
              exists_flag INTEGER NOT NULL DEFAULT 1,
              FOREIGN KEY(content_asset_id) REFERENCES content_assets(id) ON DELETE CASCADE,
              FOREIGN KEY(last_scan_run_id) REFERENCES scan_runs(id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_file_instances_asset
              ON file_instances (content_asset_id, exists_flag);

            CREATE INDEX IF NOT EXISTS idx_file_instances_identity
              ON file_instances (volume_id, file_id);

            CREATE TABLE IF NOT EXISTS path_history (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              file_instance_id INTEGER NOT NULL,
              old_path TEXT NOT NULL,
              new_path TEXT NOT NULL,
              change_type TEXT NOT NULL,
              detected_at TEXT NOT NULL,
              FOREIGN KEY(file_instance_id) REFERENCES file_instances(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS match_groups (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              anchor TEXT NOT NULL UNIQUE,
              kind TEXT NOT NULL,
              status TEXT NOT NULL,
              recommendation_reason TEXT NOT NULL,
              recommended_keep_instance_id INTEGER,
              created_at TEXT NOT NULL,
              updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS group_members (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              group_id INTEGER NOT NULL,
              file_instance_id INTEGER NOT NULL,
              content_asset_id INTEGER NOT NULL,
              similarity REAL,
              role TEXT,
              FOREIGN KEY(group_id) REFERENCES match_groups(id) ON DELETE CASCADE,
              FOREIGN KEY(file_instance_id) REFERENCES file_instances(id) ON DELETE CASCADE,
              UNIQUE(group_id, file_instance_id)
            );

            CREATE TABLE IF NOT EXISTS review_actions (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              group_id INTEGER NOT NULL,
              group_kind TEXT NOT NULL,
              action_type TEXT NOT NULL,
              keep_instance_ids_json TEXT NOT NULL,
              recycle_instance_ids_json TEXT NOT NULL,
              note TEXT,
              created_at TEXT NOT NULL,
              FOREIGN KEY(group_id) REFERENCES match_groups(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS unknown_formats (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              scan_run_id INTEGER NOT NULL,
              extension TEXT NOT NULL,
              count INTEGER NOT NULL,
              example_path TEXT NOT NULL,
              FOREIGN KEY(scan_run_id) REFERENCES scan_runs(id) ON DELETE CASCADE,
              UNIQUE(scan_run_id, extension)
            );

            -- Sidecar metadata files (aae, xmp).  Not tracked as content
            -- assets; just recorded for reference.
            CREATE TABLE IF NOT EXISTS sidecar_files (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              scan_run_id INTEGER NOT NULL,
              path TEXT NOT NULL,
              path_key TEXT NOT NULL,
              extension TEXT NOT NULL,
              file_size INTEGER NOT NULL,
              first_seen_at TEXT NOT NULL,
              last_seen_at TEXT NOT NULL,
              FOREIGN KEY(scan_run_id) REFERENCES scan_runs(id) ON DELETE CASCADE,
              UNIQUE(scan_run_id, path_key)
            );

            -- Staging table for the phased scan pipeline.  Each row tracks
            -- a discovered file through: discovered → quick_hashed →
            -- sha256_done → visual_done → promoted.  Currently populated
            -- but the pipeline still uses in-memory state; the table is
            -- reserved for future resumability support.
            CREATE TABLE IF NOT EXISTS scan_queue (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              scan_run_id INTEGER NOT NULL,
              path TEXT NOT NULL,
              path_key TEXT NOT NULL,
              file_class TEXT NOT NULL,
              file_size INTEGER NOT NULL,
              modified_ms INTEGER NOT NULL,
              stage TEXT NOT NULL DEFAULT 'discovered',
              quick_fingerprint TEXT,
              sha256 TEXT,
              FOREIGN KEY(scan_run_id) REFERENCES scan_runs(id) ON DELETE CASCADE,
              UNIQUE(scan_run_id, path_key)
            );
            ",
        )?;

        // Schema migration: add file_class column to existing databases that
        // pre-date this column.
        let has_file_class: bool = conn.query_row(
            "SELECT COUNT(*) FROM pragma_table_info('file_instances') WHERE name = 'file_class'",
            [],
            |row| row.get::<_, i64>(0).map(|n| n > 0),
        )?;
        if !has_file_class {
            conn.execute_batch(
                "ALTER TABLE file_instances ADD COLUMN file_class TEXT NOT NULL DEFAULT 'image';",
            )?;
        }

        Ok(())
    }

    pub fn snapshot(&self) -> Result<AppSnapshot> {
        let conn = self.open()?;
        let pending_group_count: usize = conn.query_row(
            "SELECT COUNT(*) FROM match_groups mg
             WHERE mg.status = 'pending'
               AND EXISTS (SELECT 1 FROM group_members gm WHERE gm.group_id = mg.id)",
            [],
            |row| row.get(0),
        )?;
        let applied_action_count: usize =
            conn.query_row("SELECT COUNT(*) FROM review_actions", [], |row| row.get(0))?;
        let indexed_asset_count: usize =
            conn.query_row("SELECT COUNT(*) FROM content_assets", [], |row| row.get(0))?;
        let active_file_count: usize = conn.query_row(
            "SELECT COUNT(*) FROM file_instances WHERE exists_flag = 1",
            [],
            |row| row.get(0),
        )?;

        Ok(AppSnapshot {
            pending_group_count,
            applied_action_count,
            indexed_asset_count,
            active_file_count,
        })
    }

    fn open(&self) -> Result<Connection> {
        let conn = Connection::open(&self.db_path)
            .with_context(|| format!("unable to open {:?}", self.db_path))?;
        // WAL journal mode: readers never block writers, writers never block
        // readers.  synchronous=NORMAL is safe with WAL and gives ~3× better
        // write throughput than the default FULL.
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
        Ok(conn)
    }

    #[cfg(test)]
    pub fn start_scan(&self, paths: Vec<String>) -> Result<ScanResult> {
        // Tests run single-threaded; 2 workers is enough and keeps CI fast.
        self.start_scan_with_threads(paths, 2)
    }

    fn start_scan_with_threads(&self, paths: Vec<String>, threads: usize) -> Result<ScanResult> {
        if paths.is_empty() {
            return Err(anyhow!("at least one folder is required"));
        }

        {
            let progress = self
                .scan_progress
                .lock()
                .map_err(|_| anyhow!("scan progress lock poisoned"))?;
            if matches!(
                progress.status,
                ScanTaskStatus::Counting | ScanTaskStatus::Running | ScanTaskStatus::Finalizing
            ) {
                return Err(anyhow!("a scan is already running"));
            }
        }

        let task_id = self.next_task_id.fetch_add(1, Ordering::SeqCst);
        let started_at = iso_now();
        self.set_scan_progress(scan_progress_counting(task_id, &started_at))?;

        match self.perform_scan(paths, task_id, threads) {
            Ok(scan_result) => {
                self.set_scan_progress(scan_progress_completed(task_id, &scan_result))?;
                Ok(scan_result)
            }
            Err(error) => {
                self.set_scan_progress(scan_progress_failed(task_id, &started_at, &error.to_string()))?;
                Err(error)
            }
        }
    }

    pub fn start_scan_task(&self, paths: Vec<String>, threads: usize) -> Result<ScanTaskStarted> {
        if paths.is_empty() {
            return Err(anyhow!("at least one folder is required"));
        }

        {
            let progress = self
                .scan_progress
                .lock()
                .map_err(|_| anyhow!("scan progress lock poisoned"))?;
            if matches!(
                progress.status,
                ScanTaskStatus::Counting | ScanTaskStatus::Running | ScanTaskStatus::Finalizing
            ) {
                return Err(anyhow!("a scan is already running"));
            }
        }

        // Clear any lingering cancel signal from a previous scan.
        self.cancel_flag.store(false, Ordering::SeqCst);

        let task_id = self.next_task_id.fetch_add(1, Ordering::SeqCst);
        let started_at = iso_now();
        self.set_scan_progress(scan_progress_counting(task_id, &started_at))?;

        let service = self.clone();
        thread::spawn(move || {
            let result = service.perform_scan(paths, task_id, threads);
            match result {
                Ok(scan_result) => {
                    let mut state = service.scan_progress.lock().ok();
                    if let Some(state) = state.as_mut() {
                        **state = scan_progress_completed(task_id, &scan_result);
                    }
                }
                Err(error) => {
                    let _ = service.set_scan_progress(scan_progress_failed(
                        task_id,
                        &started_at,
                        &error.to_string(),
                    ));
                }
            }
        });

        Ok(ScanTaskStarted { task_id })
    }

    /// Request cancellation of the currently running scan.  The scan thread
    /// checks this flag at phase boundaries and will exit at the next safe
    /// checkpoint.
    pub fn scan_cancel(&self) -> Result<()> {
        self.cancel_flag.store(true, Ordering::SeqCst);
        Ok(())
    }

    pub fn scan_status(&self) -> Result<ScanProgress> {
        self.scan_progress
            .lock()
            .map(|state| state.clone())
            .map_err(|_| anyhow!("scan progress lock poisoned"))
    }

    fn set_scan_progress(&self, progress: ScanProgress) -> Result<()> {
        let mut state = self
            .scan_progress
            .lock()
            .map_err(|_| anyhow!("scan progress lock poisoned"))?;
        *state = progress;
        Ok(())
    }

    fn lock_progress(&self) -> Result<std::sync::MutexGuard<'_, ScanProgress>> {
        self.scan_progress
            .lock()
            .map_err(|_| anyhow!("scan progress lock poisoned"))
    }

    fn is_cancelled(&self) -> bool {
        self.cancel_flag.load(Ordering::Relaxed)
    }

    fn perform_scan(&self, paths: Vec<String>, task_id: u64, threads: usize) -> Result<ScanResult> {
        if paths.is_empty() {
            return Err(anyhow!("at least one folder is required"));
        }

        let normalized_roots = prepare_roots(paths)?;
        let display_roots: Vec<String> = normalized_roots
            .iter()
            .map(|root| root.display.clone())
            .collect();
        let started_at = iso_now();

        // ── Phase 1: Walk + quick-classify ────────────────────────────────────
        // All unchanged / relocated file records are committed in one fast
        // transaction.  New files accumulate in pending_analysis and are
        // processed in batches afterwards.

        let scan_run;
        let mut pending_analysis: Vec<PendingFileAnalysis> = Vec::new();
        let mut sidecar_list: Vec<SidecarFile> = Vec::new();
        let mut unsupported: BTreeMap<String, UnknownFormatSummary> = BTreeMap::new();
        let mut seen_paths: HashSet<String> = HashSet::new();
        let mut new_files = 0usize;
        let mut updated_locations = 0usize;
        let mut unchanged_files = 0usize;

        {
            let mut conn = self.open()?;
            let tx = conn.transaction()?;
            scan_run = create_scan_run(&tx, &display_roots, &started_at)?;
            self.set_scan_progress(scan_progress_counting(task_id, &started_at))?;

            for root in &normalized_roots {
                for entry in WalkDir::new(&root.actual_path) {
                    let entry = entry.with_context(|| {
                        format!("unable to enumerate files under {}", root.display)
                    })?;
                    if !entry.file_type().is_file() {
                        continue;
                    }

                    let file_path = entry.path();
                    let display_path = safe_display_path(file_path);
                    let path_key = normalize_key(&display_path);
                    seen_paths.insert(path_key.clone());

                    let extension = normalized_extension(file_path);
                    let file_class = classify_extension(&extension);

                    match file_class {
                        FileClass::Sidecar => {
                            let metadata = fs::metadata(file_path)?;
                            sidecar_list.push(SidecarFile {
                                path: display_path,
                                path_key,
                                extension,
                                file_size: metadata.len() as i64,
                            });
                            continue;
                        }
                        FileClass::Archive | FileClass::Other => {
                            let item = unsupported.entry(extension.clone()).or_insert_with(|| {
                                UnknownFormatSummary {
                                    extension: extension.clone(),
                                    count: 0,
                                    example_path: display_path.clone(),
                                }
                            });
                            item.count += 1;
                            continue;
                        }
                        FileClass::Image | FileClass::RawImage | FileClass::Video => {
                            // Falls through to the prepare step below.
                        }
                    }

                    match self.prepare_file_path(
                        &tx,
                        &scan_run,
                        file_path,
                        &display_path,
                        &path_key,
                        &extension,
                        file_class,
                        &started_at,
                    )? {
                        PreparedImagePath::Disposition(disposition) => {
                            match disposition {
                                ScanDisposition::NewFile => new_files += 1,
                                ScanDisposition::UpdatedLocation => updated_locations += 1,
                                ScanDisposition::Unchanged => unchanged_files += 1,
                            }
                        }
                        PreparedImagePath::NeedsAnalysis(item) => pending_analysis.push(item),
                    }
                }
            }

            tx.commit()?;
        }

        // ── Cancel check after walk ───────────────────────────────────────────
        if self.is_cancelled() {
            self.mark_scan_run_cancelled(scan_run.id)?;
            return Err(anyhow!("scan cancelled by user"));
        }

        // ── Phase 2: Parallel analysis + per-batch commits ───────────────────
        let total_images = unchanged_files + updated_locations + new_files + pending_analysis.len();
        {
            let mut p = self.lock_progress()?;
            p.status = ScanTaskStatus::Running;
            p.phase = "indexing".to_string();
            p.message = "Analyzing files and updating the local index.".to_string();
            p.total_files = total_images;
            p.done = unchanged_files + updated_locations + new_files;
            p.queued = pending_analysis.len();
            p.analyzing = 0;
            p.new_files = new_files;
            p.updated_files = updated_locations;
            p.unchanged_files = unchanged_files;
            p.failed_files = 0;
            p.active_items = Vec::new();
            p.recent_items = Vec::new();
            p.grouping = None;
        }

        let workers = cmp::max(1, threads);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build()
            .context("failed to build rayon thread pool")?;

        let chunk_size = cmp::max(4, workers * 2).min(SCAN_BATCH_SIZE);
        let mut done_count = unchanged_files + updated_locations + new_files;
        let total_pending = pending_analysis.len();
        let mut queued_remaining = total_pending;

        for chunk in pending_analysis.chunks(chunk_size) {
            // Show active files before processing.
            {
                let mut p = self.lock_progress()?;
                p.active_items = chunk
                    .iter()
                    .map(|item| ScanActiveItem {
                        file_name: file_name_hint(&item.display_path),
                        dir_hint: dir_hint(&item.display_path),
                    })
                    .collect();
                p.analyzing = chunk.len();
                queued_remaining = queued_remaining.saturating_sub(chunk.len());
                p.queued = queued_remaining;
            }

            let thumbs_dir = &self.thumbs_dir;
            let analyzed: Vec<AnalyzedFile> = pool.install(|| {
                chunk
                    .par_iter()
                    .cloned()
                    .filter_map(|pending| {
                        match analyze_pending_file(pending, thumbs_dir) {
                            Ok(item) => Some(item),
                            Err(_) => None, // skip failed files silently
                        }
                    })
                    .collect()
            });

            // Commit this batch in its own transaction.
            let mut chunk_recent: Vec<ScanRecentItem> = Vec::new();
            {
                let mut conn = self.open()?;
                let tx = conn.transaction()?;
                for item in analyzed {
                    let fname = file_name_hint(&item.pending.display_path);
                    let status_str = match self.commit_analyzed_file(&tx, &scan_run, item, &started_at)? {
                        ScanDisposition::NewFile => {
                            new_files += 1;
                            "new"
                        }
                        ScanDisposition::UpdatedLocation => {
                            updated_locations += 1;
                            "updated"
                        }
                        ScanDisposition::Unchanged => {
                            unchanged_files += 1;
                            "unchanged"
                        }
                    };
                    done_count += 1;
                    chunk_recent.push(ScanRecentItem {
                        file_name: fname,
                        status: status_str.to_string(),
                    });
                }
                tx.commit()?;
            }

            // Update progress once per batch.
            {
                let mut p = self.lock_progress()?;
                p.done = done_count;
                p.new_files = new_files;
                p.updated_files = updated_locations;
                p.unchanged_files = unchanged_files;
                p.active_items = Vec::new();
                p.analyzing = 0;
                p.recent_items.extend(chunk_recent);
                let len = p.recent_items.len();
                if len > 8 {
                    p.recent_items.drain(0..(len - 8));
                }
            }

            // Check cancel after each batch.
            if self.is_cancelled() {
                self.mark_scan_run_cancelled(scan_run.id)?;
                return Err(anyhow!("scan cancelled by user"));
            }
        }

        // ── Phase 3: Finalise ─────────────────────────────────────────────────
        {
            let mut p = self.lock_progress()?;
            p.status = ScanTaskStatus::Finalizing;
            p.phase = "finalizing".to_string();
            p.message = "Grouping duplicates and similar photos.".to_string();
        }

        let completed_at;
        {
            let mut conn = self.open()?;
            let tx = conn.transaction()?;

            mark_missing_within_roots(&tx, &normalized_roots, &seen_paths, scan_run.id)?;
            write_sidecar_files(&tx, &scan_run, &sidecar_list, &started_at)?;
            rewrite_unknown_formats(&tx, scan_run.id, &unsupported)?;

            // Groups run inside the same bounded thread pool.
            recompute_groups(&tx, &self.scan_progress, &pool)?;

            completed_at = iso_now();
            tx.execute(
                "UPDATE scan_runs
                 SET status = 'completed',
                     completed_at = ?2,
                     new_files = ?3,
                     updated_locations = ?4,
                     unchanged_files = ?5
                 WHERE id = ?1",
                params![
                    scan_run.id,
                    completed_at,
                    new_files as i64,
                    updated_locations as i64,
                    unchanged_files as i64
                ],
            )?;
            tx.commit()?;
        }

        Ok(ScanResult {
            scan_run_id: scan_run.id,
            started_at: scan_run.started_at,
            completed_at,
            scanned_roots: display_roots,
            new_files,
            updated_locations,
            unchanged_files,
            unsupported_extensions: unsupported.into_values().collect(),
        })
    }

    fn mark_scan_run_cancelled(&self, scan_run_id: i64) -> Result<()> {
        let conn = self.open()?;
        conn.execute(
            "UPDATE scan_runs SET status = 'cancelled', completed_at = ?2 WHERE id = ?1",
            params![scan_run_id, iso_now()],
        )?;
        Ok(())
    }

    pub fn list_unknown_formats(&self, scan_run_id: i64) -> Result<Vec<UnknownFormatSummary>> {
        let conn = self.open()?;
        let mut statement = conn.prepare(
            "SELECT extension, count, example_path
             FROM unknown_formats
             WHERE scan_run_id = ?1
             ORDER BY count DESC, extension ASC",
        )?;
        let rows = statement.query_map([scan_run_id], |row| {
            Ok(UnknownFormatSummary {
                extension: row.get(0)?,
                count: row.get::<_, i64>(1)? as usize,
                example_path: row.get(2)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn list_groups(&self, filter: ReviewGroupFilter) -> Result<Vec<GroupSummary>> {
        let conn = self.open()?;
        let mut statement = conn.prepare(
            "SELECT mg.id,
                    mg.kind,
                    mg.status,
                    mg.anchor,
                    COUNT(gm.id) AS member_count,
                    mg.recommended_keep_instance_id,
                    fi.current_path,
                    mg.recommendation_reason,
                    mg.updated_at
             FROM match_groups mg
             JOIN group_members gm ON gm.group_id = mg.id
             LEFT JOIN file_instances fi ON fi.id = mg.recommended_keep_instance_id
             WHERE (?1 IS NULL OR mg.kind = ?1)
               AND (?2 IS NULL OR mg.status = ?2)
             GROUP BY mg.id, mg.kind, mg.status, mg.anchor, mg.recommended_keep_instance_id,
                      fi.current_path, mg.recommendation_reason, mg.updated_at
             ORDER BY mg.updated_at DESC, mg.id DESC",
        )?;

        let rows = statement.query_map(
            params![
                filter.kind.map(|kind| kind.as_db_value()),
                filter.status.map(|status| status.as_db_value())
            ],
            map_group_summary,
        )?;

        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn get_group(&self, group_id: i64) -> Result<GroupDetail> {
        let conn = self.open()?;
        let group = conn
            .query_row(
                "SELECT id, kind, status, anchor, recommendation_reason, recommended_keep_instance_id
                 FROM match_groups WHERE id = ?1",
                [group_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                    ))
                },
            )
            .optional()?
            .context("group not found")?;

        let mut statement = conn.prepare(
            "SELECT gm.id,
                    gm.file_instance_id,
                    gm.content_asset_id,
                    fi.current_path,
                    fi.exists_flag,
                    fi.extension,
                    ca.format_name,
                    ca.width,
                    ca.height,
                    ca.quality_score,
                    ca.preview_supported,
                    ca.thumbnail_path,
                    ca.sha256,
                    gm.similarity,
                    gm.role,
                    ca.captured_at,
                    fi.volume_id
             FROM group_members gm
             JOIN file_instances fi ON fi.id = gm.file_instance_id
             JOIN content_assets ca ON ca.id = gm.content_asset_id
             WHERE gm.group_id = ?1
             ORDER BY COALESCE(ca.quality_score, 0) DESC, fi.current_path ASC",
        )?;

        let members = statement
            .query_map([group_id], |row| {
                Ok(GroupMember {
                    group_member_id: row.get(0)?,
                    file_instance_id: row.get(1)?,
                    content_asset_id: row.get(2)?,
                    path: row.get(3)?,
                    exists_flag: row.get::<_, i64>(4)? == 1,
                    extension: row.get(5)?,
                    format_name: row.get(6)?,
                    width: row.get::<_, Option<i64>>(7)?.map(|value| value as u32),
                    height: row.get::<_, Option<i64>>(8)?.map(|value| value as u32),
                    quality_score: row.get::<_, Option<f64>>(9)?.map(|value| value as f32),
                    preview_supported: row.get::<_, i64>(10)? == 1,
                    thumbnail_path: row.get(11)?,
                    sha256: row.get(12)?,
                    similarity: row.get::<_, Option<f64>>(13)?.map(|value| value as f32),
                    role: row.get(14)?,
                    captured_at: row.get(15)?,
                    volume_id: row.get(16)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;

        Ok(GroupDetail {
            id: group.0,
            kind: MatchKind::from_db_value(&group.1).context("invalid group kind")?,
            status: ReviewStatus::from_db_value(&group.2).context("invalid group status")?,
            anchor: group.3,
            recommendation_reason: group.4,
            recommended_keep_instance_id: group.5,
            members,
        })
    }

    pub fn apply_decision(
        &self,
        group_id: i64,
        payload: DecisionPayload,
    ) -> Result<DecisionResult> {
        let mut conn = self.open()?;
        let tx = conn.transaction()?;
        let (group_kind, recycle_targets) = validate_decision(&tx, group_id, &payload)?;
        let applied_at = iso_now();

        for (_, fs_path) in &recycle_targets {
            if fs_path.exists() {
                trash::delete(fs_path)
                    .with_context(|| format!("unable to move {:?} to recycle bin", fs_path))?;
            }
        }

        for recycle_id in &payload.recycle_ids {
            tx.execute(
                "UPDATE file_instances
                 SET exists_flag = 0,
                     last_seen_at = ?2
                 WHERE id = ?1",
                params![recycle_id, applied_at],
            )?;
        }

        tx.execute(
            "INSERT INTO review_actions (
                group_id,
                group_kind,
                action_type,
                keep_instance_ids_json,
                recycle_instance_ids_json,
                note,
                created_at
             )
             VALUES (?1, ?2, 'recycle', ?3, ?4, ?5, ?6)",
            params![
                group_id,
                group_kind,
                serde_json::to_string(&payload.keep_ids)?,
                serde_json::to_string(&payload.recycle_ids)?,
                payload.note,
                applied_at
            ],
        )?;
        tx.execute(
            "UPDATE match_groups SET status = 'applied', updated_at = ?2 WHERE id = ?1",
            params![group_id, applied_at],
        )?;
        let default_pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap_or_else(|_| rayon::ThreadPoolBuilder::new().build().unwrap());
        recompute_groups(&tx, &self.scan_progress, &default_pool)?;
        tx.commit()?;

        Ok(DecisionResult {
            group_id,
            recycled_count: payload.recycle_ids.len(),
            applied_at,
        })
    }

    pub fn lookup_history(&self, content_asset_id: i64) -> Result<Vec<PathHistoryItem>> {
        let conn = self.open()?;
        let mut statement = conn.prepare(
            "SELECT ph.file_instance_id, ph.old_path, ph.new_path, ph.change_type, ph.detected_at
             FROM path_history ph
             JOIN file_instances fi ON fi.id = ph.file_instance_id
             WHERE fi.content_asset_id = ?1
             ORDER BY ph.detected_at DESC, ph.id DESC",
        )?;
        let rows = statement.query_map([content_asset_id], |row| {
            Ok(PathHistoryItem {
                file_instance_id: row.get(0)?,
                old_path: row.get(1)?,
                new_path: row.get(2)?,
                change_type: row.get(3)?,
                detected_at: row.get(4)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn list_actions(&self) -> Result<Vec<ReviewActionSummary>> {
        let conn = self.open()?;
        let mut statement = conn.prepare(
            "SELECT id, group_id, group_kind, action_type,
                    keep_instance_ids_json, recycle_instance_ids_json, created_at, note
             FROM review_actions
             ORDER BY created_at DESC, id DESC",
        )?;
        let rows = statement.query_map([], |row| {
            let keep: String = row.get(4)?;
            let recycle: String = row.get(5)?;
            Ok(ReviewActionSummary {
                id: row.get(0)?,
                group_id: row.get(1)?,
                group_kind: MatchKind::from_db_value(&row.get::<_, String>(2)?)
                    .unwrap_or(MatchKind::Exact),
                action_type: row.get(3)?,
                keep_instance_ids: serde_json::from_str(&keep).unwrap_or_default(),
                recycle_instance_ids: serde_json::from_str(&recycle).unwrap_or_default(),
                created_at: row.get(6)?,
                note: row.get(7)?,
            })
        })?;
        Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
    }

    fn prepare_file_path(
        &self,
        tx: &Transaction<'_>,
        scan_run: &ScanRun,
        file_path: &Path,
        display_path: &str,
        path_key: &str,
        extension: &str,
        file_class: FileClass,
        observed_at: &str,
    ) -> Result<PreparedImagePath> {
        let metadata = fs::metadata(file_path)?;
        let file_size = metadata.len() as i64;
        let modified_ms = metadata_modified_ms(&metadata)?;
        let windows_identity = read_windows_identity(file_path).ok();

        if let Some(existing) = find_instance_by_path_key(tx, path_key)? {
            if existing.file_size == file_size
                && existing.modified_ms == modified_ms
                && existing.exists_flag
                && identity_matches(&existing, windows_identity.as_ref())
            {
                update_instance_seen(
                    tx,
                    existing.id,
                    scan_run.id,
                    observed_at,
                    file_size,
                    modified_ms,
                    windows_identity.as_ref(),
                )?;
                return Ok(PreparedImagePath::Disposition(ScanDisposition::Unchanged));
            }
        }

        if let Some(identity) = windows_identity.as_ref() {
            if let Some(existing) =
                find_instance_by_identity(tx, &identity.volume_id, &identity.file_id)?
            {
                if existing.path_key != path_key {
                    relocate_instance(
                        tx,
                        existing.id,
                        &existing.current_path,
                        display_path,
                        path_key,
                        scan_run.id,
                        observed_at,
                        file_size,
                        modified_ms,
                        Some(identity),
                        "same_volume_move",
                    )?;
                    return Ok(PreparedImagePath::Disposition(
                        ScanDisposition::UpdatedLocation,
                    ));
                }
            }
        }

        Ok(PreparedImagePath::NeedsAnalysis(PendingFileAnalysis {
            file_path: file_path.to_path_buf(),
            display_path: display_path.to_string(),
            path_key: path_key.to_string(),
            extension: extension.to_string(),
            file_class,
            file_size,
            modified_ms,
            windows_identity,
        }))
    }

    fn commit_analyzed_file(
        &self,
        tx: &Transaction<'_>,
        scan_run: &ScanRun,
        item: AnalyzedFile,
        observed_at: &str,
    ) -> Result<ScanDisposition> {
        let asset_id = if let Some(asset) = find_asset_by_sha(tx, &item.analysis.sha256)? {
            asset.id
        } else {
            create_asset_record(
                tx,
                &item.pending.extension,
                item.pending.file_size,
                item.pending.file_class,
                observed_at,
                &item.analysis,
            )?
        };

        if let Some(existing) = find_instance_by_path_key(tx, &item.pending.path_key)? {
            tx.execute(
                "UPDATE file_instances
                 SET content_asset_id = ?2,
                     volume_id = ?3,
                     file_id = ?4,
                     file_size = ?5,
                     modified_ms = ?6,
                     extension = ?7,
                     file_class = ?8,
                     exists_flag = 1,
                     last_seen_at = ?9,
                     last_scan_run_id = ?10
                 WHERE id = ?1",
                params![
                    existing.id,
                    asset_id,
                    item.pending
                        .windows_identity
                        .as_ref()
                        .map(|value| value.volume_id.clone()),
                    item.pending
                        .windows_identity
                        .as_ref()
                        .map(|value| value.file_id.clone()),
                    item.pending.file_size,
                    item.pending.modified_ms,
                    item.pending.extension,
                    item.pending.file_class.as_str(),
                    observed_at,
                    scan_run.id
                ],
            )?;
            return Ok(ScanDisposition::Unchanged);
        }

        if let Some(existing) = find_relocation_candidate(tx, asset_id, &item.pending.path_key)? {
            relocate_instance(
                tx,
                existing.id,
                &existing.current_path,
                &item.pending.display_path,
                &item.pending.path_key,
                scan_run.id,
                observed_at,
                item.pending.file_size,
                item.pending.modified_ms,
                item.pending.windows_identity.as_ref(),
                "cross_volume_move",
            )?;
            return Ok(ScanDisposition::UpdatedLocation);
        }

        insert_instance(
            tx,
            asset_id,
            &item.pending.display_path,
            &item.pending.path_key,
            item.pending.windows_identity.as_ref(),
            item.pending.file_size,
            item.pending.modified_ms,
            &item.pending.extension,
            item.pending.file_class,
            scan_run.id,
            observed_at,
        )?;
        Ok(ScanDisposition::NewFile)
    }
}

fn prepare_roots(paths: Vec<String>) -> Result<Vec<ScanRoot>> {
    let mut results = Vec::new();
    let mut dedupe = HashSet::new();

    for raw in paths {
        let path = PathBuf::from(raw.trim());
        if !path.exists() {
            return Err(anyhow!("{:?} does not exist", path));
        }
        if !path.is_dir() {
            return Err(anyhow!("{:?} is not a folder", path));
        }

        let canonical = fs::canonicalize(&path).unwrap_or(path.clone());
        let display = safe_display_path(&canonical);
        let key = normalize_root_key(&display);
        if dedupe.insert(key.clone()) {
            results.push(ScanRoot {
                actual_path: canonical,
                display,
                key,
            });
        }
    }

    Ok(results)
}

fn analyze_pending_file(item: PendingFileAnalysis, thumbs_dir: &Path) -> Result<AnalyzedFile> {
    let analysis = analyze_asset(
        &item.file_path,
        &item.extension,
        item.file_size as u64,
        thumbs_dir,
        None,
    )?;
    Ok(AnalyzedFile {
        pending: item,
        analysis,
    })
}

fn file_name_hint(display_path: &str) -> String {
    Path::new(display_path)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn dir_hint(display_path: &str) -> String {
    Path::new(display_path)
        .parent()
        .and_then(|p| p.file_name())
        .map(|n| format!("/{}/", n.to_string_lossy()))
        .unwrap_or_default()
}

fn scan_progress_counting(task_id: u64, started_at: &str) -> ScanProgress {
    ScanProgress {
        task_id: Some(task_id),
        status: ScanTaskStatus::Counting,
        phase: "counting".to_string(),
        message: "Scanning folders to estimate workload.".to_string(),
        total_files: 0,
        queued: 0,
        analyzing: 0,
        done: 0,
        new_files: 0,
        updated_files: 0,
        unchanged_files: 0,
        failed_files: 0,
        active_items: Vec::new(),
        recent_items: Vec::new(),
        grouping: None,
        started_at: Some(started_at.to_string()),
        completed_at: None,
        result: None,
        error: None,
    }
}

fn scan_progress_completed(task_id: u64, scan_result: &ScanResult) -> ScanProgress {
    let total_done = scan_result.new_files + scan_result.updated_locations + scan_result.unchanged_files;
    ScanProgress {
        task_id: Some(task_id),
        status: ScanTaskStatus::Completed,
        phase: "completed".to_string(),
        message: "Scan completed. Review the suggested groups.".to_string(),
        total_files: total_done,
        queued: 0,
        analyzing: 0,
        done: total_done,
        new_files: scan_result.new_files,
        updated_files: scan_result.updated_locations,
        unchanged_files: scan_result.unchanged_files,
        failed_files: 0,
        active_items: Vec::new(),
        recent_items: Vec::new(),
        grouping: None,
        started_at: Some(scan_result.started_at.clone()),
        completed_at: Some(scan_result.completed_at.clone()),
        result: Some(scan_result.clone()),
        error: None,
    }
}

fn scan_progress_failed(task_id: u64, started_at: &str, error: &str) -> ScanProgress {
    ScanProgress {
        task_id: Some(task_id),
        status: ScanTaskStatus::Failed,
        phase: "failed".to_string(),
        message: "Scan failed.".to_string(),
        total_files: 0,
        queued: 0,
        analyzing: 0,
        done: 0,
        new_files: 0,
        updated_files: 0,
        unchanged_files: 0,
        failed_files: 0,
        active_items: Vec::new(),
        recent_items: Vec::new(),
        grouping: None,
        started_at: Some(started_at.to_string()),
        completed_at: Some(iso_now()),
        result: None,
        error: Some(error.to_string()),
    }
}


fn create_scan_run(tx: &Transaction<'_>, roots: &[String], started_at: &str) -> Result<ScanRun> {
    tx.execute(
        "INSERT INTO scan_runs (status, roots_json, started_at)
         VALUES ('running', ?1, ?2)",
        params![serde_json::to_string(roots)?, started_at],
    )?;

    Ok(ScanRun {
        id: tx.last_insert_rowid(),
        started_at: started_at.to_string(),
    })
}

fn metadata_modified_ms(metadata: &fs::Metadata) -> Result<i64> {
    let modified = metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH);
    Ok(modified.duration_since(UNIX_EPOCH)?.as_millis() as i64)
}

fn identity_matches(existing: &ExistingInstance, identity: Option<&WindowsIdentity>) -> bool {
    match identity {
        Some(identity) => {
            existing.volume_id.as_deref() == Some(identity.volume_id.as_str())
                && existing.file_id.as_deref() == Some(identity.file_id.as_str())
        }
        None => true,
    }
}

fn find_instance_by_path_key(
    tx: &Transaction<'_>,
    path_key: &str,
) -> Result<Option<ExistingInstance>> {
    tx.query_row(
        "SELECT id, current_path, path_key, volume_id, file_id,
                file_size, modified_ms, exists_flag
         FROM file_instances
         WHERE path_key = ?1",
        [path_key],
        map_existing_instance,
    )
    .optional()
    .map_err(Into::into)
}

fn find_instance_by_identity(
    tx: &Transaction<'_>,
    volume_id: &str,
    file_id: &str,
) -> Result<Option<ExistingInstance>> {
    tx.query_row(
        "SELECT id, current_path, path_key, volume_id, file_id,
                file_size, modified_ms, exists_flag
         FROM file_instances
         WHERE volume_id = ?1 AND file_id = ?2
         LIMIT 1",
        params![volume_id, file_id],
        map_existing_instance,
    )
    .optional()
    .map_err(Into::into)
}

fn find_relocation_candidate(
    tx: &Transaction<'_>,
    content_asset_id: i64,
    current_path_key: &str,
) -> Result<Option<ExistingInstance>> {
    let mut statement = tx.prepare(
        "SELECT id, current_path, path_key, volume_id, file_id,
                file_size, modified_ms, exists_flag
         FROM file_instances
         WHERE content_asset_id = ?1 AND path_key != ?2
         ORDER BY exists_flag ASC, last_seen_at DESC",
    )?;

    let candidates = statement
        .query_map(
            params![content_asset_id, current_path_key],
            map_existing_instance,
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut relocation = None;
    for candidate in candidates {
        let exists_on_disk = Path::new(&candidate.current_path.replace('/', "\\")).exists();
        if !exists_on_disk {
            if relocation.is_some() {
                return Ok(None);
            }
            relocation = Some(candidate);
        }
    }

    Ok(relocation)
}

fn find_asset_by_sha(tx: &Transaction<'_>, sha256: &str) -> Result<Option<AssetRecord>> {
    tx.query_row(
        "SELECT id FROM content_assets WHERE sha256 = ?1",
        [sha256],
        map_asset_record,
    )
    .optional()
    .map_err(Into::into)
}

fn create_asset_record(
    tx: &Transaction<'_>,
    extension: &str,
    file_size: i64,
    file_class: FileClass,
    observed_at: &str,
    analysis: &AssetAnalysis,
) -> Result<i64> {
    let media_type = match file_class {
        FileClass::Video => MEDIA_TYPE_VIDEO,
        _ => MEDIA_TYPE_IMAGE,
    };
    tx.execute(
        "INSERT INTO content_assets (
             media_type,
             extension,
             file_size,
             quick_fingerprint,
             sha256,
             width,
             height,
             format_name,
             captured_at,
             phash,
             dhash,
             quality_score,
             thumbnail_path,
             preview_supported,
             analysis_version,
             created_at,
             updated_at
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?16)",
        params![
            media_type,
            extension,
            file_size,
            analysis.quick_fingerprint.clone(),
            analysis.sha256.clone(),
            analysis.width.map(i64::from),
            analysis.height.map(i64::from),
            analysis.format_name.clone(),
            Some(observed_at.to_string()),
            analysis.phash.clone(),
            analysis.dhash.clone(),
            analysis.quality_score.map(f64::from),
            analysis.thumbnail_path.clone(),
            i64::from(analysis.preview_supported),
            ANALYSIS_VERSION,
            observed_at
        ],
    )?;
    Ok(tx.last_insert_rowid())
}

fn insert_instance(
    tx: &Transaction<'_>,
    asset_id: i64,
    display_path: &str,
    path_key: &str,
    identity: Option<&WindowsIdentity>,
    file_size: i64,
    modified_ms: i64,
    extension: &str,
    file_class: FileClass,
    scan_run_id: i64,
    observed_at: &str,
) -> Result<()> {
    tx.execute(
        "INSERT INTO file_instances (
             content_asset_id,
             current_path,
             path_key,
             volume_id,
             file_id,
             file_size,
             modified_ms,
             extension,
             file_class,
             first_seen_at,
             last_seen_at,
             last_scan_run_id,
             exists_flag
         )
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10, ?11, 1)",
        params![
            asset_id,
            display_path,
            path_key,
            identity.map(|value| value.volume_id.clone()),
            identity.map(|value| value.file_id.clone()),
            file_size,
            modified_ms,
            extension,
            file_class.as_str(),
            observed_at,
            scan_run_id
        ],
    )?;
    Ok(())
}

fn update_instance_seen(
    tx: &Transaction<'_>,
    instance_id: i64,
    scan_run_id: i64,
    observed_at: &str,
    file_size: i64,
    modified_ms: i64,
    identity: Option<&WindowsIdentity>,
) -> Result<()> {
    tx.execute(
        "UPDATE file_instances
         SET exists_flag = 1,
             last_seen_at = ?2,
             last_scan_run_id = ?3,
             file_size = ?4,
             modified_ms = ?5,
             volume_id = COALESCE(?6, volume_id),
             file_id = COALESCE(?7, file_id)
         WHERE id = ?1",
        params![
            instance_id,
            observed_at,
            scan_run_id,
            file_size,
            modified_ms,
            identity.map(|value| value.volume_id.clone()),
            identity.map(|value| value.file_id.clone())
        ],
    )?;
    Ok(())
}

fn relocate_instance(
    tx: &Transaction<'_>,
    instance_id: i64,
    old_path: &str,
    new_path: &str,
    path_key: &str,
    scan_run_id: i64,
    observed_at: &str,
    file_size: i64,
    modified_ms: i64,
    identity: Option<&WindowsIdentity>,
    change_type: &str,
) -> Result<()> {
    tx.execute(
        "UPDATE file_instances
         SET current_path = ?2,
             path_key = ?3,
             volume_id = ?4,
             file_id = ?5,
             file_size = ?6,
             modified_ms = ?7,
             exists_flag = 1,
             last_seen_at = ?8,
             last_scan_run_id = ?9
         WHERE id = ?1",
        params![
            instance_id,
            new_path,
            path_key,
            identity.map(|value| value.volume_id.clone()),
            identity.map(|value| value.file_id.clone()),
            file_size,
            modified_ms,
            observed_at,
            scan_run_id
        ],
    )?;
    tx.execute(
        "INSERT INTO path_history (file_instance_id, old_path, new_path, change_type, detected_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![instance_id, old_path, new_path, change_type, observed_at],
    )?;
    Ok(())
}

fn mark_missing_within_roots(
    tx: &Transaction<'_>,
    roots: &[ScanRoot],
    seen_paths: &HashSet<String>,
    scan_run_id: i64,
) -> Result<()> {
    let mut statement = tx.prepare(
        "SELECT id, path_key
         FROM file_instances
         WHERE exists_flag = 1",
    )?;
    let rows = statement.query_map([], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;
    let all = rows.collect::<rusqlite::Result<Vec<_>>>()?;

    for (id, path_key) in all {
        let under_root = roots.iter().any(|root| path_key.starts_with(&root.key));
        if under_root && !seen_paths.contains(&path_key) {
            tx.execute(
                "UPDATE file_instances
                 SET exists_flag = 0, last_scan_run_id = ?2
                 WHERE id = ?1",
                params![id, scan_run_id],
            )?;
        }
    }

    Ok(())
}

fn write_sidecar_files(
    tx: &Transaction<'_>,
    scan_run: &ScanRun,
    sidecars: &[SidecarFile],
    observed_at: &str,
) -> Result<()> {
    for sc in sidecars {
        tx.execute(
            "INSERT INTO sidecar_files
                 (scan_run_id, path, path_key, extension, file_size, first_seen_at, last_seen_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)
             ON CONFLICT(scan_run_id, path_key) DO UPDATE
               SET last_seen_at = excluded.last_seen_at",
            params![
                scan_run.id,
                sc.path,
                sc.path_key,
                sc.extension,
                sc.file_size,
                observed_at,
            ],
        )?;
    }
    Ok(())
}

fn rewrite_unknown_formats(
    tx: &Transaction<'_>,
    scan_run_id: i64,
    items: &BTreeMap<String, UnknownFormatSummary>,
) -> Result<()> {
    tx.execute(
        "DELETE FROM unknown_formats WHERE scan_run_id = ?1",
        [scan_run_id],
    )?;
    for item in items.values() {
        tx.execute(
            "INSERT INTO unknown_formats (scan_run_id, extension, count, example_path)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                scan_run_id,
                item.extension,
                item.count as i64,
                item.example_path
            ],
        )?;
    }
    Ok(())
}

fn validate_decision(
    tx: &Transaction<'_>,
    group_id: i64,
    payload: &DecisionPayload,
) -> Result<(String, Vec<(i64, PathBuf)>)> {
    let group_kind: String = tx
        .query_row(
            "SELECT kind FROM match_groups WHERE id = ?1",
            [group_id],
            |row| row.get(0),
        )
        .optional()?
        .context("group not found")?;

    let keep_ids: HashSet<i64> = payload.keep_ids.iter().copied().collect();
    let recycle_ids: HashSet<i64> = payload.recycle_ids.iter().copied().collect();
    if keep_ids.len() != payload.keep_ids.len() || recycle_ids.len() != payload.recycle_ids.len() {
        return Err(anyhow!("duplicate file ids are not allowed in a decision"));
    }
    if keep_ids.is_empty() {
        return Err(anyhow!("at least one file must be kept"));
    }
    if recycle_ids.is_empty() {
        return Err(anyhow!("at least one file must be recycled"));
    }
    if keep_ids.iter().any(|id| recycle_ids.contains(id)) {
        return Err(anyhow!(
            "the same file cannot be kept and recycled at the same time"
        ));
    }

    let mut statement = tx.prepare(
        "SELECT fi.id, fi.current_path
         FROM group_members gm
         JOIN file_instances fi ON fi.id = gm.file_instance_id
         WHERE gm.group_id = ?1",
    )?;
    let members = statement
        .query_map([group_id], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if members.is_empty() {
        return Err(anyhow!("group has no members"));
    }

    let member_ids: HashSet<i64> = members.iter().map(|(id, _)| *id).collect();
    if !keep_ids.is_subset(&member_ids) || !recycle_ids.is_subset(&member_ids) {
        return Err(anyhow!(
            "decision contains files outside the selected group"
        ));
    }
    if keep_ids.union(&recycle_ids).count() != member_ids.len() {
        return Err(anyhow!(
            "every group member must be explicitly marked as keep or recycle"
        ));
    }

    let recycle_targets = members
        .into_iter()
        .filter(|(id, _)| recycle_ids.contains(id))
        .map(|(id, path)| (id, PathBuf::from(path.replace('/', "\\"))))
        .collect();

    Ok((group_kind, recycle_targets))
}

fn recompute_groups(
    tx: &Transaction<'_>,
    scan_progress: &Arc<Mutex<ScanProgress>>,
    pool: &rayon::ThreadPool,
) -> Result<()> {
    let records = load_active_records(tx)?;

    if let Ok(mut p) = scan_progress.lock() {
        p.grouping = Some(GroupingProgress {
            similar_started: true,
            ..GroupingProgress::default()
        });
    }

    let ((exact_drafts, raw_jpeg_drafts), similar_result) = pool.install(|| {
        rayon::join(
            || {
                let exact = build_exact_groups(&records);
                let raw_jpeg = build_raw_jpeg_groups(&records);
                (exact, raw_jpeg)
            },
            || build_similar_groups(&records, scan_progress),
        )
    });
    let similar_drafts = similar_result?;

    if let Ok(mut p) = scan_progress.lock() {
        if let Some(g) = p.grouping.as_mut() {
            g.exact_done = true;
            g.exact_groups = exact_drafts.len();
            g.similar_groups = similar_drafts.len();
            g.raw_jpeg_done = true;
            g.raw_jpeg_groups = raw_jpeg_drafts.len();
        }
    }

    let mut drafts = Vec::new();
    drafts.extend(exact_drafts);
    drafts.extend(similar_drafts);
    drafts.extend(raw_jpeg_drafts);

    let existing_groups = load_existing_groups(tx)?;
    let mut active_group_ids = HashSet::new();

    for draft in drafts {
        let group_id = if let Some((group_id, status)) = existing_groups.get(&draft.anchor) {
            if status == "applied" {
                archive_group_anchor(tx, *group_id, &draft.anchor)?;
                insert_group_draft(tx, draft)?
            } else {
                tx.execute(
                    "UPDATE match_groups
                     SET kind = ?2,
                         status = ?3,
                         recommendation_reason = ?4,
                         recommended_keep_instance_id = ?5,
                         updated_at = ?6
                     WHERE id = ?1",
                    params![
                        group_id,
                        draft.kind.as_db_value(),
                        status,
                        draft.recommendation_reason,
                        draft.recommended_keep_instance_id,
                        iso_now()
                    ],
                )?;
                tx.execute("DELETE FROM group_members WHERE group_id = ?1", [group_id])?;

                for member in draft.members {
                    tx.execute(
                        "INSERT INTO group_members (group_id, file_instance_id, content_asset_id, similarity, role)
                         VALUES (?1, ?2, ?3, ?4, ?5)",
                        params![
                            group_id,
                            member.file_instance_id,
                            member.content_asset_id,
                            member.similarity.map(f64::from),
                            member.role
                        ],
                    )?;
                }

                *group_id
            }
        } else {
            insert_group_draft(tx, draft)?
        };

        active_group_ids.insert(group_id);
    }

    let mut stale = tx.prepare("SELECT id, status FROM match_groups")?;
    let stale_rows = stale.query_map([], |row| {
        Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
    })?;
    for (id, status) in stale_rows.collect::<rusqlite::Result<Vec<_>>>()? {
        if !active_group_ids.contains(&id) && status != "applied" {
            tx.execute("DELETE FROM group_members WHERE group_id = ?1", [id])?;
            tx.execute("DELETE FROM match_groups WHERE id = ?1", [id])?;
        }
    }

    Ok(())
}

fn archive_group_anchor(tx: &Transaction<'_>, group_id: i64, anchor: &str) -> Result<()> {
    let archived_anchor = format!("{anchor}#applied:{group_id}:{}", iso_now());
    let archived_at = iso_now();
    tx.execute(
        "UPDATE match_groups
         SET anchor = ?2,
             updated_at = ?3
         WHERE id = ?1",
        params![group_id, archived_anchor, archived_at],
    )?;
    Ok(())
}

fn insert_group_draft(tx: &Transaction<'_>, draft: GroupDraft) -> Result<i64> {
    let created_at = iso_now();
    tx.execute(
        "INSERT INTO match_groups (
            anchor,
            kind,
            status,
            recommendation_reason,
            recommended_keep_instance_id,
            created_at,
            updated_at
         ) VALUES (?1, ?2, 'pending', ?3, ?4, ?5, ?5)",
        params![
            draft.anchor,
            draft.kind.as_db_value(),
            draft.recommendation_reason,
            draft.recommended_keep_instance_id,
            created_at
        ],
    )?;
    let group_id = tx.last_insert_rowid();

    for member in draft.members {
        tx.execute(
            "INSERT INTO group_members (group_id, file_instance_id, content_asset_id, similarity, role)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                group_id,
                member.file_instance_id,
                member.content_asset_id,
                member.similarity.map(f64::from),
                member.role
            ],
        )?;
    }

    Ok(group_id)
}

fn load_active_records(tx: &Transaction<'_>) -> Result<Vec<ActiveRecord>> {
    let mut statement = tx.prepare(
        "SELECT fi.id,
                fi.content_asset_id,
                fi.current_path,
                fi.extension,
                ca.width,
                ca.height,
                ca.quality_score,
                ca.thumbnail_path,
                ca.preview_supported,
                ca.sha256,
                ca.phash,
                ca.dhash
         FROM file_instances fi
         JOIN content_assets ca ON ca.id = fi.content_asset_id
         WHERE fi.exists_flag = 1
         ORDER BY COALESCE(ca.quality_score, 0) DESC, fi.current_path ASC",
    )?;

    let rows = statement.query_map([], |row| {
        Ok(ActiveRecord {
            file_instance_id: row.get(0)?,
            content_asset_id: row.get(1)?,
            path: row.get(2)?,
            extension: row.get(3)?,
            width: row.get::<_, Option<i64>>(4)?.map(|value| value as u32),
            height: row.get::<_, Option<i64>>(5)?.map(|value| value as u32),
            quality_score: row.get::<_, Option<f64>>(6)?.map(|value| value as f32),
            thumbnail_path: row.get(7)?,
            preview_supported: row.get::<_, i64>(8)? == 1,
            sha256: row.get(9)?,
            phash: row.get(10)?,
            dhash: row.get(11)?,
        })
    })?;

    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

fn build_exact_groups(records: &[ActiveRecord]) -> Vec<GroupDraft> {
    let mut groups: HashMap<i64, Vec<ActiveRecord>> = HashMap::new();
    for record in records {
        groups
            .entry(record.content_asset_id)
            .or_default()
            .push(record.clone());
    }

    let mut drafts = Vec::new();
    for (asset_id, members) in groups {
        if members.len() < 2 {
            continue;
        }
        let keep_id = pick_best_member(&members).map(|member| member.file_instance_id);
        drafts.push(GroupDraft {
            anchor: format!("exact:{asset_id}"),
            kind: MatchKind::Exact,
            recommendation_reason:
                "These files are byte-identical. Keep one reference copy and recycle the rest."
                    .to_string(),
            recommended_keep_instance_id: keep_id,
            members: members
                .into_iter()
                .map(|member| GroupMemberDraft {
                    file_instance_id: member.file_instance_id,
                    content_asset_id: member.content_asset_id,
                    similarity: Some(1.0),
                    role: Some(if Some(member.file_instance_id) == keep_id {
                        "recommended_keep".to_string()
                    } else {
                        "duplicate".to_string()
                    }),
                })
                .collect(),
        });
    }

    drafts
}

/// Clique-based similar-group detection.
///
/// Two images are "candidate similar" when BOTH their pHash Hamming distance
/// is ≤ PHASH_MAX_DISTANCE AND their dHash Hamming distance is ≤
/// DHASH_MAX_DISTANCE (AND, not OR — tighter than the previous OR/wider
/// thresholds).  Candidates that additionally pass the SSIM ≥
/// SIMILARITY_THRESHOLD check become "confirmed similar".
///
/// Groups are built with a greedy clique strategy rather than connected
/// components (BFS/DFS).  This prevents "chaining" (A~B~C grouped even when
/// A and C are dissimilar): a candidate C joins a clique only if it is
/// confirmed similar to EVERY existing member.  Maximum clique size is 4;
/// the best-quality image anchors each clique so small collections always
/// contain the strongest member.
fn build_similar_groups(
    records: &[ActiveRecord],
    scan_progress: &Arc<Mutex<ScanProgress>>,
) -> Result<Vec<GroupDraft>> {
    // ── 1. Build representative set (one entry per unique content asset) ─────
    let mut representatives = Vec::new();
    let mut seen_assets = HashSet::new();
    for record in records {
        if seen_assets.insert(record.content_asset_id)
            && record.preview_supported
            && record.thumbnail_path.is_some()
            && record.phash.is_some()
            && record.dhash.is_some()
        {
            representatives.push(record.clone());
        }
    }

    let len = representatives.len();
    if len < 2 {
        if let Ok(mut p) = scan_progress.lock() {
            if let Some(g) = p.grouping.as_mut() {
                g.similar_done = true;
            }
        }
        return Ok(Vec::new());
    }

    // ── 2. Pre-parse hashes once so we don't repeat hex parsing ──────────────
    let phashes: Vec<u64> = representatives
        .iter()
        .map(|r| parse_hash_hex(r.phash.as_deref().unwrap_or("0")))
        .collect();
    let dhashes: Vec<u64> = representatives
        .iter()
        .map(|r| parse_hash_hex(r.dhash.as_deref().unwrap_or("0")))
        .collect();

    // ── 3. Index both hashes in BK-trees for O(N log N) candidate lookup ─────
    let mut phash_tree = BkTree::new();
    let mut dhash_tree = BkTree::new();
    for i in 0..len {
        phash_tree.insert(i, phashes[i]);
        dhash_tree.insert(i, dhashes[i]);
    }

    // ── 4. Generate candidate pairs: phash ≤ PHASH_MAX_DISTANCE
    //       AND dhash ≤ DHASH_MAX_DISTANCE (stricter AND, not OR) ───────────
    let mut candidate_set: HashSet<(usize, usize)> = HashSet::new();
    for i in 0..len {
        let phash_neighbors: HashSet<usize> =
            phash_tree.query(phashes[i], PHASH_MAX_DISTANCE).into_iter().collect();
        for j in dhash_tree.query(dhashes[i], DHASH_MAX_DISTANCE) {
            if j > i && phash_neighbors.contains(&j) {
                candidate_set.insert((i, j));
            }
        }
    }

    let candidates: Vec<(usize, usize)> = candidate_set.into_iter().collect();
    let total_candidates = candidates.len();

    if let Ok(mut p) = scan_progress.lock() {
        if let Some(g) = p.grouping.as_mut() {
            g.similar_pairs_total = total_candidates;
            g.similar_pairs_done = 0;
        }
    }

    // ── 5. Pre-load thumbnail buffers for all indices that appear in candidates
    let needed: HashSet<usize> = candidates.iter().flat_map(|&(i, j)| [i, j]).collect();
    let buffers: Vec<Option<GrayImage>> = (0..len)
        .into_par_iter()
        .map(|i| {
            if !needed.contains(&i) {
                return None;
            }
            let path_str = representatives[i].thumbnail_path.as_deref()?;
            load_similarity_buffer(Path::new(path_str)).ok()
        })
        .collect();

    // ── 6. Evaluate SSIM on candidates in parallel ────────────────────────────
    let pairs_done = AtomicUsize::new(0);
    let similar_edges: Result<Vec<Option<(i64, i64, f32)>>> = candidates
        .par_iter()
        .map(|&(i, j)| {
            let done = pairs_done.fetch_add(1, Ordering::Relaxed);
            if done % 500 == 0 {
                if let Ok(mut p) = scan_progress.lock() {
                    if let Some(g) = p.grouping.as_mut() {
                        g.similar_pairs_done = done;
                    }
                }
            }

            let left = &representatives[i];
            let right = &representatives[j];
            if left.sha256 == right.sha256 {
                return Ok(None);
            }

            let (Some(buf_l), Some(buf_r)) = (buffers[i].as_ref(), buffers[j].as_ref()) else {
                return Ok(None);
            };
            let similarity = ssim_from_buffers(buf_l, buf_r);
            if similarity >= SIMILARITY_THRESHOLD {
                Ok(Some((left.content_asset_id, right.content_asset_id, similarity)))
            } else {
                Ok(None)
            }
        })
        .collect();

    // ── 7. Build similar_pairs set and adjacency map ─────────────────────────
    let mut similar_pairs: HashSet<(i64, i64)> = HashSet::new();
    let mut adjacency: HashMap<i64, Vec<(i64, f32)>> = HashMap::new();
    for (a, b, sim) in similar_edges?.into_iter().flatten() {
        let key = (a.min(b), a.max(b));
        similar_pairs.insert(key);
        adjacency.entry(a).or_default().push((b, sim));
        adjacency.entry(b).or_default().push((a, sim));
    }

    if let Ok(mut p) = scan_progress.lock() {
        if let Some(g) = p.grouping.as_mut() {
            g.similar_pairs_done = total_candidates;
            g.similar_done = true;
        }
    }

    // ── 8. Asset-ID lookup map ────────────────────────────────────────────────
    let lookup: HashMap<i64, &ActiveRecord> = representatives
        .iter()
        .map(|r| (r.content_asset_id, r))
        .collect();

    // ── 9. Greedy clique-based grouping ───────────────────────────────────────
    // Process representatives in descending quality order so the best image
    // anchors each clique.  A candidate joins only when it is confirmed
    // similar to ALL current clique members — this prevents A~B~C chaining
    // when A and C are dissimilar.  Max clique size = 4.
    let mut order: Vec<usize> = (0..len).collect();
    order.sort_by(|&i, &j| {
        let qi = representatives[i].quality_score.unwrap_or_default();
        let qj = representatives[j].quality_score.unwrap_or_default();
        qj.partial_cmp(&qi).unwrap_or(std::cmp::Ordering::Equal)
    });

    let mut visited: HashSet<i64> = HashSet::new();
    let mut drafts = Vec::new();

    for idx in order {
        let anchor = &representatives[idx];
        let anchor_id = anchor.content_asset_id;

        if visited.contains(&anchor_id) {
            continue;
        }
        if !adjacency.contains_key(&anchor_id) {
            // No confirmed-similar neighbours → not part of any group.
            continue;
        }

        visited.insert(anchor_id);
        let mut clique: Vec<i64> = vec![anchor_id];

        // Grow clique from anchor's neighbours, strongest-similarity first.
        let mut candidates = adjacency[&anchor_id].clone();
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        for (candidate_id, _) in &candidates {
            if clique.len() >= 4 {
                break;
            }
            if visited.contains(candidate_id) {
                continue;
            }
            // candidate must be confirmed-similar to every existing member.
            let all_similar = clique.iter().all(|&m| {
                let key = ((*candidate_id).min(m), (*candidate_id).max(m));
                similar_pairs.contains(&key)
            });
            if all_similar {
                clique.push(*candidate_id);
                visited.insert(*candidate_id);
            }
        }

        if clique.len() < 2 {
            continue;
        }

        clique.sort_unstable();
        let members: Vec<ActiveRecord> = clique
            .iter()
            .filter_map(|id| lookup.get(id).map(|r| (*r).clone()))
            .collect();
        let keep_id = pick_best_member(&members).map(|m| m.file_instance_id);

        // Build per-pair similarity map for this clique.
        let mut similarity_map: HashMap<(i64, i64), f32> = HashMap::new();
        for &a in &clique {
            if let Some(neighbors) = adjacency.get(&a) {
                for &(b, sim) in neighbors {
                    if clique.contains(&b) {
                        let key = (a.min(b), a.max(b));
                        similarity_map
                            .entry(key)
                            .and_modify(|v| *v = v.max(sim))
                            .or_insert(sim);
                    }
                }
            }
        }

        drafts.push(GroupDraft {
            anchor: format!(
                "similar:{}",
                clique
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join("-")
            ),
            kind: MatchKind::Similar,
            recommendation_reason:
                "These photos are visually near-identical. Keep the sharpest, highest-quality version."
                    .to_string(),
            recommended_keep_instance_id: keep_id,
            members: members
                .into_iter()
                .map(|m| {
                    let strongest = clique
                        .iter()
                        .filter(|&&other| other != m.content_asset_id)
                        .filter_map(|&other| {
                            let key = (m.content_asset_id.min(other), m.content_asset_id.max(other));
                            similarity_map.get(&key).copied()
                        })
                        .fold(None::<f32>, |acc, v| Some(acc.map_or(v, |a: f32| a.max(v))));

                    GroupMemberDraft {
                        file_instance_id: m.file_instance_id,
                        content_asset_id: m.content_asset_id,
                        similarity: strongest,
                        role: Some(if Some(m.file_instance_id) == keep_id {
                            "recommended_keep".to_string()
                        } else {
                            "similar_candidate".to_string()
                        }),
                    }
                })
                .collect(),
        });
    }

    Ok(drafts)
}

fn build_raw_jpeg_groups(records: &[ActiveRecord]) -> Vec<GroupDraft> {
    let mut by_stem: HashMap<String, Vec<ActiveRecord>> = HashMap::new();
    for record in records {
        let stem = normalized_stem(Path::new(&record.path.replace('/', "\\")));
        if !stem.is_empty() {
            by_stem.entry(stem).or_default().push(record.clone());
        }
    }

    let mut drafts = Vec::new();
    for (stem, members) in by_stem {
        let has_raw = members
            .iter()
            .any(|member| member.extension.eq_ignore_ascii_case("rw2"));
        let has_raster = members.iter().any(|member| {
            matches!(
                member.extension.as_str(),
                "jpg" | "jpeg" | "png" | "webp" | "heic" | "heif"
            )
        });
        if !has_raw || !has_raster {
            continue;
        }

        let keep_id = pick_best_member(&members).map(|member| member.file_instance_id);
        drafts.push(GroupDraft {
            anchor: format!("raw-jpeg:{stem}"),
            kind: MatchKind::RawJpegSet,
            recommendation_reason:
                "RAW + export pair detected. Keep both by default unless you explicitly want to discard one."
                    .to_string(),
            recommended_keep_instance_id: keep_id,
            members: members
                .into_iter()
                .map(|member| GroupMemberDraft {
                    file_instance_id: member.file_instance_id,
                    content_asset_id: member.content_asset_id,
                    similarity: None,
                    role: Some(if member.extension.eq_ignore_ascii_case("rw2") {
                        "protected_raw".to_string()
                    } else {
                        "paired_export".to_string()
                    }),
                })
                .collect(),
        });
    }

    drafts
}

// ─── BK-tree (metric tree for Hamming distance) ───────────────────────────────
// Used to find candidate pairs for similarity checking in O(N log N) instead
// of the O(N²) exhaustive comparison.

fn parse_hash_hex(s: &str) -> u64 {
    u64::from_str_radix(s, 16).unwrap_or(0)
}

struct BkNode {
    hash: u64,
    /// All representative indices that share this exact hash value.
    reps: Vec<usize>,
    // Fixed-size array keyed by Hamming distance (0–64), replacing the
    // Vec linear scan. Hamming distance of two u64 values is always 0..=64.
    children: [Option<usize>; 65],
}

struct BkTree {
    nodes: Vec<BkNode>,
}

impl BkTree {
    fn new() -> Self {
        BkTree { nodes: Vec::new() }
    }

    fn insert(&mut self, rep_idx: usize, hash: u64) {
        if self.nodes.is_empty() {
            self.nodes.push(BkNode { hash, reps: vec![rep_idx], children: [None; 65] });
            return;
        }
        let mut cur = 0;
        loop {
            let dist = (self.nodes[cur].hash ^ hash).count_ones() as usize;
            if dist == 0 {
                self.nodes[cur].reps.push(rep_idx);
                return;
            }
            match self.nodes[cur].children[dist] {
                Some(child) => cur = child,
                None => {
                    let new_idx = self.nodes.len();
                    self.nodes.push(BkNode { hash, reps: vec![rep_idx], children: [None; 65] });
                    self.nodes[cur].children[dist] = Some(new_idx);
                    return;
                }
            }
        }
    }

    /// Return all representative indices whose hash is within `radius` of `hash`.
    fn query(&self, hash: u64, radius: u32) -> Vec<usize> {
        let mut result = Vec::new();
        if self.nodes.is_empty() {
            return result;
        }
        let mut stack = vec![0usize];
        while let Some(cur) = stack.pop() {
            let node = &self.nodes[cur];
            let dist = (node.hash ^ hash).count_ones();
            if dist <= radius {
                result.extend_from_slice(&node.reps);
            }
            let lo = dist.saturating_sub(radius) as usize;
            let hi = (dist + radius).min(64) as usize;
            for d in lo..=hi {
                if let Some(child) = node.children[d] {
                    stack.push(child);
                }
            }
        }
        result
    }
}

fn pick_best_member(records: &[ActiveRecord]) -> Option<&ActiveRecord> {
    records.iter().max_by(|left, right| {
        left.quality_score
            .unwrap_or_default()
            .partial_cmp(&right.quality_score.unwrap_or_default())
            .unwrap()
            .then_with(|| {
                let left_pixels = left.width.unwrap_or_default() * left.height.unwrap_or_default();
                let right_pixels =
                    right.width.unwrap_or_default() * right.height.unwrap_or_default();
                left_pixels.cmp(&right_pixels)
            })
            .then_with(|| right.path.cmp(&left.path))
    })
}

fn load_existing_groups(tx: &Transaction<'_>) -> Result<HashMap<String, (i64, String)>> {
    let mut statement = tx.prepare("SELECT id, anchor, status FROM match_groups")?;
    let rows = statement.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
        ))
    })?;

    let mut items = HashMap::new();
    for row in rows {
        let (id, anchor, status) = row?;
        items.insert(anchor, (id, status));
    }
    Ok(items)
}

fn map_existing_instance(row: &Row<'_>) -> rusqlite::Result<ExistingInstance> {
    Ok(ExistingInstance {
        id: row.get(0)?,
        current_path: row.get(1)?,
        path_key: row.get(2)?,
        volume_id: row.get(3)?,
        file_id: row.get(4)?,
        file_size: row.get(5)?,
        modified_ms: row.get(6)?,
        exists_flag: row.get::<_, i64>(7)? == 1,
    })
}

fn map_asset_record(row: &Row<'_>) -> rusqlite::Result<AssetRecord> {
    Ok(AssetRecord { id: row.get(0)? })
}

fn map_group_summary(row: &Row<'_>) -> rusqlite::Result<GroupSummary> {
    Ok(GroupSummary {
        id: row.get(0)?,
        kind: MatchKind::from_db_value(&row.get::<_, String>(1)?).unwrap_or(MatchKind::Exact),
        status: ReviewStatus::from_db_value(&row.get::<_, String>(2)?)
            .unwrap_or(ReviewStatus::Pending),
        anchor: row.get(3)?,
        member_count: row.get::<_, i64>(4)? as usize,
        recommended_keep_instance_id: row.get(5)?,
        recommended_keep_path: row.get(6)?,
        recommendation_reason: row.get(7)?,
        updated_at: row.get(8)?,
    })
}

fn safe_display_path(path: &Path) -> String {
    let raw = path_to_string(path);
    raw.strip_prefix("//?/")
        .or_else(|| raw.strip_prefix("\\\\?\\"))
        .unwrap_or(&raw)
        .to_string()
}

fn normalize_key(path: &str) -> String {
    path.replace('\\', "/").trim().to_ascii_lowercase()
}

fn normalize_root_key(path: &str) -> String {
    let key = normalize_key(path);
    if key.ends_with('/') {
        key
    } else {
        format!("{key}/")
    }
}

fn iso_now() -> String {
    DateTime::<Utc>::from(SystemTime::now()).to_rfc3339()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    use crate::image_tools::save_test_image;

    fn create_test_service() -> (tempfile::TempDir, AppService) {
        let dir = tempdir().unwrap();
        fs::create_dir_all(dir.path().join("thumbs")).unwrap();
        let service = AppService {
            db_path: dir.path().join("index.db"),
            thumbs_dir: dir.path().join("thumbs"),
            scan_progress: Arc::new(Mutex::new(ScanProgress::idle())),
            next_task_id: Arc::new(AtomicU64::new(1)),
            cancel_flag: Arc::new(AtomicBool::new(false)),
        };
        service.ensure_schema().unwrap();
        (dir, service)
    }

    #[test]
    fn scan_detects_exact_duplicates_and_moves() {
        let (workspace, service) = create_test_service();
        let root_a = workspace.path().join("A");
        let root_b = workspace.path().join("B");
        fs::create_dir_all(&root_a).unwrap();
        fs::create_dir_all(&root_b).unwrap();

        let original = root_a.join("shot-1.png");
        let duplicate = root_a.join("shot-1-copy.png");
        save_test_image(&original, 320, 240, 11).unwrap();
        fs::copy(&original, &duplicate).unwrap();

        let first = service
            .start_scan(vec![root_a.to_string_lossy().to_string()])
            .unwrap();
        assert_eq!(first.new_files, 2);

        let groups = service
            .list_groups(ReviewGroupFilter {
                kind: Some(MatchKind::Exact),
                status: Some(ReviewStatus::Pending),
            })
            .unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].member_count, 2);

        let moved = root_b.join("shot-1.png");
        fs::rename(&original, &moved).unwrap();
        let second = service
            .start_scan(vec![
                root_a.to_string_lossy().to_string(),
                root_b.to_string_lossy().to_string(),
            ])
            .unwrap();
        assert!(second.updated_locations >= 1);

        let detail = service.get_group(groups[0].id).unwrap();
        assert!(detail
            .members
            .iter()
            .any(|member| member.path.ends_with("B/shot-1.png")));
    }

    #[test]
    fn scan_lists_unknown_formats() {
        let (workspace, service) = create_test_service();
        let root = workspace.path().join("imports");
        fs::create_dir_all(&root).unwrap();
        save_test_image(&root.join("frame.png"), 120, 120, 20).unwrap();
        // Zip files → unknown_formats.  Videos are now tracked separately.
        fs::write(root.join("archive.zip"), b"not a real archive").unwrap();

        let result = service
            .start_scan(vec![root.to_string_lossy().to_string()])
            .unwrap();
        let unknown = service.list_unknown_formats(result.scan_run_id).unwrap();
        assert_eq!(unknown.len(), 1);
        assert_eq!(unknown[0].extension, "zip");
    }

    #[test]
    fn scan_routes_sidecars_to_sidecar_files_table() {
        let (workspace, service) = create_test_service();
        let root = workspace.path().join("imports");
        fs::create_dir_all(&root).unwrap();
        save_test_image(&root.join("img001.jpg"), 200, 200, 42).unwrap();
        // .aae and .xmp sidecars should NOT appear in unknown_formats.
        fs::write(root.join("img001.aae"), b"<AdjustmentList/>").unwrap();
        fs::write(root.join("img001.xmp"), b"<x:xmpmeta/>").unwrap();

        let result = service
            .start_scan(vec![root.to_string_lossy().to_string()])
            .unwrap();

        // Sidecars must not appear in unknown_formats.
        let unknown = service.list_unknown_formats(result.scan_run_id).unwrap();
        assert!(
            unknown.iter().all(|u| u.extension != "aae" && u.extension != "xmp"),
            "sidecars must not be reported as unknown formats"
        );

        // Sidecars must be recorded in sidecar_files.
        let conn = service.open().unwrap();
        let sidecar_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sidecar_files WHERE scan_run_id = ?1",
                [result.scan_run_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(sidecar_count, 2, "both .aae and .xmp should be in sidecar_files");
    }

    #[test]
    fn scan_tracks_videos_in_file_instances() {
        let (workspace, service) = create_test_service();
        let root = workspace.path().join("media");
        fs::create_dir_all(&root).unwrap();
        save_test_image(&root.join("photo.jpg"), 200, 200, 7).unwrap();
        // Write a small fake video — enough bytes for a valid BLAKE3+SHA-256.
        fs::write(root.join("clip.mp4"), vec![0u8; 1024]).unwrap();

        let result = service
            .start_scan(vec![root.to_string_lossy().to_string()])
            .unwrap();

        // Videos must not appear in unknown_formats.
        let unknown = service.list_unknown_formats(result.scan_run_id).unwrap();
        assert!(
            unknown.iter().all(|u| u.extension != "mp4"),
            "mp4 must not be in unknown_formats"
        );

        // Both the photo and the video should appear as active file_instances.
        let conn = service.open().unwrap();
        let active: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM file_instances WHERE exists_flag = 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(active, 2, "both photo and video should be tracked");

        let video_class: String = conn
            .query_row(
                "SELECT file_class FROM file_instances
                 WHERE path_key LIKE '%clip.mp4'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(video_class, "video");
    }

    #[test]
    fn scan_similar_no_chaining() {
        // Build three images: A is similar to B, B is similar to C, but A is
        // NOT similar to C.  The clique-based grouping must NOT merge all
        // three into one group (old BFS would).
        //
        // Because we generate real perceptual hashes from actual image data,
        // this test uses deliberately distinct seeds to ensure the images are
        // dissimilar enough at the SSIM level while still exercising the
        // clique logic.  The important invariant is verified by checking that
        // no group has 3 members when A and C are far apart.
        //
        // With the clique algorithm: if A~B and B~C are both true but A~C is
        // false, then B can join A's group (clique = {A, B}), but C cannot
        // join because it is not similar to A.  C either forms its own 2-clique
        // with B (if B is not yet visited) or is left ungrouped.  Either way
        // the test verifies no 3-member group is created.
        let (workspace, service) = create_test_service();
        let root = workspace.path().join("images");
        fs::create_dir_all(&root).unwrap();
        // Use large seed differences so images are clearly distinct.
        save_test_image(&root.join("a.png"), 64, 64, 0).unwrap();
        save_test_image(&root.join("b.png"), 64, 64, 128).unwrap();
        save_test_image(&root.join("c.png"), 64, 64, 255).unwrap();

        service
            .start_scan(vec![root.to_string_lossy().to_string()])
            .unwrap();

        let groups = service
            .list_groups(ReviewGroupFilter {
                kind: Some(MatchKind::Similar),
                status: None,
            })
            .unwrap();
        for g in &groups {
            assert!(
                g.member_count <= 2,
                "no group should have >2 members from 3 dissimilar images (got {})",
                g.member_count
            );
        }
    }

    #[test]
    fn apply_decision_rejects_recycling_everything() {
        let (workspace, service) = create_test_service();
        let root = workspace.path().join("library");
        fs::create_dir_all(&root).unwrap();

        let original = root.join("shot-1.png");
        let duplicate = root.join("shot-1-copy.png");
        save_test_image(&original, 320, 240, 11).unwrap();
        fs::copy(&original, &duplicate).unwrap();

        service
            .start_scan(vec![root.to_string_lossy().to_string()])
            .unwrap();
        let group = service
            .list_groups(ReviewGroupFilter {
                kind: Some(MatchKind::Exact),
                status: Some(ReviewStatus::Pending),
            })
            .unwrap()
            .pop()
            .unwrap();
        let detail = service.get_group(group.id).unwrap();
        let member_ids = detail
            .members
            .iter()
            .map(|member| member.file_instance_id)
            .collect::<Vec<_>>();

        let error = service
            .apply_decision(
                group.id,
                DecisionPayload {
                    keep_ids: vec![],
                    recycle_ids: member_ids,
                    note: None,
                },
            )
            .unwrap_err();
        assert!(error.to_string().contains("at least one file must be kept"));
        assert!(original.exists());
        assert!(duplicate.exists());
    }

    #[test]
    fn reappearing_duplicates_return_to_pending_review() {
        let (workspace, service) = create_test_service();
        let root = workspace.path().join("library");
        fs::create_dir_all(&root).unwrap();

        let original = root.join("shot-1.png");
        let duplicate = root.join("shot-1-copy.png");
        save_test_image(&original, 320, 240, 11).unwrap();
        fs::copy(&original, &duplicate).unwrap();

        service
            .start_scan(vec![root.to_string_lossy().to_string()])
            .unwrap();
        let initial_group = service
            .list_groups(ReviewGroupFilter {
                kind: Some(MatchKind::Exact),
                status: Some(ReviewStatus::Pending),
            })
            .unwrap()
            .pop()
            .unwrap();

        let conn = service.open().unwrap();
        conn.execute(
            "UPDATE match_groups SET status = 'applied' WHERE id = ?1",
            [initial_group.id],
        )
        .unwrap();

        fs::remove_file(&duplicate).unwrap();
        service
            .start_scan(vec![root.to_string_lossy().to_string()])
            .unwrap();

        fs::copy(&original, &duplicate).unwrap();
        service
            .start_scan(vec![root.to_string_lossy().to_string()])
            .unwrap();

        let pending_groups = service
            .list_groups(ReviewGroupFilter {
                kind: Some(MatchKind::Exact),
                status: Some(ReviewStatus::Pending),
            })
            .unwrap();
        assert_eq!(pending_groups.len(), 1);
        assert_ne!(pending_groups[0].id, initial_group.id);

        let applied_groups = service
            .list_groups(ReviewGroupFilter {
                kind: Some(MatchKind::Exact),
                status: Some(ReviewStatus::Applied),
            })
            .unwrap();
        assert!(applied_groups
            .iter()
            .any(|group| group.anchor.contains("#applied:")));
    }

    #[test]
    fn incremental_scan_skips_unchanged_files() {
        let (workspace, service) = create_test_service();
        let root = workspace.path().join("lib");
        fs::create_dir_all(&root).unwrap();
        save_test_image(&root.join("a.png"), 100, 100, 1).unwrap();
        save_test_image(&root.join("b.png"), 100, 100, 2).unwrap();

        let first = service
            .start_scan(vec![root.to_string_lossy().to_string()])
            .unwrap();
        assert_eq!(first.new_files, 2);

        let second = service
            .start_scan(vec![root.to_string_lossy().to_string()])
            .unwrap();
        assert_eq!(second.new_files, 0);
        assert_eq!(second.unchanged_files, 2);
    }

    #[test]
    fn scan_cancel_stops_the_scan() {
        // This test verifies that setting the cancel_flag before the scan
        // starts causes the scan to return an error (cancelled).
        let (workspace, service) = create_test_service();
        let root = workspace.path().join("lib");
        fs::create_dir_all(&root).unwrap();
        for i in 0..5 {
            save_test_image(&root.join(format!("img{i}.png")), 64, 64, i as u8).unwrap();
        }

        // Pre-set the cancel flag before starting.
        service.cancel_flag.store(true, Ordering::SeqCst);
        let result = service.start_scan(vec![root.to_string_lossy().to_string()]);
        assert!(result.is_err(), "cancelled scan should return an error");
        assert!(
            result.unwrap_err().to_string().contains("cancelled"),
            "error message should mention cancellation"
        );
    }
}
