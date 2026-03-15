use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppSnapshot {
    pub pending_group_count: usize,
    pub applied_action_count: usize,
    pub indexed_asset_count: usize,
    pub active_file_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScanResult {
    pub scan_run_id: i64,
    pub started_at: String,
    pub completed_at: String,
    pub scanned_roots: Vec<String>,
    pub new_files: usize,
    pub updated_locations: usize,
    pub unchanged_files: usize,
    pub unsupported_extensions: Vec<UnknownFormatSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScanTaskStarted {
    pub task_id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScanActiveItem {
    pub file_name: String,
    pub dir_hint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScanRecentItem {
    pub file_name: String,
    pub status: String, // "new" | "updated" | "unchanged" | "failed"
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct GroupingProgress {
    pub exact_done: bool,
    pub exact_groups: usize,
    pub similar_started: bool,
    pub similar_pairs_done: usize,
    pub similar_pairs_total: usize,
    pub similar_groups: usize,
    pub similar_done: bool,
    pub raw_jpeg_done: bool,
    pub raw_jpeg_groups: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ScanProgress {
    pub task_id: Option<u64>,
    pub status: ScanTaskStatus,
    pub phase: String,
    pub message: String,

    // file-level counts
    pub total_files: usize,
    pub queued: usize,
    pub analyzing: usize,
    pub done: usize,
    pub new_files: usize,
    pub updated_files: usize,
    pub unchanged_files: usize,
    pub failed_files: usize,

    // per-file detail
    pub active_items: Vec<ScanActiveItem>,
    pub recent_items: Vec<ScanRecentItem>,

    // phase-3 breakdown
    pub grouping: Option<GroupingProgress>,

    pub started_at: Option<String>,
    pub completed_at: Option<String>,
    pub result: Option<ScanResult>,
    pub error: Option<String>,
}

impl ScanProgress {
    pub fn idle() -> Self {
        Self {
            task_id: None,
            status: ScanTaskStatus::Idle,
            phase: "idle".to_string(),
            message: "Ready to scan.".to_string(),
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
            started_at: None,
            completed_at: None,
            result: None,
            error: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScanTaskStatus {
    Idle,
    Counting,
    Running,
    Finalizing,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UnknownFormatSummary {
    pub extension: String,
    pub count: usize,
    pub example_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupSummary {
    pub id: i64,
    pub kind: MatchKind,
    pub status: ReviewStatus,
    pub anchor: String,
    pub member_count: usize,
    pub recommended_keep_instance_id: Option<i64>,
    pub recommended_keep_path: Option<String>,
    pub recommendation_reason: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupDetail {
    pub id: i64,
    pub kind: MatchKind,
    pub status: ReviewStatus,
    pub anchor: String,
    pub recommendation_reason: String,
    pub recommended_keep_instance_id: Option<i64>,
    pub members: Vec<GroupMember>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupMember {
    pub group_member_id: i64,
    pub file_instance_id: i64,
    pub content_asset_id: i64,
    pub path: String,
    pub exists_flag: bool,
    pub extension: String,
    pub format_name: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub quality_score: Option<f32>,
    pub preview_supported: bool,
    pub thumbnail_path: Option<String>,
    pub sha256: String,
    pub similarity: Option<f32>,
    pub role: Option<String>,
    pub captured_at: Option<String>,
    pub volume_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewActionSummary {
    pub id: i64,
    pub group_id: i64,
    pub group_kind: MatchKind,
    pub action_type: String,
    pub keep_instance_ids: Vec<i64>,
    pub recycle_instance_ids: Vec<i64>,
    pub created_at: String,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PathHistoryItem {
    pub file_instance_id: i64,
    pub old_path: String,
    pub new_path: String,
    pub change_type: String,
    pub detected_at: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MatchKind {
    Exact,
    Similar,
    RawJpegSet,
}

impl MatchKind {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Similar => "similar",
            Self::RawJpegSet => "raw_jpeg_set",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "exact" => Some(Self::Exact),
            "similar" => Some(Self::Similar),
            "raw_jpeg_set" => Some(Self::RawJpegSet),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewStatus {
    Pending,
    Approved,
    Skipped,
    Applied,
}

impl ReviewStatus {
    pub fn as_db_value(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Skipped => "skipped",
            Self::Applied => "applied",
        }
    }

    pub fn from_db_value(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "approved" => Some(Self::Approved),
            "skipped" => Some(Self::Skipped),
            "applied" => Some(Self::Applied),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReviewGroupFilter {
    pub kind: Option<MatchKind>,
    pub status: Option<ReviewStatus>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DecisionPayload {
    pub keep_ids: Vec<i64>,
    pub recycle_ids: Vec<i64>,
    pub note: Option<String>,
}
