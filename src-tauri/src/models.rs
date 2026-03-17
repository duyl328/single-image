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
    pub user_rating: Option<i32>,
}

/// A single file instance with its user rating, used in the rating/review workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RatedPhoto {
    pub file_instance_id: i64,
    pub content_asset_id: i64,
    pub path: String,
    pub extension: String,
    pub format_name: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub quality_score: Option<f32>,
    pub preview_supported: bool,
    pub thumbnail_path: Option<String>,
    pub user_rating: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RatedPhotoPage {
    pub photos: Vec<RatedPhoto>,
    pub total: i64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RatingPhotoFilter {
    /// If true, only return photos that have not been rated yet.
    pub unrated_only: bool,
    /// If set, only return photos with rating >= this value.
    pub min_rating: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PhotoRating {
    pub file_instance_id: i64,
    pub rating: i32,
    pub flagged: bool,
    pub note: Option<String>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RatingUndoResult {
    pub file_instance_id: i64,
    pub restored_rating: Option<i32>,
    pub updated_at: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetRatingPayload {
    pub file_instance_id: i64,
    pub rating: i32,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RecycleRatedPhotoPayload {
    pub file_instance_id: i64,
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

/// A single file instance returned by the classify page query.
/// Contains group membership info so the UI can offer jump links.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClassifyPhoto {
    pub file_instance_id: i64,
    pub content_asset_id: i64,
    pub path: String,
    pub extension: String,
    pub format_name: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub quality_score: Option<f32>,
    pub preview_supported: bool,
    pub thumbnail_path: Option<String>,
    pub user_rating: Option<i32>,
    /// First group this file belongs to (if any).
    pub group_id: Option<i64>,
    pub group_kind: Option<MatchKind>,
    pub group_status: Option<ReviewStatus>,
    // AI prediction fields (null when no prediction exists)
    pub ai_score: Option<f32>,
    pub ai_confidence: Option<f32>,
    pub ai_bucket: Option<String>,
    pub delete_candidate: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClassifyPhotoPage {
    pub photos: Vec<ClassifyPhoto>,
    pub total: i64,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClassifyPhotoFilter {
    /// "all" | "unrated" | "rated" | "min"
    pub rating_mode: Option<String>,
    /// Used when rating_mode = "min"
    pub min_rating: Option<i32>,
    pub min_quality: Option<f32>,
    pub max_quality: Option<f32>,
    pub min_width: Option<u32>,
    pub min_height: Option<u32>,
    /// Minimum megapixel count (width × height ÷ 1_000_000)
    pub min_megapixels: Option<f32>,
    /// Filter to specific extensions, e.g. ["jpg","png"]. None/empty = no filter.
    pub extensions: Option<Vec<String>>,
    pub preview_only: Option<bool>,
    /// "all" | "in_group" | "not_in_group" | "pending_group" | "exact" | "similar" | "raw_jpeg_set"
    pub group_filter: Option<String>,
    pub path_contains: Option<String>,
    // AI filters
    pub min_ai_score: Option<f32>,
    pub max_ai_score: Option<f32>,
    /// "low" | "maybe" | "high"
    pub ai_bucket: Option<String>,
    pub delete_candidate_only: Option<bool>,
    pub has_ai_prediction: Option<bool>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClassifySortOrder {
    QualityDesc,
    QualityAsc,
    RatingDesc,
    RatingAsc,
    ResolutionDesc,
    PathAsc,
    FileIdAsc,
    UpdatedDesc,
    AiScoreDesc,
    AiScoreAsc,
}

impl ClassifySortOrder {
    pub fn to_sql(self) -> &'static str {
        match self {
            Self::QualityDesc => "ca.quality_score DESC NULLS LAST, fi.id ASC",
            Self::QualityAsc => "ca.quality_score ASC NULLS LAST, fi.id ASC",
            Self::RatingDesc => "pr.rating DESC NULLS LAST, fi.id ASC",
            Self::RatingAsc => "pr.rating ASC NULLS LAST, fi.id ASC",
            Self::ResolutionDesc => {
                "(CAST(ca.width AS INTEGER) * CAST(ca.height AS INTEGER)) DESC NULLS LAST, fi.id ASC"
            }
            Self::PathAsc => "fi.current_path ASC",
            Self::FileIdAsc => "fi.id ASC",
            Self::UpdatedDesc => "fi.last_seen_at DESC, fi.id ASC",
            Self::AiScoreDesc => "ap.ai_score DESC NULLS LAST, fi.id ASC",
            Self::AiScoreAsc => "ap.ai_score ASC NULLS LAST, fi.id ASC",
        }
    }
}

impl Default for ClassifySortOrder {
    fn default() -> Self {
        Self::QualityDesc
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

// ── AI types ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiJob {
    pub id: i64,
    pub job_type: String,
    pub status: String,
    pub payload_json: Option<String>,
    pub progress_done: i64,
    pub progress_total: i64,
    pub message: Option<String>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiJobStarted {
    pub job_id: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiModelInfo {
    pub id: i64,
    pub name: String,
    pub encoder_name: String,
    pub encoder_version: String,
    pub head_type: String,
    pub training_sample_count: i64,
    pub metrics_json: Option<String>,
    pub is_active: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiModelFile {
    pub available: bool,
    pub path: String,
    pub size_bytes: Option<u64>,
    pub encoder_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiStatus {
    pub rated_count: i64,
    pub embedding_count: i64,
    pub predicted_count: i64,
    pub total_assets: i64,
    pub active_model: Option<AiModelInfo>,
    pub running_job: Option<AiJob>,
    pub model_file: AiModelFile,
    pub active_encoder: String,
    /// Most recent download_model job (any status), so the UI can show failure reason.
    pub last_download_job: Option<AiJob>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiCreateSetPayload {
    pub name: Option<String>,
    pub filter: ClassifyPhotoFilter,
    pub sort: ClassifySortOrder,
    pub selection: Option<Vec<i64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiModelRunInfo {
    pub id: i64,
    pub name: String,
    pub encoder_name: String,
    pub encoder_version: String,
    pub head_type: String,
    pub preference_vote_count: i64,
    pub star_pair_count: i64,
    pub training_pair_count: i64,
    pub metrics_json: Option<String>,
    pub is_active: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiOverview {
    pub model_file: AiModelFile,
    pub running_job: Option<AiJob>,
    pub latest_job: Option<AiJob>,
    pub last_download_job: Option<AiJob>,
    pub active_model_run: Option<AiModelRunInfo>,
    pub set_count: i64,
    pub preference_vote_count: i64,
    pub rated_count: i64,
    /// "insufficient_data" | "untrained" | "ready"
    pub model_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiSetSummary {
    pub id: i64,
    pub name: String,
    pub item_count: i64,
    pub preference_vote_count: i64,
    pub has_ranking: bool,
    pub last_ranked_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiSetDetail {
    pub id: i64,
    pub name: String,
    pub item_count: i64,
    pub preference_vote_count: i64,
    pub created_at: String,
    pub updated_at: String,
    pub last_ranked_at: Option<String>,
    pub latest_model_run: Option<AiModelRunInfo>,
    pub top_count: i64,
    pub mid_count: i64,
    pub back_count: i64,
    pub uncertain_count: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiSetPhoto {
    pub file_instance_id: i64,
    pub content_asset_id: i64,
    pub path: String,
    pub extension: String,
    pub format_name: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub quality_score: Option<f32>,
    pub preview_supported: bool,
    pub thumbnail_path: Option<String>,
    pub user_rating: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiPreferenceTask {
    pub pair_key: String,
    pub source: String,
    pub left: AiSetPhoto,
    pub right: AiSetPhoto,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiPreferenceVotePayload {
    pub set_id: i64,
    pub left_content_asset_id: i64,
    pub right_content_asset_id: i64,
    /// "left" | "right" | "tie" | "skip"
    pub choice: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiRankedPhoto {
    pub file_instance_id: i64,
    pub content_asset_id: i64,
    pub path: String,
    pub extension: String,
    pub format_name: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub quality_score: Option<f32>,
    pub preview_supported: bool,
    pub thumbnail_path: Option<String>,
    pub user_rating: Option<i32>,
    pub rank_position: i64,
    pub percentile: f32,
    pub rank_bucket: String,
    pub uncertainty_bucket: String,
    pub score_gap: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AiRankedPhotoPage {
    pub items: Vec<AiRankedPhoto>,
    pub total: i64,
}
