export type MatchKind = "exact" | "similar" | "raw_jpeg_set";
export type ReviewStatus = "pending" | "approved" | "skipped" | "applied";

export interface AppSnapshot {
  pendingGroupCount: number;
  appliedActionCount: number;
  indexedAssetCount: number;
  activeFileCount: number;
}

export interface UnknownFormatSummary {
  extension: string;
  count: number;
  examplePath: string;
}

export interface ScanResult {
  scanRunId: number;
  startedAt: string;
  completedAt: string;
  scannedRoots: string[];
  newFiles: number;
  updatedLocations: number;
  unchangedFiles: number;
  unsupportedExtensions: UnknownFormatSummary[];
}

export interface ScanTaskStarted {
  taskId: number;
}

export type ScanTaskStatus =
  | "idle"
  | "counting"
  | "running"
  | "finalizing"
  | "completed"
  | "failed"
  | "cancelled";

export interface ScanActiveItem { fileName: string; dirHint: string; }
export interface ScanRecentItem { fileName: string; status: 'new' | 'updated' | 'unchanged' | 'failed'; }
export interface GroupingProgress {
  exactDone: boolean; exactGroups: number;
  similarStarted: boolean; similarPairsDone: number; similarPairsTotal: number;
  similarGroups: number; similarDone: boolean;
  rawJpegDone: boolean; rawJpegGroups: number;
}

export interface ScanProgress {
  taskId: number | null;
  status: ScanTaskStatus;
  phase: string;
  message: string;
  totalFiles: number;
  queued: number;
  analyzing: number;
  done: number;
  newFiles: number;
  updatedFiles: number;
  unchangedFiles: number;
  failedFiles: number;
  activeItems: ScanActiveItem[];
  recentItems: ScanRecentItem[];
  grouping: GroupingProgress | null;
  startedAt: string | null;
  completedAt: string | null;
  result: ScanResult | null;
  error: string | null;
}

export interface GroupSummary {
  id: number;
  kind: MatchKind;
  status: ReviewStatus;
  anchor: string;
  memberCount: number;
  recommendedKeepInstanceId: number | null;
  recommendedKeepPath: string | null;
  recommendationReason: string;
  updatedAt: string;
}

export interface GroupMember {
  groupMemberId: number;
  fileInstanceId: number;
  contentAssetId: number;
  path: string;
  existsFlag: boolean;
  extension: string;
  formatName: string | null;
  width: number | null;
  height: number | null;
  qualityScore: number | null;
  previewSupported: boolean;
  thumbnailPath: string | null;
  sha256: string;
  similarity: number | null;
  role: string | null;
  capturedAt: string | null;
  volumeId: string | null;
  userRating: number | null;
}

export interface RatedPhoto {
  fileInstanceId: number;
  contentAssetId: number;
  path: string;
  extension: string;
  formatName: string | null;
  width: number | null;
  height: number | null;
  qualityScore: number | null;
  previewSupported: boolean;
  thumbnailPath: string | null;
  userRating: number | null;
}

export interface RatedPhotoPage {
  photos: RatedPhoto[];
  total: number;
}

export interface RatingPhotoFilter {
  unratedOnly: boolean;
  minRating: number | null;
}

export interface PhotoRating {
  fileInstanceId: number;
  rating: number;
  flagged: boolean;
  note: string | null;
  updatedAt: string;
}

export interface RatingUndoResult {
  fileInstanceId: number;
  restoredRating: number | null;
  updatedAt: string;
}

export interface SetRatingPayload {
  fileInstanceId: number;
  rating: number;
  note?: string | null;
}

export interface RecycleRatedPhotoPayload {
  fileInstanceId: number;
}

export interface GroupDetail {
  id: number;
  kind: MatchKind;
  status: ReviewStatus;
  anchor: string;
  recommendationReason: string;
  recommendedKeepInstanceId: number | null;
  members: GroupMember[];
}

export interface DecisionPayload {
  keepIds: number[];
  recycleIds: number[];
  note?: string | null;
}

export interface DecisionResult {
  groupId: number;
  recycledCount: number;
  appliedAt: string;
}

export interface ReviewGroupFilter {
  kind?: MatchKind | null;
  status?: ReviewStatus | null;
}

export interface ReviewActionSummary {
  id: number;
  groupId: number;
  groupKind: MatchKind;
  actionType: string;
  keepInstanceIds: number[];
  recycleInstanceIds: number[];
  createdAt: string;
  note?: string | null;
}

export interface PathHistoryItem {
  fileInstanceId: number;
  oldPath: string;
  newPath: string;
  changeType: string;
  detectedAt: string;
}

export type ClassifySortOrder =
  | "quality_desc"
  | "quality_asc"
  | "rating_desc"
  | "rating_asc"
  | "resolution_desc"
  | "path_asc"
  | "file_id_asc"
  | "updated_desc"
  | "ai_score_desc"
  | "ai_score_asc";

export interface ClassifyPhotoFilter {
  /** "all" | "unrated" | "rated" | "min" */
  ratingMode?: string | null;
  minRating?: number | null;
  minQuality?: number | null;
  maxQuality?: number | null;
  minWidth?: number | null;
  minHeight?: number | null;
  minMegapixels?: number | null;
  extensions?: string[] | null;
  previewOnly?: boolean | null;
  /** "all" | "in_group" | "not_in_group" | "pending_group" | "exact" | "similar" | "raw_jpeg_set" */
  groupFilter?: string | null;
  pathContains?: string | null;
  // AI filters
  minAiScore?: number | null;
  maxAiScore?: number | null;
  /** "low" | "maybe" | "high" */
  aiBucket?: string | null;
  deleteCandidateOnly?: boolean | null;
  hasAiPrediction?: boolean | null;
}

export interface ClassifyPhoto {
  fileInstanceId: number;
  contentAssetId: number;
  path: string;
  extension: string;
  formatName: string | null;
  width: number | null;
  height: number | null;
  qualityScore: number | null;
  previewSupported: boolean;
  thumbnailPath: string | null;
  userRating: number | null;
  groupId: number | null;
  groupKind: MatchKind | null;
  groupStatus: ReviewStatus | null;
  // AI prediction fields
  aiScore: number | null;
  aiConfidence: number | null;
  aiBucket: "low" | "maybe" | "high" | null;
  deleteCandidate: boolean;
}

// ── AI types ──────────────────────────────────────────────────────────────────

export interface AiJob {
  id: number;
  jobType: string;
  status: "pending" | "running" | "completed" | "failed" | "cancelled";
  payloadJson: string | null;
  progressDone: number;
  progressTotal: number;
  message: string | null;
  createdAt: string;
  startedAt: string | null;
  finishedAt: string | null;
}

export interface AiJobStarted {
  jobId: number;
}

export interface AiModelInfo {
  id: number;
  name: string;
  encoderName: string;
  encoderVersion: string;
  headType: string;
  trainingSampleCount: number;
  metricsJson: string | null;
  isActive: boolean;
  createdAt: string;
}

export interface AiModelFile {
  available: boolean;
  path: string;
  sizeBytes: number | null;
  encoderName: string;
}

export interface AiStatus {
  ratedCount: number;
  embeddingCount: number;
  predictedCount: number;
  totalAssets: number;
  activeModel: AiModelInfo | null;
  runningJob: AiJob | null;
  modelFile: AiModelFile;
  activeEncoder: string;
  lastDownloadJob: AiJob | null;
}

export interface AiCreateSetPayload {
  name?: string | null;
  filter: ClassifyPhotoFilter;
  sort: ClassifySortOrder;
  selection?: number[] | null;
}

export interface AiModelRunInfo {
  id: number;
  name: string;
  encoderName: string;
  encoderVersion: string;
  headType: string;
  preferenceVoteCount: number;
  starPairCount: number;
  trainingPairCount: number;
  metricsJson: string | null;
  isActive: boolean;
  createdAt: string;
}

export interface AiOverview {
  modelFile: AiModelFile;
  runningJob: AiJob | null;
  latestJob: AiJob | null;
  lastDownloadJob: AiJob | null;
  activeModelRun: AiModelRunInfo | null;
  setCount: number;
  preferenceVoteCount: number;
  ratedCount: number;
  modelStatus: "insufficient_data" | "untrained" | "ready";
}

export interface AiSetSummary {
  id: number;
  name: string;
  itemCount: number;
  preferenceVoteCount: number;
  hasRanking: boolean;
  lastRankedAt: string | null;
  createdAt: string;
}

export interface AiSetDetail {
  id: number;
  name: string;
  itemCount: number;
  preferenceVoteCount: number;
  createdAt: string;
  updatedAt: string;
  lastRankedAt: string | null;
  latestModelRun: AiModelRunInfo | null;
  topCount: number;
  midCount: number;
  backCount: number;
  uncertainCount: number;
}

export interface AiSetPhoto {
  fileInstanceId: number;
  contentAssetId: number;
  path: string;
  extension: string;
  formatName: string | null;
  width: number | null;
  height: number | null;
  qualityScore: number | null;
  previewSupported: boolean;
  thumbnailPath: string | null;
  userRating: number | null;
}

export interface AiPreferenceTask {
  pairKey: string;
  source: string;
  left: AiSetPhoto;
  right: AiSetPhoto;
}

export interface AiPreferenceVotePayload {
  setId: number;
  leftContentAssetId: number;
  rightContentAssetId: number;
  choice: "left" | "right" | "tie" | "skip";
}

export interface AiRankedPhoto {
  fileInstanceId: number;
  contentAssetId: number;
  path: string;
  extension: string;
  formatName: string | null;
  width: number | null;
  height: number | null;
  qualityScore: number | null;
  previewSupported: boolean;
  thumbnailPath: string | null;
  userRating: number | null;
  rankPosition: number;
  percentile: number;
  rankBucket: "top" | "mid" | "back";
  uncertaintyBucket: "high" | "medium" | "low";
  scoreGap: number;
}

export interface AiRankedPhotoPage {
  items: AiRankedPhoto[];
  total: number;
}

export interface ClassifyPhotoPage {
  photos: ClassifyPhoto[];
  total: number;
}
