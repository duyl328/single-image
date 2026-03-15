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
