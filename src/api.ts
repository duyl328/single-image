import { invoke } from "@tauri-apps/api/core";

import type {
  AppSnapshot,
  DecisionPayload,
  DecisionResult,
  GroupDetail,
  GroupSummary,
  PathHistoryItem,
  ReviewActionSummary,
  ReviewGroupFilter,
  ScanProgress,
  ScanTaskStarted,
  UnknownFormatSummary,
} from "./types";

export function loadSnapshot() {
  return invoke<AppSnapshot>("app_snapshot");
}

export function startScan(paths: string[], threads: number) {
  return invoke<ScanTaskStarted>("scan_start", { paths, threads });
}

export function loadScanStatus() {
  return invoke<ScanProgress>("scan_status");
}

export function loadUnknownFormats(scanRunId: number) {
  return invoke<UnknownFormatSummary[]>("scan_list_unknown_formats", {
    scanRunId,
  });
}

export function loadGroups(filter: ReviewGroupFilter) {
  return invoke<GroupSummary[]>("review_list_groups", { filter });
}

export function loadGroup(groupId: number) {
  return invoke<GroupDetail>("review_get_group", { groupId });
}

export function applyDecision(groupId: number, payload: DecisionPayload) {
  return invoke<DecisionResult>("review_apply_decision", { groupId, payload });
}

export function loadHistory(contentAssetId: number) {
  return invoke<PathHistoryItem[]>("file_lookup_history", { contentAssetId });
}

export function loadActions() {
  return invoke<ReviewActionSummary[]>("history_list_actions");
}

export function cancelScan() {
  return invoke<void>("scan_cancel");
}
