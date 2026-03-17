import { invoke } from "@tauri-apps/api/core";

import type {
  AiCreateSetPayload,
  AiJob,
  AiJobStarted,
  AiModelInfo,
  AiOverview,
  AiPreferenceTask,
  AiPreferenceVotePayload,
  AiRankedPhotoPage,
  AiSetDetail,
  AiSetSummary,
  AiStatus,
  AppSnapshot,
  ClassifyPhotoFilter,
  ClassifyPhotoPage,
  ClassifySortOrder,
  DecisionPayload,
  DecisionResult,
  GroupDetail,
  GroupSummary,
  PathHistoryItem,
  PhotoRating,
  RatedPhotoPage,
  RatingUndoResult,
  RecycleRatedPhotoPayload,
  RatingPhotoFilter,
  ReviewActionSummary,
  ReviewGroupFilter,
  ScanProgress,
  ScanTaskStarted,
  SetRatingPayload,
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

export function setRating(payload: SetRatingPayload) {
  return invoke<PhotoRating>("rating_set", { payload });
}

export function undoRating() {
  return invoke<RatingUndoResult | null>("rating_undo");
}

export function recycleRatedPhoto(payload: RecycleRatedPhotoPayload) {
  return invoke<PhotoRating>("rating_recycle_photo", { payload });
}

export function listRatedPhotos(
  filter: RatingPhotoFilter,
  offset: number,
  limit: number,
) {
  return invoke<RatedPhotoPage>("rating_list_photos", { filter, offset, limit });
}

export function listClassifyPhotos(
  filter: ClassifyPhotoFilter,
  sort: ClassifySortOrder,
  offset: number,
  limit: number,
) {
  return invoke<ClassifyPhotoPage>("classify_list_photos", { filter, sort, offset, limit });
}

export function aiListJobs() {
  return invoke<AiJob[]>("ai_list_jobs");
}

export function aiGetActiveModel() {
  return invoke<AiModelInfo | null>("ai_get_active_model");
}

export function aiRunExtractEmbeddings() {
  return invoke<AiJobStarted>("ai_run_extract_embeddings");
}

export function aiRunTrainModel() {
  return invoke<AiJobStarted>("ai_run_train_model");
}

export function aiRunPredictUnrated() {
  return invoke<AiJobStarted>("ai_run_predict_unrated");
}

export function aiGetStatus() {
  return invoke<AiStatus>("ai_get_status");
}

export function aiRunFullPipeline() {
  return invoke<AiJobStarted>("ai_run_full_pipeline");
}

export function aiClearPredictions() {
  return invoke<number>("ai_clear_predictions");
}

export function aiDownloadModel() {
  return invoke<AiJobStarted>("ai_download_model");
}

export function aiGetOverview() {
  return invoke<AiOverview>("ai_get_overview");
}

export function aiCreateSetFromClassify(payload: AiCreateSetPayload) {
  return invoke<AiSetDetail>("ai_create_set_from_classify", { payload });
}

export function aiListSets() {
  return invoke<AiSetSummary[]>("ai_list_sets");
}

export function aiGetSetDetail(setId: number) {
  return invoke<AiSetDetail>("ai_get_set_detail", { setId });
}

export function aiGetPreferenceTasks(setId: number, count: number) {
  return invoke<AiPreferenceTask[]>("ai_get_preference_tasks", { setId, count });
}

export function aiSubmitPreference(payload: AiPreferenceVotePayload) {
  return invoke<void>("ai_submit_preference", { payload });
}

export function aiTrainRankModel() {
  return invoke<AiJobStarted>("ai_train_rank_model");
}

export function aiTrainAndRankSet(setId: number) {
  return invoke<AiJobStarted>("ai_train_and_rank_set", { setId });
}

export function aiRankSet(setId: number) {
  return invoke<AiJobStarted>("ai_rank_set", { setId });
}

export function aiGetRankedItems(
  setId: number,
  bucket: string | null,
  offset: number,
  limit: number,
) {
  return invoke<AiRankedPhotoPage>("ai_get_ranked_items", {
    setId,
    bucket,
    offset,
    limit,
  });
}

export function aiDeleteSet(setId: number) {
  return invoke<void>("ai_delete_set", { setId });
}
