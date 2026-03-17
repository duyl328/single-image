mod ai;
mod app;
mod fs_id;
mod image_tools;
mod models;

use app::{AppService, DecisionResult};
use models::{
    AiCreateSetPayload, AiJob, AiJobStarted, AiModelInfo, AiOverview, AiPreferenceTask,
    AiPreferenceVotePayload, AiRankedPhotoPage, AiSetDetail, AiSetSummary, AiStatus, AppSnapshot,
    ClassifyPhotoFilter, ClassifyPhotoPage, ClassifySortOrder, DecisionPayload, GroupDetail,
    GroupSummary, PathHistoryItem, PhotoRating, RatedPhotoPage, RatingPhotoFilter,
    RatingUndoResult, RecycleRatedPhotoPayload, ReviewActionSummary, ReviewGroupFilter,
    ScanProgress, ScanTaskStarted, SetRatingPayload, UnknownFormatSummary,
};
use tauri::Manager;

#[tauri::command]
fn app_snapshot(service: tauri::State<'_, AppService>) -> Result<AppSnapshot, String> {
    service.snapshot().map_err(|error| error.to_string())
}

#[tauri::command]
fn scan_start(
    service: tauri::State<'_, AppService>,
    paths: Vec<String>,
    threads: usize,
) -> Result<ScanTaskStarted, String> {
    service
        .start_scan_task(paths, threads)
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn scan_status(service: tauri::State<'_, AppService>) -> Result<ScanProgress, String> {
    service.scan_status().map_err(|error| error.to_string())
}

#[tauri::command]
fn scan_list_unknown_formats(
    service: tauri::State<'_, AppService>,
    scan_run_id: i64,
) -> Result<Vec<UnknownFormatSummary>, String> {
    service
        .list_unknown_formats(scan_run_id)
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn review_list_groups(
    service: tauri::State<'_, AppService>,
    filter: ReviewGroupFilter,
) -> Result<Vec<GroupSummary>, String> {
    service
        .list_groups(filter)
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn review_get_group(
    service: tauri::State<'_, AppService>,
    group_id: i64,
) -> Result<GroupDetail, String> {
    service
        .get_group(group_id)
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn review_apply_decision(
    service: tauri::State<'_, AppService>,
    group_id: i64,
    payload: DecisionPayload,
) -> Result<DecisionResult, String> {
    service
        .apply_decision(group_id, payload)
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn file_lookup_history(
    service: tauri::State<'_, AppService>,
    content_asset_id: i64,
) -> Result<Vec<PathHistoryItem>, String> {
    service
        .lookup_history(content_asset_id)
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn history_list_actions(
    service: tauri::State<'_, AppService>,
) -> Result<Vec<ReviewActionSummary>, String> {
    service.list_actions().map_err(|error| error.to_string())
}

#[tauri::command]
fn scan_cancel(service: tauri::State<'_, AppService>) -> Result<(), String> {
    service.scan_cancel().map_err(|error| error.to_string())
}

#[tauri::command]
fn rating_set(
    service: tauri::State<'_, AppService>,
    payload: SetRatingPayload,
) -> Result<PhotoRating, String> {
    service
        .set_rating(payload.file_instance_id, payload.rating, payload.note)
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn rating_undo(service: tauri::State<'_, AppService>) -> Result<Option<RatingUndoResult>, String> {
    service.undo_rating().map_err(|error| error.to_string())
}

#[tauri::command]
fn rating_recycle_photo(
    service: tauri::State<'_, AppService>,
    payload: RecycleRatedPhotoPayload,
) -> Result<PhotoRating, String> {
    service
        .recycle_rated_photo(payload.file_instance_id)
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn rating_list_photos(
    service: tauri::State<'_, AppService>,
    filter: RatingPhotoFilter,
    offset: i64,
    limit: i64,
) -> Result<RatedPhotoPage, String> {
    service
        .list_rated_photos(filter, offset, limit)
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn classify_list_photos(
    service: tauri::State<'_, AppService>,
    filter: ClassifyPhotoFilter,
    sort: ClassifySortOrder,
    offset: i64,
    limit: i64,
) -> Result<ClassifyPhotoPage, String> {
    service
        .classify_list_photos(filter, sort, offset, limit)
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn ai_list_jobs(service: tauri::State<'_, AppService>) -> Result<Vec<AiJob>, String> {
    service.ai_list_jobs().map_err(|e| e.to_string())
}

#[tauri::command]
fn ai_get_active_model(
    service: tauri::State<'_, AppService>,
) -> Result<Option<AiModelInfo>, String> {
    service.ai_get_active_model().map_err(|e| e.to_string())
}

#[tauri::command]
fn ai_run_extract_embeddings(
    service: tauri::State<'_, AppService>,
) -> Result<AiJobStarted, String> {
    service.ai_run_extract_embeddings().map_err(|e| e.to_string())
}

#[tauri::command]
fn ai_run_train_model(service: tauri::State<'_, AppService>) -> Result<AiJobStarted, String> {
    service.ai_run_train_model().map_err(|e| e.to_string())
}

#[tauri::command]
fn ai_run_predict_unrated(service: tauri::State<'_, AppService>) -> Result<AiJobStarted, String> {
    service.ai_run_predict_unrated().map_err(|e| e.to_string())
}

#[tauri::command]
fn ai_get_status(service: tauri::State<'_, AppService>) -> Result<AiStatus, String> {
    service.ai_get_status().map_err(|e| e.to_string())
}

#[tauri::command]
fn ai_run_full_pipeline(service: tauri::State<'_, AppService>) -> Result<AiJobStarted, String> {
    service.ai_run_full_pipeline().map_err(|e| e.to_string())
}

#[tauri::command]
fn ai_clear_predictions(service: tauri::State<'_, AppService>) -> Result<i64, String> {
    service.ai_clear_predictions().map_err(|e| e.to_string())
}

#[tauri::command]
fn ai_download_model(service: tauri::State<'_, AppService>) -> Result<AiJobStarted, String> {
    service.ai_download_model().map_err(|e| e.to_string())
}

#[tauri::command]
fn ai_get_overview(service: tauri::State<'_, AppService>) -> Result<AiOverview, String> {
    service.ai_get_overview().map_err(|error| error.to_string())
}

#[tauri::command]
fn ai_create_set_from_classify(
    service: tauri::State<'_, AppService>,
    payload: AiCreateSetPayload,
) -> Result<AiSetDetail, String> {
    service
        .ai_create_set_from_classify(payload)
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn ai_list_sets(service: tauri::State<'_, AppService>) -> Result<Vec<AiSetSummary>, String> {
    service.ai_list_sets().map_err(|error| error.to_string())
}

#[tauri::command]
fn ai_get_set_detail(
    service: tauri::State<'_, AppService>,
    set_id: i64,
) -> Result<AiSetDetail, String> {
    service
        .ai_get_set_detail(set_id)
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn ai_get_preference_tasks(
    service: tauri::State<'_, AppService>,
    set_id: i64,
    count: i64,
) -> Result<Vec<AiPreferenceTask>, String> {
    service
        .ai_get_preference_tasks(set_id, count)
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn ai_submit_preference(
    service: tauri::State<'_, AppService>,
    payload: AiPreferenceVotePayload,
) -> Result<(), String> {
    service
        .ai_submit_preference(payload)
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn ai_train_rank_model(service: tauri::State<'_, AppService>) -> Result<AiJobStarted, String> {
    service.ai_train_rank_model().map_err(|error| error.to_string())
}

#[tauri::command]
fn ai_train_and_rank_set(
    service: tauri::State<'_, AppService>,
    set_id: i64,
) -> Result<AiJobStarted, String> {
    service
        .ai_train_and_rank_set(set_id)
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn ai_rank_set(
    service: tauri::State<'_, AppService>,
    set_id: i64,
) -> Result<AiJobStarted, String> {
    service.ai_rank_set(set_id).map_err(|error| error.to_string())
}

#[tauri::command]
fn ai_get_ranked_items(
    service: tauri::State<'_, AppService>,
    set_id: i64,
    bucket: Option<String>,
    offset: i64,
    limit: i64,
) -> Result<AiRankedPhotoPage, String> {
    service
        .ai_get_ranked_items(set_id, bucket, offset, limit)
        .map_err(|error| error.to_string())
}

#[tauri::command]
fn ai_delete_set(service: tauri::State<'_, AppService>, set_id: i64) -> Result<(), String> {
    service.ai_delete_set(set_id).map_err(|error| error.to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .setup(|app| {
            let service = AppService::new(&app.handle())?;
            app.manage(service);
            Ok(())
        })
        .plugin(tauri_plugin_dialog::init())
        .invoke_handler(tauri::generate_handler![
            app_snapshot,
            scan_start,
            scan_cancel,
            scan_status,
            scan_list_unknown_formats,
            review_list_groups,
            review_get_group,
            review_apply_decision,
            file_lookup_history,
            history_list_actions,
            rating_set,
            rating_undo,
            rating_recycle_photo,
            rating_list_photos,
            classify_list_photos,
            ai_list_jobs,
            ai_get_active_model,
            ai_run_extract_embeddings,
            ai_run_train_model,
            ai_run_predict_unrated,
            ai_get_status,
            ai_run_full_pipeline,
            ai_clear_predictions,
            ai_download_model,
            ai_get_overview,
            ai_create_set_from_classify,
            ai_list_sets,
            ai_get_set_detail,
            ai_get_preference_tasks,
            ai_submit_preference,
            ai_train_rank_model,
            ai_train_and_rank_set,
            ai_rank_set,
            ai_get_ranked_items,
            ai_delete_set
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
