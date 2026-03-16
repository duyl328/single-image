mod app;
mod fs_id;
mod image_tools;
mod models;

use app::{AppService, DecisionResult};
use models::{
    AppSnapshot, DecisionPayload, GroupDetail, GroupSummary, PathHistoryItem, PhotoRating,
    RatedPhotoPage, RatingPhotoFilter, RatingUndoResult, RecycleRatedPhotoPayload,
    ReviewActionSummary, ReviewGroupFilter, ScanProgress, ScanTaskStarted, SetRatingPayload,
    UnknownFormatSummary,
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
            rating_list_photos
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
