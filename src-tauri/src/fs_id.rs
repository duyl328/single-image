use std::iter::once;
use std::os::windows::ffi::OsStrExt;
use std::path::Path;

use anyhow::{Context, Result};
use windows::core::PCWSTR;
use windows::Win32::Foundation::CloseHandle;
use windows::Win32::Storage::FileSystem::{
    CreateFileW, GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_NORMAL,
    FILE_FLAG_BACKUP_SEMANTICS, FILE_GENERIC_READ, FILE_SHARE_DELETE, FILE_SHARE_READ,
    FILE_SHARE_WRITE, OPEN_EXISTING,
};

#[derive(Debug, Clone)]
pub struct WindowsIdentity {
    pub volume_id: String,
    pub file_id: String,
}

pub fn read_windows_identity(path: &Path) -> Result<WindowsIdentity> {
    let wide_path: Vec<u16> = path.as_os_str().encode_wide().chain(once(0)).collect();

    let handle = unsafe {
        CreateFileW(
            PCWSTR::from_raw(wide_path.as_ptr()),
            FILE_GENERIC_READ.0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            None,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_BACKUP_SEMANTICS,
            None,
        )
    }
    .with_context(|| format!("unable to open file handle for {:?}", path))?;

    let info = unsafe {
        let mut info = BY_HANDLE_FILE_INFORMATION::default();
        GetFileInformationByHandle(handle, &mut info)
            .ok()
            .with_context(|| format!("unable to read file id for {:?}", path))?;
        CloseHandle(handle)
            .ok()
            .context("unable to close file handle")?;
        info
    };

    let file_index = ((info.nFileIndexHigh as u64) << 32) | info.nFileIndexLow as u64;

    Ok(WindowsIdentity {
        volume_id: format!("{:08X}", info.dwVolumeSerialNumber),
        file_id: format!("{:016X}", file_index),
    })
}
