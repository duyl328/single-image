use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom};
use std::path::Path;

#[cfg(test)]
use std::path::PathBuf;

use anyhow::{Context, Result};
use image::codecs::jpeg::JpegEncoder;
use image::imageops::{resize, FilterType};
use image::{DynamicImage, GenericImageView, GrayImage, ImageReader};

#[cfg(test)]
use image::Luma;
use serde::Serialize;
use sha2::{Digest, Sha256};

pub const ANALYSIS_VERSION: i32 = 2;
pub const SIMILARITY_THRESHOLD: f32 = 0.97;
// Hamming-distance thresholds for the BK-tree candidate filter.
// Both must be satisfied (AND, not OR) to reduce false positives.
pub const PHASH_MAX_DISTANCE: u32 = 8;
pub const DHASH_MAX_DISTANCE: u32 = 12;

const FAST_SLICE_BYTES: usize = 64 * 1024;
const THUMB_EDGE: u32 = 384;
const SIMILAR_EDGE: u32 = 128;
const PHASH_EDGE: u32 = 32;
const DHASH_WIDTH: u32 = 9;
const DHASH_HEIGHT: u32 = 8;
const THUMB_JPEG_QUALITY: u8 = 85;

/// Coarse classification of a file based on its extension.
/// Drives which analysis stages are applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileClass {
    /// Standard raster images: jpg, jpeg, png, webp, heic, heif.
    /// Full pipeline: quick hash → SHA-256 → decode → phash/dhash/thumb.
    Image,
    /// RAW files (rw2). Quick hash + SHA-256 only; no decode/visual analysis.
    RawImage,
    /// Video files (mp4, mov). Quick hash + SHA-256 only; no similar detection.
    Video,
    /// Sidecar metadata files (aae, xmp). Written to sidecar_files table,
    /// never treated as a standalone photo.
    Sidecar,
    /// Archives (zip, 7z, rar). Recorded as unsupported/ignored; not analysed.
    Archive,
    /// Anything else. Recorded as unsupported/ignored.
    Other,
}

impl FileClass {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::RawImage => "raw_image",
            Self::Video => "video",
            Self::Sidecar => "sidecar",
            Self::Archive => "archive",
            Self::Other => "other",
        }
    }

    pub fn from_str(s: &str) -> Self {
        match s {
            "image" => Self::Image,
            "raw_image" => Self::RawImage,
            "video" => Self::Video,
            "sidecar" => Self::Sidecar,
            "archive" => Self::Archive,
            _ => Self::Other,
        }
    }

    /// Returns true for file classes that require SHA-256 computation.
    pub fn needs_sha256(self) -> bool {
        matches!(self, Self::Image | Self::RawImage | Self::Video)
    }

    /// Returns true for file classes that support full visual analysis
    /// (phash / dhash / thumbnail).
    pub fn needs_visual(self) -> bool {
        matches!(self, Self::Image)
    }
}

pub fn classify_extension(ext: &str) -> FileClass {
    match ext {
        "jpg" | "jpeg" | "png" | "webp" | "heic" | "heif" => FileClass::Image,
        "rw2" => FileClass::RawImage,
        "mp4" | "mov" => FileClass::Video,
        "aae" | "xmp" => FileClass::Sidecar,
        "zip" | "7z" | "rar" => FileClass::Archive,
        _ => FileClass::Other,
    }
}

/// Returns true when the extension is handled as a media asset (Image or
/// RawImage). Used for compatibility with callers that only care about
/// "is this a photo/raw we track?".
pub fn is_supported_image_extension(ext: &str) -> bool {
    matches!(
        classify_extension(ext),
        FileClass::Image | FileClass::RawImage
    )
}

/// Returns true when the extension can be decoded to produce visual features
/// (phash / dhash / thumbnail).
pub fn can_decode_preview(ext: &str) -> bool {
    matches!(ext, "jpg" | "jpeg" | "png" | "webp")
}

pub fn normalized_extension(path: &Path) -> String {
    path.extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

pub fn normalized_stem(path: &Path) -> String {
    path.file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

pub fn hash_file_sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path).with_context(|| format!("unable to open {:?}", path))?;
    let mut buffer = vec![0u8; 256 * 1024];
    let mut hasher = Sha256::new();

    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(hex::encode(hasher.finalize()))
}

/// Cheap fingerprint: three 64-KB slices (head, middle, tail) hashed with
/// BLAKE3, salted with the file size. Reads at most 192 KB regardless of
/// file size, making it safe to call on large RAW files.
pub fn hash_file_quick(path: &Path, file_size: u64) -> Result<String> {
    let mut file = File::open(path).with_context(|| format!("unable to open {:?}", path))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0u8; FAST_SLICE_BYTES];

    let slices = if file_size <= (FAST_SLICE_BYTES as u64 * 2) {
        vec![0]
    } else {
        vec![
            0,
            file_size / 2 - (FAST_SLICE_BYTES as u64 / 2),
            file_size.saturating_sub(FAST_SLICE_BYTES as u64),
        ]
    };

    for offset in slices {
        file.seek(SeekFrom::Start(offset))?;
        let read = file.read(&mut buffer)?;
        hasher.update(&buffer[..read]);
    }

    hasher.update(file_size.to_le_bytes().as_slice());
    Ok(hasher.finalize().to_hex().to_string())
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AssetAnalysis {
    pub sha256: String,
    pub quick_fingerprint: String,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub format_name: Option<String>,
    pub phash: Option<String>,
    pub dhash: Option<String>,
    pub quality_score: Option<f32>,
    pub thumbnail_path: Option<String>,
    pub preview_supported: bool,
}

/// Visual analysis results for a decodable image (phash, dhash, thumbnail,
/// quality score, dimensions).  Computed separately from the hash phase so
/// the pipeline can defer it until after exact-duplicate detection.
#[derive(Debug, Clone)]
pub struct VisualAnalysis {
    pub width: u32,
    pub height: u32,
    pub phash: String,
    pub dhash: String,
    pub quality_score: f32,
    pub thumbnail_path: String,
}

/// Full analysis pipeline (legacy single-shot path, kept for compatibility).
/// Computes quick hash + SHA-256 + visual features in one call.
pub fn analyze_asset(
    path: &Path,
    ext: &str,
    file_size: u64,
    thumbs_dir: &Path,
    known_sha256: Option<String>,
) -> Result<AssetAnalysis> {
    let quick_fingerprint = hash_file_quick(path, file_size)?;
    let sha256 = match known_sha256 {
        Some(value) => value,
        None => hash_file_sha256(path)?,
    };

    if !can_decode_preview(ext) {
        return Ok(AssetAnalysis {
            sha256,
            quick_fingerprint,
            width: None,
            height: None,
            format_name: Some(ext.to_ascii_uppercase()),
            phash: None,
            dhash: None,
            quality_score: None,
            thumbnail_path: None,
            preview_supported: false,
        });
    }

    let format_name = Some(match ext {
        "jpg" | "jpeg" => "JPEG".to_string(),
        "png" => "PNG".to_string(),
        "webp" => "WEBP".to_string(),
        other => other.to_ascii_uppercase(),
    });

    let visual = compute_visual_features(path, thumbs_dir, &sha256)?;

    Ok(AssetAnalysis {
        sha256,
        quick_fingerprint,
        width: Some(visual.width),
        height: Some(visual.height),
        format_name,
        phash: Some(visual.phash),
        dhash: Some(visual.dhash),
        quality_score: Some(visual.quality_score),
        thumbnail_path: Some(visual.thumbnail_path),
        preview_supported: true,
    })
}

/// Compute only the visual features (phash / dhash / thumbnail / quality)
/// for a decodable image.  Called in the visual-analysis phase after SHA-256
/// is already known.
pub fn compute_visual_features(
    path: &Path,
    thumbs_dir: &Path,
    sha256: &str,
) -> Result<VisualAnalysis> {
    let image = ImageReader::open(path)
        .with_context(|| format!("unable to read image {:?}", path))?
        .with_guessed_format()?
        .decode()
        .with_context(|| format!("unable to decode image {:?}", path))?;

    let (width, height) = image.dimensions();
    let grayscale = resize(
        &image.to_luma8(),
        SIMILAR_EDGE,
        SIMILAR_EDGE,
        FilterType::Triangle,
    );
    let phash = compute_perceptual_hash(&image);
    let dhash = compute_difference_hash(&image);
    let quality_score = compute_quality_score(&grayscale, width, height);

    std::fs::create_dir_all(thumbs_dir)?;
    // Thumbnails are stored as JPEG for smaller file sizes.
    let thumb_path = thumbs_dir.join(format!("{sha256}.jpg"));
    let thumb = image.thumbnail(THUMB_EDGE, THUMB_EDGE);
    let file = File::create(&thumb_path)
        .with_context(|| format!("unable to create thumbnail {:?}", thumb_path))?;
    let mut writer = BufWriter::new(file);
    thumb
        .write_with_encoder(JpegEncoder::new_with_quality(&mut writer, THUMB_JPEG_QUALITY))
        .with_context(|| format!("unable to encode thumbnail {:?}", thumb_path))?;

    Ok(VisualAnalysis {
        width,
        height,
        phash: format!("{:016x}", phash),
        dhash: format!("{:016x}", dhash),
        quality_score,
        thumbnail_path: path_to_string(&thumb_path),
    })
}

pub fn load_similarity_buffer(path: &Path) -> Result<GrayImage> {
    // Accept both legacy .png thumbnails and new .jpg thumbnails.
    let image = ImageReader::open(path)
        .with_context(|| format!("unable to open thumbnail {:?}", path))?
        .with_guessed_format()?
        .decode()
        .with_context(|| format!("unable to decode thumbnail {:?}", path))?;
    Ok(resize(
        &image.to_luma8(),
        SIMILAR_EDGE,
        SIMILAR_EDGE,
        FilterType::Triangle,
    ))
}

/// Global SSIM computed on 128×128 greyscale thumbnails.
/// Fast approximation used as the final gate after hash candidate filtering.
pub fn ssim_from_buffers(left: &GrayImage, right: &GrayImage) -> f32 {
    let left_pixels = left.as_raw();
    let right_pixels = right.as_raw();
    let count = left_pixels.len() as f64;

    let left_mean = left_pixels.iter().map(|&v| v as f64 / 255.0).sum::<f64>() / count;
    let right_mean = right_pixels.iter().map(|&v| v as f64 / 255.0).sum::<f64>() / count;

    let mut left_var = 0.0f64;
    let mut right_var = 0.0f64;
    let mut covariance = 0.0f64;
    for (&l, &r) in left_pixels.iter().zip(right_pixels.iter()) {
        let l = l as f64 / 255.0 - left_mean;
        let r = r as f64 / 255.0 - right_mean;
        left_var += l * l;
        right_var += r * r;
        covariance += l * r;
    }
    let denom = (count - 1.0).max(1.0);
    left_var /= denom;
    right_var /= denom;
    covariance /= denom;

    let c1 = 0.01_f64.powi(2);
    let c2 = 0.03_f64.powi(2);
    let numerator = (2.0 * left_mean * right_mean + c1) * (2.0 * covariance + c2);
    let denominator =
        (left_mean.powi(2) + right_mean.powi(2) + c1) * (left_var + right_var + c2);
    (numerator / denominator).clamp(0.0, 1.0) as f32
}

fn compute_difference_hash(image: &DynamicImage) -> u64 {
    let small = resize(
        &image.to_luma8(),
        DHASH_WIDTH,
        DHASH_HEIGHT,
        FilterType::Triangle,
    );
    let mut value = 0u64;

    for y in 0..DHASH_HEIGHT {
        for x in 0..(DHASH_WIDTH - 1) {
            let left = small.get_pixel(x, y).0[0];
            let right = small.get_pixel(x + 1, y).0[0];
            let bit = if left > right { 1 } else { 0 };
            value = (value << 1) | bit;
        }
    }

    value
}

/// Perceptual hash based on the top-left 8×8 DCT coefficients of a 32×32
/// greyscale image.
///
/// **Bug fixed (v2):** the previous implementation sorted the 63 AC
/// coefficients before bit-packing, which produced nearly identical hashes
/// for all images (the sorted values always straddle the median in the same
/// pattern regardless of image content).
///
/// The correct algorithm:
/// 1. Resize to 32×32 greyscale.
/// 2. Apply 2-D DCT.
/// 3. Collect the 63 AC coefficients in natural row-major order from the
///    8×8 top-left region (skip DC at [0][0]).
/// 4. Compute the **mean** of those 63 values.
/// 5. Pack bits in the **same natural order**: bit = 1 if value > mean.
///
/// Comparing two hashes with Hamming distance gives a meaningful similarity
/// signal; distance ≤ 8 is a good candidate threshold for near-duplicate
/// detection.
fn compute_perceptual_hash(image: &DynamicImage) -> u64 {
    let n = PHASH_EDGE as usize;
    let small = resize(&image.to_luma8(), PHASH_EDGE, PHASH_EDGE, FilterType::Triangle);

    let pixels: Vec<f64> = small.as_raw().iter().map(|&p| p as f64 / 255.0).collect();
    let dct = dct_2d_separable(&pixels, n);

    // Collect the 63 AC coefficients in natural row-major order
    // (top-left 8×8 block, skipping DC at [0][0]).
    let mut values = Vec::with_capacity(63);
    for y in 0..8usize {
        for x in 0..8usize {
            if x == 0 && y == 0 {
                continue;
            }
            values.push(dct[y][x]);
        }
    }

    // Compute the mean – do NOT sort the values.
    let mean = values.iter().sum::<f64>() / values.len() as f64;

    // Pack bits in the same natural order as the collected values.
    let mut hash = 0u64;
    for &value in &values {
        hash = (hash << 1) | u64::from(value > mean);
    }
    hash
}

/// Separable 2D DCT-II with precomputed cosine table — O(N³) rather than
/// the naïve O(N⁴).
fn dct_2d_separable(pixels: &[f64], n: usize) -> Vec<Vec<f64>> {
    let mut cos_table = vec![0.0f64; n * n];
    for k in 0..n {
        for i in 0..n {
            cos_table[k * n + i] =
                ((2 * i + 1) as f64 * k as f64 * std::f64::consts::PI / (2.0 * n as f64)).cos();
        }
    }
    let alpha: Vec<f64> = (0..n)
        .map(|k| if k == 0 { (1.0 / n as f64).sqrt() } else { (2.0 / n as f64).sqrt() })
        .collect();

    // Step 1: row-wise 1D DCT.
    let mut temp = vec![0.0f64; n * n];
    for row in 0..n {
        for v in 0..n {
            let mut sum = 0.0;
            for col in 0..n {
                sum += pixels[row * n + col] * cos_table[v * n + col];
            }
            temp[row * n + v] = sum;
        }
    }

    // Step 2: column-wise 1D DCT with alpha scaling.
    let mut output = vec![vec![0.0f64; n]; n];
    for u in 0..n {
        for v in 0..n {
            let mut sum = 0.0;
            for row in 0..n {
                sum += temp[row * n + v] * cos_table[u * n + row];
            }
            output[u][v] = alpha[u] * alpha[v] * sum;
        }
    }

    output
}

fn compute_quality_score(image: &GrayImage, width: u32, height: u32) -> f32 {
    let sharpness = variance_of_laplacian(image).clamp(0.0, 1.0);
    let exposure = exposure_score(image).clamp(0.0, 1.0);
    let megapixels = (width as f32 * height as f32) / 1_000_000.0;
    let resolution = (megapixels / 16.0).clamp(0.0, 1.0);

    ((resolution * 0.35) + (sharpness * 0.40) + (exposure * 0.25)) * 100.0
}

fn variance_of_laplacian(image: &GrayImage) -> f32 {
    let mut count = 0u32;
    let mut mean = 0.0f32;
    let mut m2 = 0.0f32;

    for y in 1..image.height().saturating_sub(1) {
        for x in 1..image.width().saturating_sub(1) {
            let center = image.get_pixel(x, y).0[0] as f32;
            let up = image.get_pixel(x, y - 1).0[0] as f32;
            let down = image.get_pixel(x, y + 1).0[0] as f32;
            let left = image.get_pixel(x - 1, y).0[0] as f32;
            let right = image.get_pixel(x + 1, y).0[0] as f32;
            let lap = (4.0 * center - up - down - left - right).abs();
            count += 1;
            let delta = lap - mean;
            mean += delta / count as f32;
            m2 += delta * (lap - mean);
        }
    }

    if count == 0 {
        return 0.0;
    }

    let variance = m2 / count as f32;
    (variance / 12_000.0).clamp(0.0, 1.0)
}

fn exposure_score(image: &GrayImage) -> f32 {
    let pixels = image.as_raw();
    if pixels.is_empty() {
        return 0.0;
    }

    let mean = pixels.iter().map(|v| *v as f32 / 255.0).sum::<f32>() / pixels.len() as f32;
    let variance = pixels
        .iter()
        .map(|v| {
            let normalized = *v as f32 / 255.0;
            (normalized - mean).powi(2)
        })
        .sum::<f32>()
        / pixels.len() as f32;
    let dynamic_range = (variance.sqrt() / 0.25).clamp(0.0, 1.0);
    let balanced_mean = 1.0 - ((mean - 0.5).abs() * 2.0).clamp(0.0, 1.0);
    (balanced_mean * 0.65) + (dynamic_range * 0.35)
}

pub fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

// ─── Test helpers ─────────────────────────────────────────────────────────────

#[cfg(test)]
pub fn luma_from_value(value: u8) -> Luma<u8> {
    Luma([value])
}

#[cfg(test)]
pub fn save_test_image(path: &Path, width: u32, height: u32, seed: u8) -> Result<PathBuf> {
    let mut image = GrayImage::new(width, height);
    for (idx, pixel) in image.pixels_mut().enumerate() {
        let value = seed.wrapping_add((idx % 255) as u8);
        *pixel = luma_from_value(value);
    }
    DynamicImage::ImageLuma8(image).save(path)?;
    Ok(path.to_path_buf())
}

/// Creates a test image where every pixel has a different value based on
/// spatial position, making it visually distinct from `save_test_image`.
/// Used for tests that need demonstrably different images.
#[cfg(test)]
pub fn save_distinct_test_image(
    path: &Path,
    width: u32,
    height: u32,
    base: u8,
) -> Result<PathBuf> {
    use image::ImageBuffer;
    let image: GrayImage = ImageBuffer::from_fn(width, height, |x, y| {
        Luma([base.wrapping_add(((x * 7 + y * 13) % 256) as u8)])
    });
    DynamicImage::ImageLuma8(image).save(path)?;
    Ok(path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    /// Fixed pHash should produce distinct hashes for structurally different
    /// images and small Hamming distances for near-identical ones.
    #[test]
    fn phash_is_diverse_not_constant() {
        let dir = tempdir().unwrap();
        let p1 = dir.path().join("img1.png");
        let p2 = dir.path().join("img2.png");
        let p3 = dir.path().join("img3.png");

        save_test_image(&p1, 64, 64, 0).unwrap();
        save_distinct_test_image(&p2, 64, 64, 128).unwrap();
        save_distinct_test_image(&p3, 64, 64, 64).unwrap();

        let load = |p: &std::path::PathBuf| {
            ImageReader::open(p)
                .unwrap()
                .with_guessed_format()
                .unwrap()
                .decode()
                .unwrap()
        };
        let h1 = compute_perceptual_hash(&load(&p1));
        let h2 = compute_perceptual_hash(&load(&p2));
        let h3 = compute_perceptual_hash(&load(&p3));

        // Different images must not all hash to the same value.
        let hashes = [h1, h2, h3];
        let unique: std::collections::HashSet<u64> = hashes.iter().copied().collect();
        assert!(
            unique.len() >= 2,
            "pHash produced identical hashes for different images: {:?}",
            hashes
        );

        // Near-identical images (same content, just brightness shift) should
        // have small Hamming distance.
        let p_near = dir.path().join("near.png");
        save_test_image(&p_near, 64, 64, 1).unwrap(); // seed=1 ≈ seed=0
        let h_near = compute_perceptual_hash(&load(&p_near));
        let dist_same_family = (h1 ^ h_near).count_ones();
        // Different-family images should generally be further apart.
        let dist_different = (h1 ^ h2).count_ones();
        assert!(
            dist_same_family < dist_different || dist_same_family <= 16,
            "pHash: near-identical pair distance {} vs different pair distance {}",
            dist_same_family,
            dist_different
        );
    }

    #[test]
    fn classify_extension_is_correct() {
        assert_eq!(classify_extension("jpg"), FileClass::Image);
        assert_eq!(classify_extension("jpeg"), FileClass::Image);
        assert_eq!(classify_extension("png"), FileClass::Image);
        assert_eq!(classify_extension("webp"), FileClass::Image);
        assert_eq!(classify_extension("heic"), FileClass::Image);
        assert_eq!(classify_extension("heif"), FileClass::Image);
        assert_eq!(classify_extension("rw2"), FileClass::RawImage);
        assert_eq!(classify_extension("mp4"), FileClass::Video);
        assert_eq!(classify_extension("mov"), FileClass::Video);
        assert_eq!(classify_extension("aae"), FileClass::Sidecar);
        assert_eq!(classify_extension("xmp"), FileClass::Sidecar);
        assert_eq!(classify_extension("zip"), FileClass::Archive);
        assert_eq!(classify_extension("7z"), FileClass::Archive);
        assert_eq!(classify_extension("rar"), FileClass::Archive);
        assert_eq!(classify_extension("txt"), FileClass::Other);
    }

    #[test]
    fn thumbnail_is_generated_as_jpeg() {
        let dir = tempdir().unwrap();
        let src = dir.path().join("src.png");
        let thumbs = dir.path().join("thumbs");
        std::fs::create_dir_all(&thumbs).unwrap();
        save_test_image(&src, 200, 200, 42).unwrap();

        let sha256 = "aabbcc";
        let result = compute_visual_features(&src, &thumbs, sha256).unwrap();
        assert!(
            result.thumbnail_path.ends_with(".jpg"),
            "thumbnail should be JPEG, got: {}",
            result.thumbnail_path
        );
        let thumb_file = std::path::Path::new(&result.thumbnail_path);
        assert!(thumb_file.exists(), "thumbnail file was not created");
    }
}
