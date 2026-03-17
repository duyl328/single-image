/// AI aesthetic scoring module.
///
/// Architecture:
///   EmbeddingProvider trait  →  StubEmbeddingProvider (DB features → 32-dim f32 vec)
///                            →  ClipEmbeddingProvider  (CLIP ViT-B/32 ONNX → 512-dim f32 vec)
///   AestheticModel trait     →  LinearAestheticModel  (gradient descent, no new deps)
use std::path::Path;
use std::sync::Mutex;

use anyhow::{anyhow, Result};
use ort::session::Session;
use ort::value::TensorRef;
use rusqlite::{Connection, OptionalExtension};
use serde::{Deserialize, Serialize};

// ── Output types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct AiPredictionOutput {
    /// 0.0–1.0, normalised aesthetic score
    pub score: f32,
    /// 2 * |score - 0.5|, higher = more confident
    pub confidence: f32,
    /// "low" (<0.33) / "maybe" (0.33–0.67) / "high" (>0.67)
    pub bucket: String,
    /// score < 0.2 && confidence > 0.6
    pub delete_candidate: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrainingMetrics {
    pub mse: f32,
    pub mae: f32,
    pub sample_count: usize,
}

// ── EmbeddingProvider trait ──────────────────────────────────────────────────

pub trait EmbeddingProvider: Send + Sync {
    fn encoder_name(&self) -> &str;
    fn encoder_version(&self) -> &str;
    fn embedding_dim(&self) -> usize;
    /// Read already-computed DB fields for content_asset_id and return a feature vector.
    /// Does NOT decode image pixels — relies on pre-computed phash/dhash/quality/resolution.
    fn extract_for_content(&self, conn: &Connection, content_asset_id: i64) -> Result<Vec<f32>>;
}

// ── StubEmbeddingProvider ─────────────────────────────────────────────────────

/// First-version provider: 32-dim feature vector built from phash bits (16),
/// dhash bits (12), quality_score (1), log(megapixels) (1), and 2 padding zeros.
///
/// Replacement path: swap in SigLip2EmbeddingProvider (reads thumbnail_path,
/// resizes to 384×384, runs ONNX inference, returns 1152-dim vec).
pub struct StubEmbeddingProvider;

impl EmbeddingProvider for StubEmbeddingProvider {
    fn encoder_name(&self) -> &str {
        "stub_v1"
    }
    fn encoder_version(&self) -> &str {
        "1"
    }
    fn embedding_dim(&self) -> usize {
        32
    }

    fn extract_for_content(&self, conn: &Connection, content_asset_id: i64) -> Result<Vec<f32>> {
        let row: Option<(Option<String>, Option<String>, Option<f64>, Option<i64>, Option<i64>)> =
            conn.query_row(
                "SELECT phash, dhash, quality_score, width, height \
                 FROM content_assets WHERE id = ?1",
                [content_asset_id],
                |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                    ))
                },
            )
            .optional()?;

        let (phash, dhash, quality, width, height) = row
            .ok_or_else(|| anyhow!("content_asset {} not found", content_asset_id))?;

        let mut vec: Vec<f32> = Vec::with_capacity(32);

        // 16 features from phash hex string (each hex char → 4 bits → 4 floats 0/1)
        let phash_bits = hex_to_bits(phash.as_deref().unwrap_or(""), 16);
        vec.extend_from_slice(&phash_bits);

        // 12 features from dhash
        let dhash_bits = hex_to_bits(dhash.as_deref().unwrap_or(""), 12);
        vec.extend_from_slice(&dhash_bits);

        // 1 feature: quality_score normalised to 0–1 (scores are typically 0–100)
        let q = quality.unwrap_or(50.0) as f32 / 100.0;
        vec.push(q.clamp(0.0, 1.0));

        // 1 feature: log(megapixels + 1), normalised by log(50)  (50 MP ≈ high-end)
        let mp = match (width, height) {
            (Some(w), Some(h)) => (w * h) as f64 / 1_000_000.0,
            _ => 1.0,
        };
        let log_mp = (mp as f32 + 1.0).ln() / (51.0_f32).ln();
        vec.push(log_mp.clamp(0.0, 1.0));

        // 2 padding zeros to reach dim 32
        vec.push(0.0);
        vec.push(0.0);

        debug_assert_eq!(vec.len(), 32);
        Ok(vec)
    }
}

// ── ClipEmbeddingProvider ─────────────────────────────────────────────────────

/// CLIP ViT-B/32 ONNX provider: loads visual.onnx and returns 512-dim embeddings.
/// encoder_name = "clip_vitb32", encoder_version = "1"
pub struct ClipEmbeddingProvider {
    session: Mutex<Session>,
}

impl ClipEmbeddingProvider {
    pub fn load(model_path: &Path) -> Result<Self> {
        let session = Session::builder()?.commit_from_file(model_path)?;
        Ok(Self {
            session: Mutex::new(session),
        })
    }

    pub fn model_filename() -> &'static str {
        "clip_vitb32_visual.onnx"
    }

    pub fn download_url() -> &'static str {
        "https://huggingface.co/immich-app/ViT-B-32__openai/resolve/main/visual/model.onnx"
    }
}

impl EmbeddingProvider for ClipEmbeddingProvider {
    fn encoder_name(&self) -> &str {
        "clip_vitb32"
    }
    fn encoder_version(&self) -> &str {
        "1"
    }
    fn embedding_dim(&self) -> usize {
        512
    }

    fn extract_for_content(&self, conn: &Connection, content_asset_id: i64) -> Result<Vec<f32>> {
        // 1. Get thumbnail path (preferred) or current path
        let paths: Option<(Option<String>, String)> = conn
            .query_row(
                "SELECT ca.thumbnail_path, fi.current_path \
                 FROM file_instances fi \
                 JOIN content_assets ca ON ca.id = fi.content_asset_id \
                 WHERE fi.content_asset_id = ?1 AND fi.exists_flag = 1 \
                 LIMIT 1",
                [content_asset_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        let (thumbnail_path, current_path) = paths.ok_or_else(|| {
            anyhow!("no active file instance for content_asset {}", content_asset_id)
        })?;

        let img_path = thumbnail_path.unwrap_or(current_path);

        // 2. Load image
        let img = image::open(&img_path)
            .map_err(|e| anyhow!("failed to open image {}: {}", img_path, e))?;

        // 3. Resize to 224×224 and convert to RGB
        let img = img.resize_exact(224, 224, image::imageops::FilterType::Triangle);
        let rgb = img.to_rgb8();

        // 4. Normalize with CLIP-specific stats (not ImageNet)
        let mean = [0.48145466_f32, 0.4578275, 0.40821073];
        let std = [0.26862954_f32, 0.26130258, 0.27577711];

        // 5. Build CHW tensor [1, 3, 224, 224]
        let mut arr = ndarray::Array4::<f32>::zeros((1, 3, 224, 224));
        for y in 0..224_usize {
            for x in 0..224_usize {
                let pixel = rgb.get_pixel(x as u32, y as u32);
                for c in 0..3_usize {
                    let val = pixel[c] as f32 / 255.0;
                    arr[[0, c, y, x]] = (val - mean[c]) / std[c];
                }
            }
        }

        // 6. Run ONNX inference
        let mut session = self
            .session
            .lock()
            .map_err(|_| anyhow!("clip session lock poisoned"))?;
        let input_data = arr
            .as_slice()
            .ok_or_else(|| anyhow!("clip input tensor is not contiguous"))?;
        let outputs = session
            .run(ort::inputs![TensorRef::from_array_view(([1usize, 3, 224, 224], input_data))?])
            .map_err(|e| anyhow!("ONNX inference failed: {}", e))?;

        // 7. Extract first output as 512-dim embedding
        let (_, output_tensor) = outputs[0]
            .try_extract_tensor::<f32>()
            .map_err(|e| anyhow!("failed to extract output tensor: {}", e))?;
        let mut embedding: Vec<f32> = output_tensor.to_vec();

        if embedding.len() != 512 {
            return Err(anyhow!(
                "expected 512-dim embedding, got {}",
                embedding.len()
            ));
        }

        l2_normalize(&mut embedding);
        Ok(embedding)
    }
}

/// Decode up to `max_features` bit-values from a hex string (MSB first per nibble).
fn hex_to_bits(hex: &str, max_features: usize) -> Vec<f32> {
    let mut bits: Vec<f32> = Vec::with_capacity(max_features);
    for ch in hex.chars() {
        if bits.len() >= max_features {
            break;
        }
        if let Some(nibble) = ch.to_digit(16) {
            for shift in (0..4).rev() {
                if bits.len() >= max_features {
                    break;
                }
                bits.push(((nibble >> shift) & 1) as f32);
            }
        }
    }
    // Pad with 0 if hex string was shorter than expected
    while bits.len() < max_features {
        bits.push(0.0);
    }
    bits
}

// ── AestheticModel trait ─────────────────────────────────────────────────────

pub trait AestheticModel: Send + Sync {
    fn head_type(&self) -> &str;
    fn predict(&self, embedding: &[f32]) -> Result<AiPredictionOutput>;
    fn train(&mut self, samples: &[(Vec<f32>, f32)]) -> Result<TrainingMetrics>;
    fn save_to_path(&self, path: &Path) -> Result<()>;
}

// ── LinearAestheticModel ──────────────────────────────────────────────────────

/// Pure-Rust gradient descent linear regression with L2 regularisation.
/// Weights are stored as JSON for easy inspection and portability.
///
/// head_type = "linear_v1"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinearAestheticModel {
    pub weights: Vec<f32>,
    pub bias: f32,
}

impl LinearAestheticModel {
    pub fn new(dim: usize) -> Self {
        Self {
            weights: vec![0.0; dim],
            bias: 0.5,
        }
    }

    pub fn load_from_path(path: &Path) -> Result<Self> {
        let json = std::fs::read_to_string(path)?;
        let model: Self = serde_json::from_str(&json)?;
        Ok(model)
    }

    fn dot(&self, embedding: &[f32]) -> f32 {
        self.weights
            .iter()
            .zip(embedding.iter())
            .map(|(w, x)| w * x)
            .sum::<f32>()
            + self.bias
    }
}

impl AestheticModel for LinearAestheticModel {
    fn head_type(&self) -> &str {
        "linear_v1"
    }

    fn predict(&self, embedding: &[f32]) -> Result<AiPredictionOutput> {
        // Sigmoid activation to keep score in [0, 1]
        let logit = self.dot(embedding);
        let score = sigmoid(logit);
        let confidence = 2.0 * (score - 0.5).abs();
        let bucket = if score < 0.33 {
            "low".to_string()
        } else if score < 0.67 {
            "maybe".to_string()
        } else {
            "high".to_string()
        };
        let delete_candidate = score < 0.2 && confidence > 0.6;
        Ok(AiPredictionOutput {
            score,
            confidence,
            bucket,
            delete_candidate,
        })
    }

    fn train(&mut self, samples: &[(Vec<f32>, f32)]) -> Result<TrainingMetrics> {
        if samples.is_empty() {
            return Err(anyhow!("no training samples"));
        }
        let n = samples.len();
        let dim = self.weights.len();

        // Normalise labels to [0, 1] (input is user rating 1–5)
        let normalised: Vec<(Vec<f32>, f32)> = samples
            .iter()
            .map(|(x, y)| (x.clone(), (y.clamp(1.0, 5.0) - 1.0) / 4.0))
            .collect();

        let lr = 0.01_f32;
        let lambda = 0.001_f32; // L2 regularisation
        let epochs = 500;

        for _ in 0..epochs {
            let mut dw = vec![0.0_f32; dim];
            let mut db = 0.0_f32;

            for (x, y) in &normalised {
                let pred = sigmoid(self.dot(x));
                let err = pred - y;
                for j in 0..dim {
                    dw[j] += err * x[j];
                }
                db += err;
            }

            for j in 0..dim {
                self.weights[j] -= lr * (dw[j] / n as f32 + lambda * self.weights[j]);
            }
            self.bias -= lr * db / n as f32;
        }

        // Compute metrics on training set
        let mut mse_sum = 0.0_f32;
        let mut mae_sum = 0.0_f32;
        for (x, y) in &normalised {
            let pred = sigmoid(self.dot(x));
            let diff = pred - y;
            mse_sum += diff * diff;
            mae_sum += diff.abs();
        }

        Ok(TrainingMetrics {
            mse: mse_sum / n as f32,
            mae: mae_sum / n as f32,
            sample_count: n,
        })
    }

    fn save_to_path(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}

// ── MlpAestheticModel ─────────────────────────────────────────────────────────

/// Small two-layer MLP head trained on top of CLIP / stub embeddings.
///
/// head_type = "mlp_v1"
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MlpAestheticModel {
    pub input_dim: usize,
    pub hidden_dim: usize,
    pub input_hidden_weights: Vec<f32>,
    pub hidden_biases: Vec<f32>,
    pub hidden_output_weights: Vec<f32>,
    pub output_bias: f32,
}

impl MlpAestheticModel {
    pub fn new(input_dim: usize) -> Self {
        let hidden_dim = (input_dim / 24).clamp(8, 32);
        let mut input_hidden_weights = vec![0.0; input_dim * hidden_dim];
        let input_scale = (2.0_f32 / input_dim.max(1) as f32).sqrt() * 0.35;
        for (idx, weight) in input_hidden_weights.iter_mut().enumerate() {
            *weight = deterministic_weight(idx, input_scale);
        }

        let mut hidden_output_weights = vec![0.0; hidden_dim];
        let hidden_scale = (2.0_f32 / hidden_dim.max(1) as f32).sqrt() * 0.35;
        for (idx, weight) in hidden_output_weights.iter_mut().enumerate() {
            *weight = deterministic_weight(idx + input_hidden_weights.len(), hidden_scale);
        }

        Self {
            input_dim,
            hidden_dim,
            input_hidden_weights,
            hidden_biases: vec![0.0; hidden_dim],
            hidden_output_weights,
            output_bias: 0.0,
        }
    }

    pub fn load_from_path(path: &Path) -> Result<Self> {
        let json = std::fs::read_to_string(path)?;
        let model: Self = serde_json::from_str(&json)?;
        Ok(model)
    }

    fn forward(&self, embedding: &[f32]) -> (Vec<f32>, Vec<f32>, f32, f32) {
        let input = normalized_embedding(embedding);
        let mut hidden_pre = vec![0.0; self.hidden_dim];
        let mut hidden = vec![0.0; self.hidden_dim];

        for j in 0..self.hidden_dim {
            let row_start = j * self.input_dim;
            let row_end = row_start + self.input_dim;
            let row = &self.input_hidden_weights[row_start..row_end];
            let sum = row
                .iter()
                .zip(input.iter())
                .map(|(w, x)| w * x)
                .sum::<f32>()
                + self.hidden_biases[j];
            hidden_pre[j] = sum;
            hidden[j] = sum.max(0.0);
        }

        let logit = self
            .hidden_output_weights
            .iter()
            .zip(hidden.iter())
            .map(|(w, x)| w * x)
            .sum::<f32>()
            + self.output_bias;
        let score = sigmoid(logit);
        (input, hidden, logit, score)
    }
}

impl AestheticModel for MlpAestheticModel {
    fn head_type(&self) -> &str {
        "mlp_v1"
    }

    fn predict(&self, embedding: &[f32]) -> Result<AiPredictionOutput> {
        if embedding.len() != self.input_dim {
            return Err(anyhow!(
                "expected {}-dim embedding, got {}",
                self.input_dim,
                embedding.len()
            ));
        }

        let (_, hidden, _, score) = self.forward(embedding);
        let hidden_energy =
            hidden.iter().map(|v| v.abs()).sum::<f32>() / self.hidden_dim.max(1) as f32;
        let confidence = (2.0 * (score - 0.5).abs() + hidden_energy * 0.15).clamp(0.0, 1.0);
        let bucket = if score < 0.33 {
            "low".to_string()
        } else if score < 0.67 {
            "maybe".to_string()
        } else {
            "high".to_string()
        };
        let delete_candidate = score < 0.2 && confidence > 0.6;
        Ok(AiPredictionOutput {
            score,
            confidence,
            bucket,
            delete_candidate,
        })
    }

    fn train(&mut self, samples: &[(Vec<f32>, f32)]) -> Result<TrainingMetrics> {
        if samples.is_empty() {
            return Err(anyhow!("no training samples"));
        }

        let n = samples.len() as f32;
        let input_dim = self.input_dim;
        let hidden_dim = self.hidden_dim;
        let lr = 0.03_f32;
        let lambda = 0.0005_f32;
        let epochs = 140;

        let normalised: Vec<(Vec<f32>, f32)> = samples
            .iter()
            .map(|(x, y)| {
                if x.len() != input_dim {
                    return Err(anyhow!(
                        "sample embedding has dim {}, expected {}",
                        x.len(),
                        input_dim
                    ));
                }
                Ok((normalized_embedding(x), (y.clamp(1.0, 5.0) - 1.0) / 4.0))
            })
            .collect::<Result<Vec<_>>>()?;

        for _ in 0..epochs {
            let mut grad_input_hidden = vec![0.0_f32; self.input_hidden_weights.len()];
            let mut grad_hidden_biases = vec![0.0_f32; hidden_dim];
            let mut grad_hidden_output = vec![0.0_f32; hidden_dim];
            let mut grad_output_bias = 0.0_f32;

            for (x, y) in &normalised {
                let mut hidden_pre = vec![0.0_f32; hidden_dim];
                let mut hidden = vec![0.0_f32; hidden_dim];
                for j in 0..hidden_dim {
                    let row_start = j * input_dim;
                    let row_end = row_start + input_dim;
                    let row = &self.input_hidden_weights[row_start..row_end];
                    let sum = row
                        .iter()
                        .zip(x.iter())
                        .map(|(w, v)| w * v)
                        .sum::<f32>()
                        + self.hidden_biases[j];
                    hidden_pre[j] = sum;
                    hidden[j] = sum.max(0.0);
                }

                let logit = self
                    .hidden_output_weights
                    .iter()
                    .zip(hidden.iter())
                    .map(|(w, v)| w * v)
                    .sum::<f32>()
                    + self.output_bias;
                let pred = sigmoid(logit);
                let err = (pred - y).clamp(-1.0, 1.0);

                for j in 0..hidden_dim {
                    grad_hidden_output[j] += err * hidden[j];
                }
                grad_output_bias += err;

                for j in 0..hidden_dim {
                    let delta = if hidden_pre[j] > 0.0 {
                        err * self.hidden_output_weights[j]
                    } else {
                        0.0
                    };
                    grad_hidden_biases[j] += delta;
                    let row_start = j * input_dim;
                    let row_end = row_start + input_dim;
                    let grad_row = &mut grad_input_hidden[row_start..row_end];
                    for (g, value) in grad_row.iter_mut().zip(x.iter()) {
                        *g += delta * value;
                    }
                }
            }

            for (weight, grad) in self
                .input_hidden_weights
                .iter_mut()
                .zip(grad_input_hidden.iter())
            {
                *weight -= lr * (grad / n + lambda * *weight);
            }
            for (bias, grad) in self.hidden_biases.iter_mut().zip(grad_hidden_biases.iter()) {
                *bias -= lr * grad / n;
            }
            for (weight, grad) in self
                .hidden_output_weights
                .iter_mut()
                .zip(grad_hidden_output.iter())
            {
                *weight -= lr * (grad / n + lambda * *weight);
            }
            self.output_bias -= lr * grad_output_bias / n;
        }

        let mut mse_sum = 0.0_f32;
        let mut mae_sum = 0.0_f32;
        for (x, y) in &normalised {
            let (_, _, _, pred) = self.forward(x);
            let diff = pred - y;
            mse_sum += diff * diff;
            mae_sum += diff.abs();
        }

        Ok(TrainingMetrics {
            mse: mse_sum / n,
            mae: mae_sum / n,
            sample_count: samples.len(),
        })
    }

    fn save_to_path(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum LoadedAestheticModel {
    Linear(LinearAestheticModel),
    Mlp(MlpAestheticModel),
}

impl AestheticModel for LoadedAestheticModel {
    fn head_type(&self) -> &str {
        match self {
            Self::Linear(model) => model.head_type(),
            Self::Mlp(model) => model.head_type(),
        }
    }

    fn predict(&self, embedding: &[f32]) -> Result<AiPredictionOutput> {
        match self {
            Self::Linear(model) => model.predict(embedding),
            Self::Mlp(model) => model.predict(embedding),
        }
    }

    fn train(&mut self, samples: &[(Vec<f32>, f32)]) -> Result<TrainingMetrics> {
        match self {
            Self::Linear(model) => model.train(samples),
            Self::Mlp(model) => model.train(samples),
        }
    }

    fn save_to_path(&self, path: &Path) -> Result<()> {
        match self {
            Self::Linear(model) => model.save_to_path(path),
            Self::Mlp(model) => model.save_to_path(path),
        }
    }
}

pub fn load_aesthetic_model(path: &Path, head_type: &str) -> Result<LoadedAestheticModel> {
    match head_type {
        "linear_v1" => Ok(LoadedAestheticModel::Linear(
            LinearAestheticModel::load_from_path(path)?,
        )),
        "mlp_v1" => Ok(LoadedAestheticModel::Mlp(MlpAestheticModel::load_from_path(path)?)),
        other => Err(anyhow!("unsupported aesthetic head_type: {}", other)),
    }
}

#[derive(Debug, Clone)]
pub struct RankPreferenceSample {
    pub better_embedding: Vec<f32>,
    pub worse_embedding: Vec<f32>,
    pub weight: f32,
}

#[derive(Debug, Clone)]
pub struct RankTieSample {
    pub left_embedding: Vec<f32>,
    pub right_embedding: Vec<f32>,
    pub weight: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankTrainingMetrics {
    pub pairwise_loss: f32,
    pub tie_loss: f32,
    pub preference_pair_count: usize,
    pub tie_pair_count: usize,
    pub preference_accuracy: f32,
    pub weak_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairwiseRankModel {
    pub input_dim: usize,
    pub hidden_dim: usize,
    pub input_hidden_weights: Vec<f32>,
    pub hidden_biases: Vec<f32>,
    pub hidden_output_weights: Vec<f32>,
    pub output_bias: f32,
}

impl PairwiseRankModel {
    pub fn new(input_dim: usize) -> Self {
        let hidden_dim = (input_dim / 16).clamp(16, 64);
        let mut input_hidden_weights = vec![0.0; input_dim * hidden_dim];
        let input_scale = (2.0_f32 / input_dim.max(1) as f32).sqrt() * 0.28;
        for (idx, weight) in input_hidden_weights.iter_mut().enumerate() {
            *weight = deterministic_weight(idx + 97, input_scale);
        }

        let mut hidden_output_weights = vec![0.0; hidden_dim];
        let hidden_scale = (2.0_f32 / hidden_dim.max(1) as f32).sqrt() * 0.25;
        for (idx, weight) in hidden_output_weights.iter_mut().enumerate() {
            *weight = deterministic_weight(idx + input_hidden_weights.len() + 193, hidden_scale);
        }

        Self {
            input_dim,
            hidden_dim,
            input_hidden_weights,
            hidden_biases: vec![0.0; hidden_dim],
            hidden_output_weights,
            output_bias: 0.0,
        }
    }

    pub fn load_from_path(path: &Path) -> Result<Self> {
        let json = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&json)?)
    }

    pub fn save_to_path(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    fn forward_normalized(&self, embedding: &[f32]) -> Result<(Vec<f32>, Vec<f32>, Vec<f32>, f32)> {
        if embedding.len() != self.input_dim {
            return Err(anyhow!(
                "expected {}-dim embedding, got {}",
                self.input_dim,
                embedding.len()
            ));
        }

        let input = normalized_embedding(embedding);
        let mut hidden_pre = vec![0.0_f32; self.hidden_dim];
        let mut hidden = vec![0.0_f32; self.hidden_dim];

        for j in 0..self.hidden_dim {
            let row_start = j * self.input_dim;
            let row_end = row_start + self.input_dim;
            let row = &self.input_hidden_weights[row_start..row_end];
            let sum = row
                .iter()
                .zip(input.iter())
                .map(|(w, x)| w * x)
                .sum::<f32>()
                + self.hidden_biases[j];
            hidden_pre[j] = sum;
            hidden[j] = sum.max(0.0);
        }

        let score = self
            .hidden_output_weights
            .iter()
            .zip(hidden.iter())
            .map(|(w, x)| w * x)
            .sum::<f32>()
            + self.output_bias;
        Ok((input, hidden_pre, hidden, score))
    }

    fn accumulate_score_gradient(
        &self,
        input: &[f32],
        hidden_pre: &[f32],
        hidden: &[f32],
        grad_output: f32,
        grad_input_hidden: &mut [f32],
        grad_hidden_biases: &mut [f32],
        grad_hidden_output: &mut [f32],
        grad_output_bias: &mut f32,
    ) {
        for j in 0..self.hidden_dim {
            grad_hidden_output[j] += grad_output * hidden[j];
        }
        *grad_output_bias += grad_output;

        for j in 0..self.hidden_dim {
            let delta = if hidden_pre[j] > 0.0 {
                grad_output * self.hidden_output_weights[j]
            } else {
                0.0
            };
            grad_hidden_biases[j] += delta;
            let row_start = j * self.input_dim;
            let row_end = row_start + self.input_dim;
            let grad_row = &mut grad_input_hidden[row_start..row_end];
            for (grad, value) in grad_row.iter_mut().zip(input.iter()) {
                *grad += delta * value;
            }
        }
    }

    pub fn predict_score(&self, embedding: &[f32]) -> Result<f32> {
        let (_, _, _, score) = self.forward_normalized(embedding)?;
        Ok(score)
    }

    pub fn train(
        &mut self,
        preference_samples: &[RankPreferenceSample],
        tie_samples: &[RankTieSample],
    ) -> Result<RankTrainingMetrics> {
        if preference_samples.is_empty() && tie_samples.is_empty() {
            return Err(anyhow!("no ranking samples"));
        }

        let lr = 0.02_f32;
        let lambda = 0.0004_f32;
        let epochs = 90;
        let sample_norm = (preference_samples.len() + tie_samples.len()).max(1) as f32;

        for _ in 0..epochs {
            let mut grad_input_hidden = vec![0.0_f32; self.input_hidden_weights.len()];
            let mut grad_hidden_biases = vec![0.0_f32; self.hidden_dim];
            let mut grad_hidden_output = vec![0.0_f32; self.hidden_dim];
            let mut grad_output_bias = 0.0_f32;

            for sample in preference_samples {
                let (better_input, better_hidden_pre, better_hidden, better_score) =
                    self.forward_normalized(&sample.better_embedding)?;
                let (worse_input, worse_hidden_pre, worse_hidden, worse_score) =
                    self.forward_normalized(&sample.worse_embedding)?;
                let diff = better_score - worse_score;
                let grad_diff = (sigmoid(diff) - 1.0) * sample.weight;

                self.accumulate_score_gradient(
                    &better_input,
                    &better_hidden_pre,
                    &better_hidden,
                    grad_diff,
                    &mut grad_input_hidden,
                    &mut grad_hidden_biases,
                    &mut grad_hidden_output,
                    &mut grad_output_bias,
                );
                self.accumulate_score_gradient(
                    &worse_input,
                    &worse_hidden_pre,
                    &worse_hidden,
                    -grad_diff,
                    &mut grad_input_hidden,
                    &mut grad_hidden_biases,
                    &mut grad_hidden_output,
                    &mut grad_output_bias,
                );
            }

            for sample in tie_samples {
                let (left_input, left_hidden_pre, left_hidden, left_score) =
                    self.forward_normalized(&sample.left_embedding)?;
                let (right_input, right_hidden_pre, right_hidden, right_score) =
                    self.forward_normalized(&sample.right_embedding)?;
                let diff = left_score - right_score;
                let grad_diff = diff * sample.weight;

                self.accumulate_score_gradient(
                    &left_input,
                    &left_hidden_pre,
                    &left_hidden,
                    grad_diff,
                    &mut grad_input_hidden,
                    &mut grad_hidden_biases,
                    &mut grad_hidden_output,
                    &mut grad_output_bias,
                );
                self.accumulate_score_gradient(
                    &right_input,
                    &right_hidden_pre,
                    &right_hidden,
                    -grad_diff,
                    &mut grad_input_hidden,
                    &mut grad_hidden_biases,
                    &mut grad_hidden_output,
                    &mut grad_output_bias,
                );
            }

            for (weight, grad) in self
                .input_hidden_weights
                .iter_mut()
                .zip(grad_input_hidden.iter())
            {
                *weight -= lr * (grad / sample_norm + lambda * *weight);
            }
            for (bias, grad) in self.hidden_biases.iter_mut().zip(grad_hidden_biases.iter()) {
                *bias -= lr * grad / sample_norm;
            }
            for (weight, grad) in self
                .hidden_output_weights
                .iter_mut()
                .zip(grad_hidden_output.iter())
            {
                *weight -= lr * (grad / sample_norm + lambda * *weight);
            }
            self.output_bias -= lr * grad_output_bias / sample_norm;
        }

        let mut pairwise_loss = 0.0_f32;
        let mut pairwise_correct = 0usize;
        for sample in preference_samples {
            let better = self.predict_score(&sample.better_embedding)?;
            let worse = self.predict_score(&sample.worse_embedding)?;
            let diff = better - worse;
            pairwise_loss += (1.0 + (-diff).exp()).ln() * sample.weight;
            if diff > 0.0 {
                pairwise_correct += 1;
            }
        }

        let mut tie_loss = 0.0_f32;
        for sample in tie_samples {
            let left = self.predict_score(&sample.left_embedding)?;
            let right = self.predict_score(&sample.right_embedding)?;
            let diff = left - right;
            tie_loss += 0.5 * diff * diff * sample.weight;
        }

        Ok(RankTrainingMetrics {
            pairwise_loss: pairwise_loss / preference_samples.len().max(1) as f32,
            tie_loss: tie_loss / tie_samples.len().max(1) as f32,
            preference_pair_count: preference_samples.len(),
            tie_pair_count: tie_samples.len(),
            preference_accuracy: pairwise_correct as f32 / preference_samples.len().max(1) as f32,
            weak_only: preference_samples.is_empty(),
        })
    }
}

pub fn load_rank_model(path: &Path) -> Result<PairwiseRankModel> {
    PairwiseRankModel::load_from_path(path)
}

fn sigmoid(x: f32) -> f32 {
    1.0 / (1.0 + (-x).exp())
}

fn l2_normalize(values: &mut [f32]) {
    let norm = values.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm > 1e-6 {
        for value in values.iter_mut() {
            *value /= norm;
        }
    }
}

fn normalized_embedding(values: &[f32]) -> Vec<f32> {
    let mut normalized = values.to_vec();
    l2_normalize(&mut normalized);
    normalized
}

fn deterministic_weight(index: usize, scale: f32) -> f32 {
    ((index as f32 + 1.0) * 0.618_034).sin() * scale
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_to_bits_correct_length() {
        let bits = hex_to_bits("ff00", 16);
        assert_eq!(bits.len(), 16);
        // "f" = 1111, "f" = 1111
        assert_eq!(&bits[0..8], &[1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]);
        // "0" = 0000, "0" = 0000
        assert_eq!(&bits[8..16], &[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]);
    }

    #[test]
    fn linear_model_train_predict() {
        // Build trivial 2-feature dataset: high x[0] → high score (5 stars)
        let samples: Vec<(Vec<f32>, f32)> = vec![
            (vec![1.0, 0.0], 5.0),
            (vec![1.0, 0.0], 4.0),
            (vec![0.0, 1.0], 1.0),
            (vec![0.0, 1.0], 2.0),
        ];
        let mut model = LinearAestheticModel::new(2);
        let metrics = model.train(&samples).unwrap();
        assert!(metrics.mse < 0.1, "mse should be low after training");

        let high_out = model.predict(&[1.0, 0.0]).unwrap();
        let low_out = model.predict(&[0.0, 1.0]).unwrap();
        assert!(
            high_out.score > low_out.score,
            "high-feature sample should score higher"
        );
    }

    #[test]
    fn linear_model_save_load() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("model.json");
        let mut model = LinearAestheticModel::new(4);
        model.weights = vec![0.1, 0.2, 0.3, 0.4];
        model.bias = 0.05;
        model.save_to_path(&path).unwrap();

        let loaded = LinearAestheticModel::load_from_path(&path).unwrap();
        assert_eq!(loaded.weights, model.weights);
        assert!((loaded.bias - model.bias).abs() < 1e-6);
    }

    #[test]
    fn mlp_model_train_predict() {
        let samples: Vec<(Vec<f32>, f32)> = vec![
            (vec![1.0, 0.0, 0.1, 0.0], 5.0),
            (vec![0.9, 0.0, 0.0, 0.2], 4.0),
            (vec![0.0, 1.0, 0.2, 0.0], 1.0),
            (vec![0.0, 0.8, 0.0, 0.1], 2.0),
        ];
        let mut model = MlpAestheticModel::new(4);
        let metrics = model.train(&samples).unwrap();
        assert!(metrics.mse < 0.2, "mlp mse should be low after training");

        let high_out = model.predict(&[1.0, 0.0, 0.0, 0.0]).unwrap();
        let low_out = model.predict(&[0.0, 1.0, 0.0, 0.0]).unwrap();
        assert!(high_out.score > low_out.score);
    }

    #[test]
    fn mlp_model_save_load() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("mlp.json");
        let model = MlpAestheticModel::new(8);
        model.save_to_path(&path).unwrap();

        let loaded = MlpAestheticModel::load_from_path(&path).unwrap();
        assert_eq!(loaded.input_dim, model.input_dim);
        assert_eq!(loaded.hidden_dim, model.hidden_dim);
        assert_eq!(loaded.input_hidden_weights.len(), model.input_hidden_weights.len());
        assert_eq!(loaded.hidden_output_weights.len(), model.hidden_output_weights.len());
    }
}
