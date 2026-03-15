# Single Image

本地优先的 Windows 照片整理工具，用来扫描你指定的目录，建立全局内容索引，并把 `完全重复`、`高相似度照片`、`RAW + 导出图` 分组展示出来，给出默认保留建议，再由你确认是否移到回收站。

## 当前能力

- 指定一组目录发起扫描，不要求先注册整库。
- 用 `内容资产 + 文件实例` 双层模型保存历史，支持文件移动、重命名、跨盘迁移后继续追踪。
- 用 `SHA-256` 识别完全重复。
- 用 `感知哈希 + SSIM` 检测视觉相似照片。
- 为每个分组给出默认保留项，并支持人工确认后移到 Windows 回收站。
- 记录路径历史、扫描历史和审核动作。
- 汇总本次扫描里出现但暂未处理的扩展名。

## 当前格式范围

- 已纳入扫描：`jpg` `jpeg` `png` `webp` `heic` `heif` `rw2`
- 已支持缩略图/相似度/质量评分：`jpg` `jpeg` `png` `webp`
- `heic` `heif` `rw2` 当前已支持索引、全量哈希、重复检测和 RAW/JPEG 同组保护；预览解码链路留作下一步增强。

## 技术栈

- 前端：`React 19 + TypeScript + Vite`
- 桌面壳：`Tauri 2`
- 后端：`Rust + SQLite`

## 本地运行

1. 安装前端依赖

```bash
pnpm install --ignore-scripts
```

2. 启动前端开发服务器

```bash
pnpm dev
```

3. 启动 Tauri 桌面端

```bash
pnpm desktop:dev
```

说明：

- `pnpm tauri` 通过 `pnpm dlx @tauri-apps/cli@2` 按需拉起 Tauri CLI，不把它作为常驻依赖。
- 不要直接执行 `pnpm run tauri`；那只会打印 Tauri CLI 帮助。要么用 `pnpm tauri dev`，要么直接用上面的 `pnpm desktop:dev`。
- 首次运行 `pnpm tauri dev` 或 `pnpm tauri build` 时，Tauri CLI 可能会下载额外平台工具。

## 构建

仅构建前端产物：

```bash
pnpm build
```

构建桌面应用：

```bash
pnpm desktop:build --debug
```

Windows 首次打包时，Tauri 可能会额外下载 WiX。若网络超时，Rust 可执行文件通常仍会先生成在：

```text
src-tauri/target/debug/single-image.exe
```

## 已验证

- `cargo test --target-dir target-check`
- `vite build`
- `pnpm tauri info`

另外，`pnpm tauri build --debug` 已经验证到应用可执行文件生成成功；本机失败点是后续 WiX 下载超时，不是项目代码编译失败。
