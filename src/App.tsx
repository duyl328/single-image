import {
  startTransition,
  useDeferredValue,
  useEffect,
  useState,
} from "react";
import { convertFileSrc } from "@tauri-apps/api/core";
import { open } from "@tauri-apps/plugin-dialog";
import "./App.css";

import {
  applyDecision,
  cancelScan,
  loadActions,
  loadGroup,
  loadGroups,
  loadHistory,
  loadScanStatus,
  loadSnapshot,
  loadUnknownFormats,
  startScan,
} from "./api";
import type {
  GroupDetail,
  GroupMember,
  GroupingProgress,
  GroupSummary,
  MatchKind,
  PathHistoryItem,
  ReviewActionSummary,
  ReviewStatus,
  ScanActiveItem,
  ScanProgress,
  ScanRecentItem,
  ScanResult,
  UnknownFormatSummary,
} from "./types";

type AppTab = "scan" | "review" | "history";

const kindTabs: Array<{ label: string; value: MatchKind | null }> = [
  { label: "全部", value: null },
  { label: "完全重复", value: "exact" },
  { label: "视觉相似", value: "similar" },
  { label: "RAW + 导出", value: "raw_jpeg_set" },
];

const statusTabs: Array<{ label: string; value: ReviewStatus | null }> = [
  { label: "待处理", value: "pending" },
  { label: "已应用", value: "applied" },
  { label: "全部", value: null },
];

function App() {
  const [activeTab, setActiveTab] = useState<AppTab>("scan");
  const [snapshot, setSnapshot] = useState<{
    pendingGroupCount: number;
    appliedActionCount: number;
    indexedAssetCount: number;
    activeFileCount: number;
  } | null>(null);
  const [scanPaths, setScanPaths] = useState<string[]>([]);
  const [scanResult, setScanResult] = useState<ScanResult | null>(null);
  const [unknownFormats, setUnknownFormats] = useState<UnknownFormatSummary[]>([]);
  const [groups, setGroups] = useState<GroupSummary[]>([]);
  const [selectedGroupId, setSelectedGroupId] = useState<number | null>(null);
  const [selectedGroup, setSelectedGroup] = useState<GroupDetail | null>(null);
  const [actions, setActions] = useState<ReviewActionSummary[]>([]);
  const [pathHistory, setPathHistory] = useState<PathHistoryItem[]>([]);
  const [historyAssetId, setHistoryAssetId] = useState<number | null>(null);
  const [historyExpanded, setHistoryExpanded] = useState(false);
  const [filterKind, setFilterKind] = useState<MatchKind | null>(null);
  const [filterStatus, setFilterStatus] = useState<ReviewStatus | null>("pending");
  const [searchText, setSearchText] = useState("");
  const [recycleIds, setRecycleIds] = useState<Set<number>>(new Set());
  const [note, setNote] = useState("");
  const [scanThreads, setScanThreads] = useState(4);
  const [busyLabel, setBusyLabel] = useState<string | null>(null);
  const [scanProgress, setScanProgress] = useState<ScanProgress | null>(null);
  const [error, setError] = useState<string | null>(null);
  const deferredSearch = useDeferredValue(searchText);

  useEffect(() => {
    void refreshDashboard();
  }, [filterKind, filterStatus]);

  useEffect(() => {
    if (selectedGroupId == null) {
      setSelectedGroup(null);
      setRecycleIds(new Set());
      return;
    }

    let cancelled = false;
    void (async () => {
      try {
        const detail = await loadGroup(selectedGroupId);
        if (cancelled) return;

        setSelectedGroup(detail);
        const nextRecycle = new Set<number>();
        if (detail.kind !== "raw_jpeg_set") {
          detail.members.forEach((member) => {
            if (member.fileInstanceId !== detail.recommendedKeepInstanceId) {
              nextRecycle.add(member.fileInstanceId);
            }
          });
        }
        setRecycleIds(nextRecycle);
        setNote("");
        setHistoryExpanded(false);

        const leadMember =
          detail.members.find(
            (member) => member.fileInstanceId === detail.recommendedKeepInstanceId,
          ) ?? detail.members[0];
        setHistoryAssetId(leadMember?.contentAssetId ?? null);
      } catch (reason) {
        if (!cancelled) setError(String(reason));
      }
    })();

    return () => { cancelled = true; };
  }, [selectedGroupId]);

  useEffect(() => {
    if (historyAssetId == null) {
      setPathHistory([]);
      return;
    }

    let cancelled = false;
    void (async () => {
      try {
        const nextHistory = await loadHistory(historyAssetId);
        if (!cancelled) setPathHistory(nextHistory);
      } catch (reason) {
        if (!cancelled) setError(String(reason));
      }
    })();

    return () => { cancelled = true; };
  }, [historyAssetId]);

  const visibleGroups = groups.filter((group) => {
    if (!deferredSearch.trim()) return true;
    const query = deferredSearch.trim().toLowerCase();
    return (
      group.recommendationReason.toLowerCase().includes(query) ||
      group.anchor.toLowerCase().includes(query) ||
      (group.recommendedKeepPath ?? "").toLowerCase().includes(query)
    );
  });

  async function refreshDashboard() {
    try {
      setError(null);
      const [nextSnapshot, nextGroups, nextActions] = await Promise.all([
        loadSnapshot(),
        loadGroups({ kind: filterKind, status: filterStatus }),
        loadActions(),
      ]);
      setSnapshot(nextSnapshot);
      setGroups(nextGroups);
      setActions(nextActions);
      startTransition(() => {
        setSelectedGroupId((current) => {
          if (current && nextGroups.some((group) => group.id === current)) {
            return current;
          }
          return nextGroups[0]?.id ?? null;
        });
      });
    } catch (reason) {
      setError(String(reason));
    }
  }

  async function handlePickFolders() {
    try {
      const selection = await open({
        directory: true,
        multiple: true,
        title: "选择要扫描的照片目录",
      });
      if (!selection) return;
      const values = Array.isArray(selection) ? selection : [selection];
      setScanPaths((current) => Array.from(new Set([...current, ...values])));
    } catch (reason) {
      setError(String(reason));
    }
  }

  async function handleScan() {
    if (scanPaths.length === 0) {
      setError("请先选择至少一个目录。");
      return;
    }
    try {
      setBusyLabel("正在启动扫描任务…");
      setError(null);
      await startScan(scanPaths, scanThreads);
      setBusyLabel(null);
      const result = await waitForScanCompletion((progress) => {
        setScanProgress(progress);
      });
      setScanProgress(null);
      const unsupported = await loadUnknownFormats(result.scanRunId);
      setScanResult(result);
      setUnknownFormats(unsupported);
      setFilterStatus("pending");
      await refreshDashboard();
      setActiveTab("review");
    } catch (reason) {
      setScanProgress(null);
      setError(String(reason));
    } finally {
      setBusyLabel(null);
    }
  }

  async function handleApplyDecision() {
    if (!selectedGroup) return;
    const keepIds = selectedGroup.members
      .filter((member) => !recycleIds.has(member.fileInstanceId))
      .map((member) => member.fileInstanceId);
    const recycle = [...recycleIds];
    if (keepIds.length === 0) {
      setError("至少保留一个文件，不能整组全部回收。");
      return;
    }
    if (recycle.length === 0) {
      setError("请至少选择一个要回收的文件。");
      return;
    }
    try {
      setBusyLabel("正在写入决策并移动文件到回收站…");
      setError(null);
      await applyDecision(selectedGroup.id, {
          keepIds,
        recycleIds: recycle,
        note: note.trim() || null,
      });
      await refreshDashboard();
    } catch (reason) {
      setError(String(reason));
    } finally {
      setBusyLabel(null);
    }
  }

  function toggleRecycle(member: GroupMember) {
    setRecycleIds((current) => {
      const next = new Set(current);
      if (next.has(member.fileInstanceId)) {
        next.delete(member.fileInstanceId);
      } else {
        next.add(member.fileInstanceId);
      }
      return next;
    });
  }

  const pendingRecycleCount = recycleIds.size;
  const pendingKeepCount = selectedGroup
    ? selectedGroup.members.length - pendingRecycleCount
    : 0;

  async function waitForScanCompletion(
    onProgress: (progress: ScanProgress) => void,
  ): Promise<ScanResult> {
    while (true) {
      const progress = await loadScanStatus();
      onProgress(progress);

      if (progress.status === "completed" && progress.result) {
        return progress.result;
      }
      if (progress.status === "failed") {
        throw new Error(progress.error ?? "扫描失败");
      }

      await delay(250);
    }
  }

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <div className="sidebar-brand">
          <div className="brand-mark">SI</div>
          <div className="brand-info">
            <span className="brand-name">Single Image</span>
            <span className="brand-sub">本地照片整理</span>
          </div>
        </div>

        <nav className="sidebar-nav">
          {(["scan", "review", "history"] as AppTab[]).map((tab) => (
            <button
              key={tab}
              className={`nav-item ${activeTab === tab ? "nav-item--active" : ""}`}
              onClick={() => setActiveTab(tab)}
            >
              <span className="nav-item-icon">{tabIcon(tab)}</span>
              <span className="nav-item-label">{tabLabel(tab)}</span>
              {tab === "review" && (snapshot?.pendingGroupCount ?? 0) > 0 && (
                <span className="nav-badge">{snapshot!.pendingGroupCount}</span>
              )}
            </button>
          ))}
        </nav>

        <div className="sidebar-divider" />

        <div className="sidebar-stats">
          <SidebarStat
            value={snapshot?.pendingGroupCount ?? 0}
            label="待审核分组"
            highlight
          />
          <SidebarStat value={snapshot?.indexedAssetCount ?? 0} label="已索引内容" />
          <SidebarStat value={snapshot?.activeFileCount ?? 0} label="活跃文件" />
          <SidebarStat value={snapshot?.appliedActionCount ?? 0} label="已执行操作" />
        </div>
      </aside>

      <main className="main-area">
        {activeTab === "scan" && renderScanTab()}
        {activeTab === "review" && renderReviewTab()}
        {activeTab === "history" && renderHistoryTab()}
      </main>

      {busyLabel ? (
        <div className="toast toast--busy">
          <span className="toast-spinner" />
          {busyLabel}
        </div>
      ) : null}
      {error ? (
        <div className="toast toast--error" onClick={() => setError(null)}>
          <span>{error}</span>
          <span className="toast-close">×</span>
        </div>
      ) : null}
    </div>
  );

  // ─── Scan Tab ────────────────────────────────────────────────────────────────

  function renderScanTab() {
    return (
      <div className="page scan-page">
        <div className="page-header">
          <div>
            <h1 className="page-title">扫描目录</h1>
            <p className="page-subtitle">
              选择本次要处理的照片目录。扫描时会自动对比历史全库，路径变更不影响内容判断。
            </p>
          </div>
          <div className="page-actions">
            <div className="thread-picker">
              <span className="thread-picker-label">并发</span>
              {([1, 2, 4, 8] as const).map((n) => (
                <button
                  key={n}
                  className={`chip ${scanThreads === n ? "chip--active" : ""}`}
                  onClick={() => setScanThreads(n)}
                >
                  {n}
                </button>
              ))}
            </div>
            <button className="btn btn--ghost" onClick={handlePickFolders}>
              添加目录
            </button>
            {scanPaths.length > 0 && (
              <button
                className="btn btn--ghost btn--subtle"
                onClick={() => setScanPaths([])}
              >
                清空列表
              </button>
            )}
            <button
              className="btn btn--primary"
              onClick={handleScan}
              disabled={
                scanPaths.length === 0 ||
                (scanProgress != null &&
                  (scanProgress.status === "counting" ||
                    scanProgress.status === "running" ||
                    scanProgress.status === "finalizing"))
              }
            >
              开始扫描
            </button>
            {scanProgress != null &&
              (scanProgress.status === "counting" ||
                scanProgress.status === "running" ||
                scanProgress.status === "finalizing") && (
              <button
                className="btn btn--ghost"
                onClick={() => void cancelScan()}
              >
                取消
              </button>
            )}
          </div>
        </div>

        <div className="scan-content">
          {scanProgress &&
            (scanProgress.status === "counting" ||
              scanProgress.status === "running" ||
              scanProgress.status === "finalizing") && (
            <ScanProgressPanel progress={scanProgress} />
          )}
          <div className="scan-paths-panel">
            {scanPaths.length === 0 ? (
              <div className="empty-state">
                <div className="empty-state-icon">
                  <FolderIcon size={28} />
                </div>
                <p className="empty-state-title">尚未添加目录</p>
                <p className="empty-state-body">
                  点击右上角「添加目录」选择要扫描的照片文件夹
                </p>
                <button className="btn btn--ghost" onClick={handlePickFolders}>
                  选择目录
                </button>
              </div>
            ) : (
              <>
                <div className="paths-list-header">
                  <span>{scanPaths.length} 个目录待扫描</span>
                </div>
                <div className="paths-list">
                  {scanPaths.map((path) => (
                    <div className="path-row" key={path}>
                      <span className="path-row-icon">
                        <FolderIcon size={16} />
                      </span>
                      <span className="path-row-text">{path}</span>
                      <button
                        className="path-row-remove"
                        onClick={() =>
                          setScanPaths((c) => c.filter((p) => p !== path))
                        }
                      >
                        ×
                      </button>
                    </div>
                  ))}
                </div>
              </>
            )}
          </div>

          {scanResult ? (
            <div className="scan-result">
              <div className="scan-result-header">
                <span className="scan-result-title">上次扫描结果</span>
                <span className="scan-result-time">
                  {formatDate(scanResult.completedAt)}
                </span>
              </div>
              <div className="scan-metrics">
                <ScanMetric value={scanResult.newFiles} label="新文件" accent />
                <ScanMetric value={scanResult.updatedLocations} label="更新位置" />
                <ScanMetric value={scanResult.unchangedFiles} label="跳过重算" />
                <ScanMetric value={unknownFormats.length} label="未知格式" />
              </div>
              {unknownFormats.length > 0 && (
                <div className="unknown-formats">
                  <div className="unknown-formats-title">未处理格式</div>
                  {unknownFormats.map((item) => (
                    <div className="unknown-format-row" key={item.extension}>
                      <span className="ext-badge">
                        .{item.extension || "(无扩展名)"}
                      </span>
                      <span className="unknown-format-path">{item.examplePath}</span>
                      <span className="unknown-format-count">{item.count}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          ) : null}
        </div>
      </div>
    );
  }

  // ─── Review Tab ───────────────────────────────────────────────────────────────

  function renderReviewTab() {
    return (
      <div className="page review-page">
        <div className="review-list-panel">
          <div className="review-list-header">
            <input
              className="search-input"
              value={searchText}
              onChange={(e) => setSearchText(e.currentTarget.value)}
              placeholder="搜索路径、说明…"
            />
            <div className="filter-section">
              <div className="filter-label">类型</div>
              <div className="chip-row">
                {kindTabs.map((tab) => (
                  <button
                    key={tab.label}
                    className={`chip ${filterKind === tab.value ? "chip--active" : ""}`}
                    onClick={() => setFilterKind(tab.value)}
                  >
                    {tab.label}
                  </button>
                ))}
              </div>
            </div>
            <div className="filter-section">
              <div className="filter-label">状态</div>
              <div className="chip-row">
                {statusTabs.map((tab) => (
                  <button
                    key={tab.label}
                    className={`chip ${filterStatus === tab.value ? "chip--active" : ""}`}
                    onClick={() => setFilterStatus(tab.value)}
                  >
                    {tab.label}
                  </button>
                ))}
              </div>
            </div>
          </div>

          <div className="group-list">
            {visibleGroups.length === 0 ? (
              <div className="empty-state empty-state--compact">
                <p className="empty-state-title">暂无分组</p>
                <p className="empty-state-body">
                  {groups.length === 0
                    ? "先扫描目录，系统会自动检测重复和相似图片"
                    : "当前筛选条件下没有匹配的分组"}
                </p>
                {groups.length === 0 && (
                  <button
                    className="btn btn--ghost btn--sm"
                    onClick={() => setActiveTab("scan")}
                  >
                    前往扫描
                  </button>
                )}
              </div>
            ) : (
              visibleGroups.map((group) => (
                <button
                  key={group.id}
                  className={`group-item ${group.id === selectedGroupId ? "group-item--active" : ""}`}
                  onClick={() => setSelectedGroupId(group.id)}
                >
                  <div className="group-item-top">
                    <span className={`kind-badge kind-badge--${group.kind}`}>
                      {kindLabel(group.kind)}
                    </span>
                    <div className="group-item-meta">
                      <span className={`status-dot status-dot--${group.status}`} />
                      <span className="group-item-count">{group.memberCount} 项</span>
                    </div>
                  </div>
                  <p className="group-item-reason">{group.recommendationReason}</p>
                  <p className="group-item-path">
                    {group.recommendedKeepPath ?? group.anchor}
                  </p>
                  <div className="group-item-footer">
                    <span className={`status-pill status-pill--${group.status}`}>
                      {statusLabel(group.status)}
                    </span>
                    <span className="group-item-date">{formatDate(group.updatedAt)}</span>
                  </div>
                </button>
              ))
            )}
          </div>
        </div>

        <div className="review-detail-panel">
          {selectedGroup ? (
            <>
              <div className="detail-header">
                <div className="detail-header-left">
                  <span className={`kind-badge kind-badge--${selectedGroup.kind}`}>
                    {kindLabel(selectedGroup.kind)}
                  </span>
                  <h2 className="detail-title">
                    {selectedGroup.recommendationReason}
                  </h2>
                </div>
                <div className="detail-header-right">
                  <div className="decision-summary">
                    <span className="decision-keep">保留 {pendingKeepCount}</span>
                    <span className="decision-sep">/</span>
                    <span className="decision-recycle">回收 {pendingRecycleCount}</span>
                  </div>
                  <button
                    className="btn btn--primary"
                    onClick={handleApplyDecision}
                    disabled={pendingKeepCount === 0 || pendingRecycleCount === 0}
                  >
                    应用决策
                  </button>
                </div>
              </div>

              <div className="member-grid">
                {selectedGroup.members.map((member) => {
                  const isRecycle = recycleIds.has(member.fileInstanceId);
                  const isRecommended =
                    member.fileInstanceId === selectedGroup.recommendedKeepInstanceId;
                  const filename =
                    member.path.split(/[\\/]/).pop() ?? member.path;

                  return (
                    <article
                      key={member.fileInstanceId}
                      className={`member-card ${isRecycle ? "member-card--recycle" : "member-card--keep"}`}
                    >
                      <div className="member-preview">
                        {member.previewSupported && member.thumbnailPath ? (
                          <img
                            alt={member.path}
                            src={convertFileSrc(member.thumbnailPath)}
                          />
                        ) : (
                          <div className="preview-fallback">
                            <span className="preview-ext">
                              {member.extension.toUpperCase()}
                            </span>
                            <span className="preview-note">无缩略图预览</span>
                          </div>
                        )}
                        <div
                          className={`member-overlay ${isRecycle ? "member-overlay--recycle" : "member-overlay--keep"}`}
                        >
                          {isRecycle ? "回收" : "保留"}
                        </div>
                        {isRecommended && (
                          <span className="recommended-badge">推荐</span>
                        )}
                      </div>

                      <div className="member-body">
                        <div className="member-filename" title={member.path}>
                          {filename}
                        </div>
                        <div className="member-path">{member.path}</div>
                        <div className="member-specs">
                          <span>{member.formatName ?? member.extension.toUpperCase()}</span>
                          <span>{formatResolution(member)}</span>
                          <span>{formatScore(member.qualityScore)}</span>
                          {member.similarity != null && (
                            <span>SSIM {member.similarity.toFixed(3)}</span>
                          )}
                        </div>
                      </div>

                      <div className="member-footer">
                        <button
                          className={`toggle-btn ${isRecycle ? "toggle-btn--recycle" : "toggle-btn--keep"}`}
                          onClick={() => toggleRecycle(member)}
                        >
                          {isRecycle ? "移入回收站" : "保留此文件"}
                        </button>
                        <button
                          className="history-btn"
                          onClick={() => {
                            setHistoryAssetId(member.contentAssetId);
                            setHistoryExpanded(true);
                          }}
                        >
                          路径历史
                        </button>
                      </div>
                    </article>
                  );
                })}
              </div>

              <div className="detail-footer">
                <textarea
                  className="note-input"
                  rows={2}
                  placeholder="可选备注，例如：这一组只删副本。"
                  value={note}
                  onChange={(e) => setNote(e.currentTarget.value)}
                />
              </div>

              {historyExpanded && (
                <div className="path-history-section">
                  <div className="section-header">
                    <span>路径历史</span>
                    <button
                      className="btn btn--ghost btn--sm"
                      onClick={() => setHistoryExpanded(false)}
                    >
                      收起
                    </button>
                  </div>
                  {pathHistory.length === 0 ? (
                    <div className="empty-state empty-state--compact">
                      <p className="empty-state-body">未检测到移动记录</p>
                    </div>
                  ) : (
                    <div className="timeline">
                      {pathHistory.map((item) => (
                        <div
                          className="timeline-item"
                          key={`${item.fileInstanceId}-${item.detectedAt}`}
                        >
                          <div className="timeline-dot" />
                          <div className="timeline-content">
                            <div className="timeline-event">
                              {translateChange(item.changeType)}
                            </div>
                            {item.oldPath && (
                              <div className="timeline-path timeline-path--old">
                                {item.oldPath}
                              </div>
                            )}
                            <div className="timeline-path">{item.newPath}</div>
                            <div className="timeline-date">
                              {formatDate(item.detectedAt)}
                            </div>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </>
          ) : (
            <div className="empty-state">
              <div className="empty-state-icon">
                <PhotoIcon />
              </div>
              <p className="empty-state-title">选择一个分组</p>
              <p className="empty-state-body">
                左侧选中一个分组后，这里会展示缩略图、评分和默认建议
              </p>
            </div>
          )}
        </div>
      </div>
    );
  }

  // ─── History Tab ─────────────────────────────────────────────────────────────

  function renderHistoryTab() {
    return (
      <div className="page history-page">
        <div className="page-header">
          <div>
            <h1 className="page-title">处理历史</h1>
            <p className="page-subtitle">已执行的审核动作完整记录</p>
          </div>
        </div>

        <div className="history-content">
          {actions.length === 0 ? (
            <div className="empty-state">
              <div className="empty-state-icon">
                <ClockIcon />
              </div>
              <p className="empty-state-title">暂无历史记录</p>
              <p className="empty-state-body">
                完成一次回收操作后，这里会保留详细记录
              </p>
            </div>
          ) : (
            <div className="history-list">
              {actions.map((action) => (
                <article className="history-card" key={action.id}>
                  <div className="history-card-header">
                    <span className={`kind-badge kind-badge--${action.groupKind}`}>
                      {kindLabel(action.groupKind)}
                    </span>
                    <span className="history-card-date">
                      {formatDate(action.createdAt)}
                    </span>
                  </div>
                  <div className="history-card-body">
                    <span className="history-keep">
                      保留 {action.keepInstanceIds.length} 项
                    </span>
                    <span className="history-sep">·</span>
                    <span className="history-recycle">
                      回收 {action.recycleInstanceIds.length} 项
                    </span>
                  </div>
                  {action.note && (
                    <div className="history-card-note">{action.note}</div>
                  )}
                </article>
              ))}
            </div>
          )}
        </div>
      </div>
    );
  }
}

// ─── Sub-components ───────────────────────────────────────────────────────────

function SidebarStat({
  value,
  label,
  highlight = false,
}: {
  value: number;
  label: string;
  highlight?: boolean;
}) {
  return (
    <div className={`stat-item ${highlight ? "stat-item--highlight" : ""}`}>
      <span className="stat-value">{value.toLocaleString()}</span>
      <span className="stat-label">{label}</span>
    </div>
  );
}

function ScanMetric({
  value,
  label,
  accent = false,
}: {
  value: number;
  label: string;
  accent?: boolean;
}) {
  return (
    <div className={`scan-metric ${accent ? "scan-metric--accent" : ""}`}>
      <span className="scan-metric-value">{value.toLocaleString()}</span>
      <span className="scan-metric-label">{label}</span>
    </div>
  );
}

// ─── Scan Progress Panel ──────────────────────────────────────────────────────

function ScanProgressPanel({ progress }: { progress: ScanProgress }) {
  const { totalFiles, done, queued, analyzing, activeItems, recentItems, grouping, status } = progress;
  const isGrouping = status === "finalizing";
  const pct = totalFiles > 0 ? Math.min(100, (done / totalFiles) * 100) : 0;

  return (
    <div className="scan-progress-panel">
      {!isGrouping && (
        <>
          <div className="spp-header">
            <span className="spp-phase">{progress.message}</span>
            <span className="spp-counts">
              {totalFiles > 0
                ? `${done.toLocaleString()} / ${totalFiles.toLocaleString()}  ${pct.toFixed(1)}%`
                : `待索引 ${queued.toLocaleString()} 文件`}
            </span>
          </div>
          {totalFiles > 0 && (
            <div className="progress-bar-track">
              <div className="progress-bar-fill" style={{ width: `${pct}%` }} />
            </div>
          )}
          {analyzing > 0 && activeItems.length > 0 && (
            <div className="spp-section">
              <div className="spp-section-title">正在处理 ({analyzing} 文件)</div>
              {activeItems.slice(0, 4).map((item, i) => (
                <ActiveItemRow key={i} item={item} />
              ))}
              {activeItems.length > 4 && (
                <div className="spp-more">+ {activeItems.length - 4} 更多…</div>
              )}
            </div>
          )}
          {recentItems.length > 0 && (
            <div className="spp-section">
              <div className="spp-section-title">最近完成</div>
              {recentItems.slice(-4).map((item, i) => (
                <RecentItemRow key={i} item={item} />
              ))}
            </div>
          )}
          <div className="spp-stats">
            {progress.newFiles > 0 && <span className="spp-stat spp-stat--new">新增 {progress.newFiles}</span>}
            {progress.updatedFiles > 0 && <span className="spp-stat spp-stat--updated">更新 {progress.updatedFiles}</span>}
            {progress.unchangedFiles > 0 && <span className="spp-stat spp-stat--unchanged">跳过 {progress.unchangedFiles}</span>}
            {progress.failedFiles > 0 && <span className="spp-stat spp-stat--failed">失败 {progress.failedFiles}</span>}
          </div>
        </>
      )}
      {isGrouping && grouping && <GroupingPanel grouping={grouping} />}
      {isGrouping && !grouping && (
        <div className="spp-header">
          <span className="spp-phase">正在分组照片…</span>
        </div>
      )}
    </div>
  );
}

function ActiveItemRow({ item }: { item: ScanActiveItem }) {
  return (
    <div className="active-item-row">
      <span className="spp-spinner">⟳</span>
      <span className="active-item-name">{item.fileName}</span>
      <span className="active-item-dir">{item.dirHint}</span>
    </div>
  );
}

function RecentItemRow({ item }: { item: ScanRecentItem }) {
  const icons: Record<string, string> = {
    new: "✓",
    updated: "✓",
    unchanged: "↺",
    failed: "✗",
  };
  return (
    <div className={`recent-item-row recent-item-row--${item.status}`}>
      <span className="recent-item-icon">{icons[item.status] ?? "·"}</span>
      <span className="recent-item-name">{item.fileName}</span>
    </div>
  );
}

function GroupingPanel({ grouping }: { grouping: GroupingProgress }) {
  const similarPct = grouping.similarPairsTotal > 0
    ? Math.min(100, (grouping.similarPairsDone / grouping.similarPairsTotal) * 100)
    : 0;
  return (
    <div className="grouping-panel">
      <div className="spp-header">
        <span className="spp-phase">正在分组照片…</span>
      </div>
      <div className="grouping-step">
        <span className={`grouping-step-icon ${grouping.exactDone ? "step-done" : "step-pending"}`}>
          {grouping.exactDone ? "✓" : "○"}
        </span>
        <span className="grouping-step-label">完全重复</span>
        {grouping.exactDone && (
          <span className="grouping-step-result">{grouping.exactGroups} 组</span>
        )}
      </div>
      <div className="grouping-step">
        <span className={`grouping-step-icon ${grouping.similarDone ? "step-done" : grouping.similarStarted ? "step-running" : "step-pending"}`}>
          {grouping.similarDone ? "✓" : grouping.similarStarted ? "⟳" : "○"}
        </span>
        <span className="grouping-step-label">视觉相似</span>
        {grouping.similarStarted && grouping.similarPairsTotal > 0 && !grouping.similarDone && (
          <span className="grouping-step-progress">
            {grouping.similarPairsDone.toLocaleString()} / {grouping.similarPairsTotal.toLocaleString()}
            <span className="grouping-mini-bar-track">
              <span className="grouping-mini-bar-fill" style={{ width: `${similarPct}%` }} />
            </span>
            {similarPct.toFixed(0)}%
          </span>
        )}
        {grouping.similarDone && (
          <span className="grouping-step-result">{grouping.similarGroups} 组</span>
        )}
        {!grouping.similarStarted && (
          <span className="grouping-step-waiting">等待中…</span>
        )}
      </div>
      <div className="grouping-step">
        <span className={`grouping-step-icon ${grouping.rawJpegDone ? "step-done" : "step-pending"}`}>
          {grouping.rawJpegDone ? "✓" : "○"}
        </span>
        <span className="grouping-step-label">RAW+JPEG 配对</span>
        {grouping.rawJpegDone && (
          <span className="grouping-step-result">{grouping.rawJpegGroups} 组</span>
        )}
        {!grouping.rawJpegDone && (
          <span className="grouping-step-waiting">等待中…</span>
        )}
      </div>
    </div>
  );
}

// ─── Icons ────────────────────────────────────────────────────────────────────

function FolderIcon({ size = 20 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <path d="M3 7a2 2 0 012-2h3.17a2 2 0 011.42.59l1.41 1.41H19a2 2 0 012 2v9a2 2 0 01-2 2H5a2 2 0 01-2-2V7z" />
    </svg>
  );
}

function PhotoIcon() {
  return (
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <rect x="3" y="3" width="18" height="18" rx="2" />
      <circle cx="8.5" cy="8.5" r="1.5" />
      <path d="M21 15l-5-5L5 21" />
    </svg>
  );
}

function ClockIcon() {
  return (
    <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
      <circle cx="12" cy="12" r="9" />
      <path d="M12 7v5l3 3" />
    </svg>
  );
}

// ─── Utilities ────────────────────────────────────────────────────────────────

function tabLabel(tab: AppTab): string {
  switch (tab) {
    case "scan": return "扫描";
    case "review": return "审核";
    case "history": return "历史";
  }
}

function tabIcon(tab: AppTab): string {
  switch (tab) {
    case "scan": return "⊕";
    case "review": return "◈";
    case "history": return "◷";
  }
}

function kindLabel(kind: MatchKind) {
  switch (kind) {
    case "exact": return "完全重复";
    case "similar": return "视觉相似";
    case "raw_jpeg_set": return "RAW + 导出图";
  }
}

function statusLabel(status: ReviewStatus) {
  switch (status) {
    case "pending": return "待处理";
    case "approved": return "已确认";
    case "skipped": return "已跳过";
    case "applied": return "已应用";
  }
}

function formatDate(value: string) {
  return new Intl.DateTimeFormat("zh-CN", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(new Date(value));
}

function formatScore(score: number | null) {
  if (score == null) return "质量分未知";
  return `质量 ${score.toFixed(1)}`;
}

function formatResolution(member: GroupMember) {
  if (!member.width || !member.height) return "尺寸未知";
  return `${member.width} × ${member.height}`;
}

function translateChange(changeType: string) {
  switch (changeType) {
    case "same_volume_move": return "同盘移动 / 重命名";
    case "cross_volume_move": return "跨盘迁移";
    default: return changeType;
  }
}

function delay(ms: number) {
  return new Promise<void>((resolve) => window.setTimeout(resolve, ms));
}

export default App;
