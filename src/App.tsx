import {
  startTransition,
  useDeferredValue,
  useCallback,
  useEffect,
  useRef,
  useState,
} from "react";
import type { CSSProperties, ImgHTMLAttributes } from "react";
import { convertFileSrc } from "@tauri-apps/api/core";
import { open } from "@tauri-apps/plugin-dialog";
import "./App.css";

import {
  applyDecision,
  cancelScan,
  listRatedPhotos,
  loadActions,
  loadGroup,
  loadGroups,
  loadHistory,
  loadScanStatus,
  loadSnapshot,
  loadUnknownFormats,
  recycleRatedPhoto,
  setRating,
  startScan,
  undoRating,
} from "./api";
import type {
  GroupDetail,
  GroupMember,
  GroupingProgress,
  GroupSummary,
  MatchKind,
  PathHistoryItem,
  RatedPhoto,
  RatingPhotoFilter,
  ReviewActionSummary,
  ReviewStatus,
  ScanActiveItem,
  ScanProgress,
  ScanRecentItem,
  ScanResult,
  UnknownFormatSummary,
} from "./types";

type AppTab = "scan" | "review" | "history" | "rating";

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

const loadedPreviewImageSrcs = new Set<string>();
const pendingPreviewImageLoads = new Map<string, Promise<void>>();

function preloadPreviewImage(src: string): Promise<void> {
  if (loadedPreviewImageSrcs.has(src)) {
    return Promise.resolve();
  }

  const pending = pendingPreviewImageLoads.get(src);
  if (pending) {
    return pending;
  }

  const promise = new Promise<void>((resolve, reject) => {
    const img = new Image();
    let settled = false;

    function finishLoaded() {
      if (settled) return;
      settled = true;
      loadedPreviewImageSrcs.add(src);
      pendingPreviewImageLoads.delete(src);
      resolve();
    }

    function finishFailed() {
      if (settled) return;
      settled = true;
      pendingPreviewImageLoads.delete(src);
      reject(new Error(`Failed to preload image: ${src}`));
    }

    img.decoding = "async";
    img.onload = () => {
      if (typeof img.decode === "function") {
        void img.decode().catch(() => undefined).finally(finishLoaded);
        return;
      }
      finishLoaded();
    };
    img.onerror = finishFailed;
    img.src = src;

    if (img.complete) {
      if (typeof img.decode === "function") {
        void img.decode().catch(() => undefined).finally(finishLoaded);
      } else {
        finishLoaded();
      }
    }
  });

  pendingPreviewImageLoads.set(src, promise);
  return promise;
}

function BufferedPreviewImage(
  props: Omit<ImgHTMLAttributes<HTMLImageElement>, "src"> & { src: string },
) {
  const { src, ...imgProps } = props;
  const [displaySrc, setDisplaySrc] = useState(src);
  const resolvedSrc = loadedPreviewImageSrcs.has(src) ? src : displaySrc;

  useEffect(() => {
    let cancelled = false;
    if (resolvedSrc === src) return;

    void preloadPreviewImage(src)
      .catch(() => undefined)
      .finally(() => {
        if (!cancelled) {
          setDisplaySrc(src);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [resolvedSrc, src]);

  return <img {...imgProps} src={resolvedSrc} draggable={false} />;
}

function sameRatingFilter(a: RatingPhotoFilter, b: RatingPhotoFilter) {
  return a.unratedOnly === b.unratedOnly && a.minRating === b.minRating;
}

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

  const [ratingPhotos, setRatingPhotos] = useState<RatedPhoto[]>([]);
  const [ratingTotal, setRatingTotal] = useState(0);
  const [ratingPhotoIdx, setRatingPhotoIdx] = useState(0);
  const [ratingFilter, setRatingFilter] = useState<RatingPhotoFilter>({
    unratedOnly: false,
    minRating: null,
  });
  const [ratingLoading, setRatingLoading] = useState(false);
  const [ratingLoadingMore, setRatingLoadingMore] = useState(false);
  const [ratingHasMore, setRatingHasMore] = useState(false);
  const [ratings, setRatings] = useState<Map<number, number | null>>(new Map());
  const [previewMode, setPreviewMode] = useState<"fit" | "actual">("fit");
  const [previewScale, setPreviewScale] = useState(1);
  const [isPreviewDragging, setIsPreviewDragging] = useState(false);
  const [ratingImmersive, setRatingImmersive] = useState(false);
  const [ratingListVisible, setRatingListVisible] = useState(true);
  const previewModeRef = useRef(previewMode);
  previewModeRef.current = previewMode;
  const previewScaleRef = useRef(previewScale);
  previewScaleRef.current = previewScale;
  const ratingPhotoIdxRef = useRef(ratingPhotoIdx);
  ratingPhotoIdxRef.current = ratingPhotoIdx;
  const ratingPhotosRef = useRef(ratingPhotos);
  ratingPhotosRef.current = ratingPhotos;
  const ratingTotalRef = useRef(ratingTotal);
  ratingTotalRef.current = ratingTotal;
  const ratingFilterRef = useRef(ratingFilter);
  ratingFilterRef.current = ratingFilter;
  const ratingListScrollRef = useRef<HTMLDivElement | null>(null);
  const ratingItemRefs = useRef(new Map<number, HTMLButtonElement>());
  const previewScrollRef = useRef<HTMLDivElement | null>(null);
  const compareCurrentPaneRef = useRef<HTMLDivElement | null>(null);
  const comparePeerPaneRef = useRef<HTMLDivElement | null>(null);
  const previewDragRef = useRef<{
    active: boolean;
    startX: number;
    startY: number;
    targets: Array<{ element: HTMLDivElement; left: number; top: number }>;
  }>({
    active: false,
    startX: 0,
    startY: 0,
    targets: [],
  });
  const PAGE_SIZE = 500;
  const ratingCurrentPhoto = ratingPhotos[ratingPhotoIdx] ?? null;
  const ratingPrevPhoto = ratingPhotoIdx > 0 ? ratingPhotos[ratingPhotoIdx - 1] : null;
  const ratingNextPhoto = ratingPhotoIdx < ratingPhotos.length - 1
    ? ratingPhotos[ratingPhotoIdx + 1]
    : null;
  const ratingComparePhoto = ratingNextPhoto ?? ratingPrevPhoto ?? null;
  const showDualPortraitCompare = Boolean(
    previewMode === "fit"
    && ratingCurrentPhoto
    && ratingCurrentPhoto.width
    && ratingCurrentPhoto.height
    && ratingCurrentPhoto.height > ratingCurrentPhoto.width
    && ratingComparePhoto,
  );
  const isPreviewPannable = previewMode === "actual" || previewScale > 1;

  useEffect(() => {
    if (activeTab !== "rating") return;
    void loadRatingPhotoPage(ratingFilter, 0, { append: false });
  }, [activeTab, ratingFilter]);

  useEffect(() => {
    if (activeTab === "rating") return;
    setRatingImmersive(false);
    setRatingListVisible(true);
    setPreviewScale(1);
  }, [activeTab]);

  useEffect(() => {
    if (activeTab !== "rating" || !ratingListVisible) return;
    const currentPhoto = ratingPhotos[ratingPhotoIdx] ?? null;
    if (!currentPhoto) return;
    const item = ratingItemRefs.current.get(currentPhoto.fileInstanceId);
    item?.scrollIntoView({
      block: "center",
      inline: "nearest",
      behavior: "smooth",
    });
  }, [activeTab, ratingListVisible, ratingPhotoIdx, ratingPhotos]);

  useEffect(() => {
    if (activeTab !== "rating" || ratingPhotos.length === 0) return;

    const nearbyPhotos = ratingPhotos
      .slice(Math.max(0, ratingPhotoIdx - 1), Math.min(ratingPhotos.length, ratingPhotoIdx + 5))
      .filter((photo) => photo.previewSupported);

    nearbyPhotos.forEach((photo) => {
      const src = convertFileSrc(photo.path);
      void preloadPreviewImage(src).catch(() => undefined);
    });
  }, [activeTab, ratingPhotoIdx, ratingPhotos]);

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

  const refreshDashboard = useCallback(async () => {
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
  }, [filterKind, filterStatus]);

  useEffect(() => {
    void refreshDashboard();
  }, [refreshDashboard]);

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

  async function loadRatingPhotoPage(
    filter: RatingPhotoFilter,
    offset: number,
    options?: { append?: boolean },
  ) {
    const append = options?.append ?? false;
    if (append) {
      setRatingLoadingMore(true);
    } else {
      setRatingLoading(true);
      setRatingHasMore(false);
    }
    try {
      const page = await listRatedPhotos(filter, offset, PAGE_SIZE);
      if (!sameRatingFilter(filter, ratingFilterRef.current)) {
        return;
      }

      const nextPhotos = append
        ? [...ratingPhotosRef.current, ...page.photos].filter((photo, idx, all) =>
          all.findIndex((candidate) => candidate.fileInstanceId === photo.fileInstanceId) === idx)
        : page.photos;

      setRatingPhotos(nextPhotos);
      setRatingTotal(page.total);
      setRatingHasMore(nextPhotos.length < page.total);
      if (!append) {
        setRatingPhotoIdx(0);
      }
      setRatings((prev) => {
        const next = new Map(prev);
        page.photos.forEach((photo) => {
          if (!next.has(photo.fileInstanceId)) {
            next.set(photo.fileInstanceId, photo.userRating ?? null);
          }
        });
        return next;
      });
    } catch (reason) {
      setError(String(reason));
    } finally {
      if (append) {
        setRatingLoadingMore(false);
      } else {
        setRatingLoading(false);
      }
    }
  }

  function clampPreviewScale(value: number) {
    return Math.min(3, Math.max(0.5, Number(value.toFixed(2))));
  }

  function adjustPreviewScale(delta: number) {
    setPreviewScale((current) => clampPreviewScale(current + delta));
  }

  const getPreviewScrollElements = useCallback(() => (
    showDualPortraitCompare
      ? [compareCurrentPaneRef.current, comparePeerPaneRef.current].filter(
        (pane): pane is HTMLDivElement => pane != null,
      )
      : previewScrollRef.current
        ? [previewScrollRef.current]
        : []
  ), [showDualPortraitCompare]);

  const stopPreviewDrag = useCallback(() => {
    if (!previewDragRef.current.active) return;
    previewDragRef.current.active = false;
    previewDragRef.current.targets = [];
    setIsPreviewDragging(false);
    document.body.style.userSelect = "";
    document.body.style.cursor = "";
  }, []);

  const handlePreviewMouseDown = useCallback((event: React.MouseEvent<HTMLDivElement>) => {
    if (event.button !== 0 || !isPreviewPannable) return;

    const targets = getPreviewScrollElements();

    if (targets.length === 0) return;

    const scrollTargets = targets.map((element) => ({
      element,
      left: element.scrollLeft,
      top: element.scrollTop,
    }));
    const hasOverflow = scrollTargets.some(({ element }) =>
      element.scrollWidth > element.clientWidth + 1
      || element.scrollHeight > element.clientHeight + 1);

    if (!hasOverflow) return;

    event.preventDefault();
    previewDragRef.current = {
      active: true,
      startX: event.clientX,
      startY: event.clientY,
      targets: scrollTargets,
    };
    setIsPreviewDragging(true);
    document.body.style.userSelect = "none";
    document.body.style.cursor = "grabbing";
  }, [getPreviewScrollElements, isPreviewPannable]);

  function removeRatingPhotoFromState(fileInstanceId: number) {
    const currentPhotos = ratingPhotosRef.current;
    const currentTotal = ratingTotalRef.current;
    const removedIdx = currentPhotos.findIndex((photo) => photo.fileInstanceId === fileInstanceId);
    if (removedIdx === -1) return;

    const nextPhotos = currentPhotos.filter((photo) => photo.fileInstanceId !== fileInstanceId);
    setRatingPhotos(nextPhotos);
    setRatingTotal((current) => Math.max(0, current - 1));
    setRatingHasMore(nextPhotos.length < Math.max(0, currentTotal - 1));
    setSnapshot((current) => current
      ? { ...current, activeFileCount: Math.max(0, current.activeFileCount - 1) }
      : current);
    setRatingPhotoIdx((current) => {
      if (nextPhotos.length === 0) return 0;
      if (current > removedIdx) return current - 1;
      return Math.min(current, nextPhotos.length - 1);
    });
  }

  const handleRecycleCurrentPhoto = useCallback(async (
    photo = ratingPhotosRef.current[ratingPhotoIdxRef.current],
  ) => {
    if (!photo) return;

    try {
      setError(null);
      await recycleRatedPhoto({ fileInstanceId: photo.fileInstanceId });
      setRatings((prev) => new Map(prev).set(photo.fileInstanceId, 0));
      removeRatingPhotoFromState(photo.fileInstanceId);
      void refreshDashboard();
    } catch (reason) {
      setError(String(reason));
    }
  }, [refreshDashboard]);

  const handlePreviewWheel = useCallback((event: React.WheelEvent<HTMLDivElement>) => {
    event.preventDefault();
    const currentScale = previewScaleRef.current;
    const nextScale = clampPreviewScale(currentScale + (event.deltaY < 0 ? 0.1 : -0.1));
    if (nextScale === currentScale) return;

    const targets = getPreviewScrollElements();
    const hoveredElement = (event.target as HTMLElement | null)?.closest(
      ".rating-preview-compare-pane, .rating-preview-img-wrap",
    ) as HTMLDivElement | null;
    const anchorRect = hoveredElement?.getBoundingClientRect() ?? event.currentTarget.getBoundingClientRect();
    const anchorRatioX = anchorRect.width > 0
      ? Math.min(1, Math.max(0, (event.clientX - anchorRect.left) / anchorRect.width))
      : 0.5;
    const anchorRatioY = anchorRect.height > 0
      ? Math.min(1, Math.max(0, (event.clientY - anchorRect.top) / anchorRect.height))
      : 0.5;
    const scaleRatio = nextScale / currentScale;
    const scrollAnchors = targets.map((element) => {
      const viewportX = anchorRatioX * element.clientWidth;
      const viewportY = anchorRatioY * element.clientHeight;
      return {
        element,
        viewportX,
        viewportY,
        contentX: element.scrollLeft + viewportX,
        contentY: element.scrollTop + viewportY,
      };
    });

    setPreviewScale(nextScale);
    requestAnimationFrame(() => {
      scrollAnchors.forEach(({ element, viewportX, viewportY, contentX, contentY }) => {
        element.scrollLeft = contentX * scaleRatio - viewportX;
        element.scrollTop = contentY * scaleRatio - viewportY;
      });
    });
  }, [getPreviewScrollElements]);

  const handlePreviewDoubleClick = useCallback(() => {
    setPreviewScale(1);
  }, []);

  const handleRatingListScroll = useCallback((event: React.UIEvent<HTMLDivElement>) => {
    if (ratingLoading || ratingLoadingMore || !ratingHasMore) return;

    const element = event.currentTarget;
    const remaining = element.scrollHeight - element.scrollTop - element.clientHeight;
    if (remaining > 240) return;

    void loadRatingPhotoPage(ratingFilterRef.current, ratingPhotosRef.current.length, {
      append: true,
    });
  }, [ratingHasMore, ratingLoading, ratingLoadingMore]);

  useEffect(() => {
    function handleMouseMove(event: MouseEvent) {
      const state = previewDragRef.current;
      if (!state.active) return;

      const deltaX = event.clientX - state.startX;
      const deltaY = event.clientY - state.startY;

      state.targets.forEach(({ element, left, top }) => {
        element.scrollLeft = left - deltaX;
        element.scrollTop = top - deltaY;
      });
    }

    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseup", stopPreviewDrag);
    window.addEventListener("mouseleave", stopPreviewDrag);
    return () => {
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseup", stopPreviewDrag);
      window.removeEventListener("mouseleave", stopPreviewDrag);
      stopPreviewDrag();
    };
  }, [stopPreviewDrag]);

  useEffect(() => {
    if (!isPreviewPannable) {
      stopPreviewDrag();
    }
  }, [isPreviewPannable, stopPreviewDrag]);

  const handleRatingKey = useCallback((key: string) => {
    const photos = ratingPhotosRef.current;
    const idx = ratingPhotoIdxRef.current;
    const currentPhoto = photos[idx];

    if (key >= "0" && key <= "5") {
      const ratingVal = parseInt(key, 10);
      if (!currentPhoto) return;
      const fileInstanceId = currentPhoto.fileInstanceId;
      setRatings((prev) => new Map(prev).set(fileInstanceId, ratingVal));
      void setRating({ fileInstanceId, rating: ratingVal }).catch(() => {
        setRatings((prev) => new Map(prev).set(fileInstanceId, currentPhoto.userRating ?? null));
      });
      if (ratingVal > 0) {
        setRatingPhotoIdx((current) => Math.min(current + 1, photos.length - 1));
      }
      return;
    }
    if (key === "ArrowRight" || key === "ArrowDown") {
      setRatingPhotoIdx((current) => Math.min(current + 1, photos.length - 1));
      return;
    }
    if (key === "ArrowLeft" || key === "ArrowUp") {
      setRatingPhotoIdx((current) => Math.max(current - 1, 0));
      return;
    }
    if (key === "u" || key === "U") {
      void undoRating().then((restored) => {
        if (restored) {
          setRatings((prev) =>
            new Map(prev).set(restored.fileInstanceId, restored.restoredRating));
        }
      });
      return;
    }
    if (key === "Delete") {
      void handleRecycleCurrentPhoto(currentPhoto);
      return;
    }
    if (key === "z" || key === "Z") {
      setPreviewMode((mode) => (mode === "fit" ? "actual" : "fit"));
      return;
    }
    if (key === "+" || key === "=") {
      setPreviewScale((current) => clampPreviewScale(current + 0.1));
      return;
    }
    if (key === "-" || key === "_") {
      setPreviewScale((current) => clampPreviewScale(current - 0.1));
      return;
    }
    if (key === "Backspace") {
      setPreviewScale(1);
      return;
    }
    if (key === "f" || key === "F") {
      setRatingImmersive((current) => {
        const next = !current;
        setRatingListVisible(!next);
        return next;
      });
      return;
    }
    if (key === "l" || key === "L") {
      setRatingListVisible((visible) => !visible);
    }
  }, [handleRecycleCurrentPhoto]);

  useEffect(() => {
    if (activeTab !== "rating") return;
    function onKeyDown(event: KeyboardEvent) {
      const tag = (event.target as HTMLElement).tagName.toLowerCase();
      if (tag === "input" || tag === "textarea" || tag === "select") return;
      if (event.key === "Tab") {
        event.preventDefault();
        setRatingListVisible((visible) => !visible);
        return;
      }
      if (event.key === "Escape" && ratingImmersive) {
        event.preventDefault();
        setRatingImmersive(false);
        setRatingListVisible(true);
        return;
      }
      handleRatingKey(event.key);
    }
    window.addEventListener("keydown", onKeyDown);
    return () => window.removeEventListener("keydown", onKeyDown);
  }, [activeTab, handleRatingKey, ratingImmersive]);

  async function waitForScanCompletion(
    onProgress: (progress: ScanProgress) => void,
  ): Promise<ScanResult> {
    while (true) {
      const progress = await loadScanStatus();
      onProgress(progress);

      if (progress.status === "completed" && progress.result) {
        return progress.result;
      }
      if (progress.status === "cancelled") {
        throw new Error("__cancelled__");
      }
      if (progress.status === "failed") {
        throw new Error(progress.error ?? "扫描失败");
      }

      await delay(250);
    }
  }

  const isRatingImmersive = activeTab === "rating" && ratingImmersive;

  return (
    <div className={`app-shell ${isRatingImmersive ? "app-shell--immersive" : ""}`}>
      <aside className="sidebar">
        <div className="sidebar-brand">
          <div className="brand-mark">SI</div>
          <div className="brand-info">
            <span className="brand-name">Single Image</span>
            <span className="brand-sub">本地照片整理</span>
          </div>
        </div>

        <nav className="sidebar-nav">
          {(["scan", "review", "rating", "history"] as AppTab[]).map((tab) => (
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

      <main className={`main-area ${isRatingImmersive ? "main-area--immersive" : ""}`}>
        {activeTab === "scan" && renderScanTab()}
        {activeTab === "review" && renderReviewTab()}
        {activeTab === "rating" && renderRatingTab()}
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

  function renderRatingTab() {
    const currentPhoto = ratingCurrentPhoto;
    const filename = currentPhoto
      ? currentPhoto.path.split(/[\\/]/).pop() ?? currentPhoto.path
      : null;
    const prevPhoto = ratingPrevPhoto;
    const nextPhoto = ratingNextPhoto;

    function getPhotoRating(fid: number): number | null {
      if (ratings.has(fid)) return ratings.get(fid) ?? null;
      return ratingPhotos.find((photo) => photo.fileInstanceId === fid)?.userRating ?? null;
    }

    const currentRating = currentPhoto ? getPhotoRating(currentPhoto.fileInstanceId) : null;
    const comparePhoto = ratingComparePhoto;
    const comparePhotoId = showDualPortraitCompare ? comparePhoto?.fileInstanceId ?? null : null;

    const filterChips: Array<{ label: string; filter: RatingPhotoFilter }> = [
      { label: "全部", filter: { unratedOnly: false, minRating: null } },
      { label: "未评分", filter: { unratedOnly: true, minRating: null } },
      { label: "★3+", filter: { unratedOnly: false, minRating: 3 } },
      { label: "★5", filter: { unratedOnly: false, minRating: 5 } },
    ];

    function isFilterActive(filter: RatingPhotoFilter) {
      return filter.unratedOnly === ratingFilter.unratedOnly
        && filter.minRating === ratingFilter.minRating;
    }

    function toggleImmersiveMode() {
      setRatingImmersive((current) => {
        const next = !current;
        setRatingListVisible(!next);
        return next;
      });
    }

    return (
      <div className={`page rating-page ${ratingImmersive ? "rating-page--immersive" : ""}`}>
        <div className={`rating-hint-bar ${ratingImmersive ? "rating-hint-bar--immersive" : ""}`}>
          <span className="rating-hint"><kbd>0-5</kbd> 评分</span>
          <span className="rating-hint"><kbd>←</kbd><kbd>→</kbd> 切换</span>
          <span className="rating-hint"><kbd>Del</kbd> 回收</span>
          <span className="rating-hint"><kbd>U</kbd> 撤销</span>
          <span className="rating-hint"><kbd>Z</kbd> 缩放</span>
          <span className="rating-hint">滚轮缩放</span>
          <span className="rating-hint"><kbd>+</kbd><kbd>-</kbd> 缩放比</span>
          <span className="rating-hint"><kbd>F</kbd> Immersive</span>
          <span className="rating-hint"><kbd>Tab</kbd> List</span>
          <div className="rating-hint-spacer" />
          <div className="preview-mode-toggle">
            <button
              className={`preview-mode-btn ${previewMode === "fit" ? "preview-mode-btn--active" : ""}`}
              onClick={() => setPreviewMode("fit")}
              title="适合窗口 (Z)"
            >
              适合
            </button>
            <button
              className={`preview-mode-btn ${previewMode === "actual" ? "preview-mode-btn--active" : ""}`}
              onClick={() => setPreviewMode("actual")}
              title="按宽边铺满"
            >
              1:1
            </button>
          </div>
          <div className="preview-zoom-controls">
            <button
              className="rating-toolbar-btn"
              onClick={() => adjustPreviewScale(-0.1)}
              title="缩小 (-)"
            >
              -
            </button>
            <button
              className="rating-toolbar-btn rating-toolbar-btn--active"
              onClick={() => setPreviewScale(1)}
              title="重置缩放 (Backspace)"
            >
              {Math.round(previewScale * 100)}%
            </button>
            <button
              className="rating-toolbar-btn"
              onClick={() => adjustPreviewScale(0.1)}
              title="放大 (+)"
            >
              +
            </button>
          </div>
          <button
            className={`rating-toolbar-btn ${ratingListVisible ? "rating-toolbar-btn--active" : ""}`}
            onClick={() => setRatingListVisible((visible) => !visible)}
            title="Toggle photo list (Tab)"
          >
            {ratingListVisible ? "Hide List" : "Show List"}
          </button>
          <button
            className={`rating-toolbar-btn ${ratingImmersive ? "rating-toolbar-btn--active" : ""}`}
            onClick={toggleImmersiveMode}
            title="Toggle immersive mode (F / Esc)"
          >
            {ratingImmersive ? "Exit Immersive" : "Immersive Mode"}
          </button>
          {ratingLoading && <span className="rating-loading">加载中...</span>}
          {!ratingLoading && (
            <span className="rating-total">
              {ratingTotal.toLocaleString()} items
              {ratingPhotos.length < ratingTotal && ` (已加载 ${ratingPhotos.length})`}
            </span>
          )}
          <button
            className="btn btn--ghost btn--sm"
            onClick={() => void loadRatingPhotoPage(ratingFilter, 0, { append: false })}
          >
            刷新
          </button>
        </div>

        <div className={`rating-layout ${ratingImmersive ? "rating-layout--immersive" : ""}`}>
          <div className={`rating-photo-list ${!ratingListVisible ? "rating-photo-list--hidden" : ""}`}>
            <div className="rating-filter-row">
              {filterChips.map((chip) => (
                <button
                  key={chip.label}
                  className={`chip ${isFilterActive(chip.filter) ? "chip--active" : ""}`}
                  onClick={() => setRatingFilter(chip.filter)}
                >
                  {chip.label}
                </button>
              ))}
            </div>

            <div
              className="rating-photo-scroll"
              ref={ratingListScrollRef}
              onScroll={handleRatingListScroll}
            >
              {ratingPhotos.length === 0 && !ratingLoading ? (
                <div className="empty-state empty-state--compact">
                  <p className="empty-state-title">
                    {ratingTotal === 0 ? "暂无已索引照片" : "没有符合条件的照片"}
                  </p>
                  <p className="empty-state-body">
                    {ratingTotal === 0
                      ? "先扫描目录，照片会在这里显示"
                      : "尝试切换过滤条件"}
                  </p>
                </div>
              ) : (
                <>
                  {ratingPhotos.map((photo, idx) => {
                  const ratingValue = getPhotoRating(photo.fileInstanceId);
                  const photoName = photo.path.split(/[\\/]/).pop() ?? photo.path;
                  return (
                    <button
                      key={photo.fileInstanceId}
                      ref={(node) => {
                        if (node) {
                          ratingItemRefs.current.set(photo.fileInstanceId, node);
                        } else {
                          ratingItemRefs.current.delete(photo.fileInstanceId);
                        }
                      }}
                      className={`rating-photo-item ${
                        idx === ratingPhotoIdx ? "rating-photo-item--active" : ""
                      } ${
                        comparePhotoId === photo.fileInstanceId ? "rating-photo-item--compare" : ""
                      }`}
                      onClick={() => setRatingPhotoIdx(idx)}
                    >
                      <div className="rating-photo-thumb">
                        {photo.previewSupported && photo.thumbnailPath ? (
                          <img src={convertFileSrc(photo.thumbnailPath)} alt={photoName} loading="lazy" />
                        ) : (
                          <div className="rating-photo-thumb-fallback">{photo.extension.toUpperCase()}</div>
                        )}
                      </div>
                      <div className="rating-photo-info">
                        <div className="rating-photo-name" title={photo.path}>{photoName}</div>
                        <div className="rating-photo-stars">
                          {ratingValue != null && ratingValue > 0
                            ? "★".repeat(ratingValue) + "☆".repeat(5 - ratingValue)
                            : "☆☆☆☆☆"}
                        </div>
                      </div>
                    </button>
                  );
                  })}
                  {ratingLoadingMore && (
                    <div className="rating-list-loading-more">正在加载更多照片...</div>
                  )}
                </>
              )}
            </div>
          </div>

          <div className={`rating-preview-area ${ratingImmersive ? "rating-preview-area--immersive" : ""}`}>
            {currentPhoto ? (
              <>
                <div
                  ref={previewScrollRef}
                  className={`rating-preview-img-wrap rating-preview-img-wrap--${previewMode} ${
                    isPreviewPannable ? "rating-preview-img-wrap--pannable" : ""
                  } ${isPreviewDragging ? "rating-preview-img-wrap--dragging" : ""}`}
                  onWheel={handlePreviewWheel}
                  onMouseDown={handlePreviewMouseDown}
                  onDoubleClick={handlePreviewDoubleClick}
                  style={{ ["--preview-scale" as string]: previewScale } as CSSProperties}
                >
                  {showDualPortraitCompare ? (
                    <div className="rating-preview-compare">
                      <div
                        ref={compareCurrentPaneRef}
                        className="rating-preview-compare-pane"
                      >
                        <div className="rating-preview-compare-label">当前</div>
                        {currentPhoto.previewSupported ? (
                          <BufferedPreviewImage
                            className={`rating-preview-img rating-preview-img--${previewMode}`}
                            src={convertFileSrc(currentPhoto.path)}
                            alt={filename ?? ""}
                          />
                        ) : (
                          <div className="rating-preview-fallback">
                            <span className="preview-ext">{currentPhoto.extension.toUpperCase()}</span>
                            <span className="preview-note">无预览</span>
                          </div>
                        )}
                      </div>
                      <div
                        ref={comparePeerPaneRef}
                        className="rating-preview-compare-pane"
                      >
                        <div className="rating-preview-compare-label">对比</div>
                        {comparePhoto?.previewSupported ? (
                          <BufferedPreviewImage
                            className={`rating-preview-img rating-preview-img--${previewMode}`}
                            src={convertFileSrc(comparePhoto.path)}
                            alt={comparePhoto.path.split(/[\\/]/).pop() ?? comparePhoto.path}
                          />
                        ) : (
                          <div className="rating-preview-fallback">
                            <span className="preview-ext">
                              {comparePhoto?.extension.toUpperCase() ?? "--"}
                            </span>
                            <span className="preview-note">无预览</span>
                          </div>
                        )}
                      </div>
                    </div>
                  ) : currentPhoto.previewSupported ? (
                    <BufferedPreviewImage
                      className={`rating-preview-img rating-preview-img--${previewMode}`}
                      src={convertFileSrc(currentPhoto.path)}
                      alt={filename ?? ""}
                    />
                  ) : (
                    <div className="rating-preview-fallback">
                      <span className="preview-ext">{currentPhoto.extension.toUpperCase()}</span>
                      <span className="preview-note">无预览</span>
                    </div>
                  )}
                  {previewMode === "fit" && prevPhoto?.previewSupported && prevPhoto.thumbnailPath && (
                    <img className="rating-preload" src={convertFileSrc(prevPhoto.thumbnailPath)} alt="" aria-hidden="true" />
                  )}
                  {previewMode === "fit" && nextPhoto?.previewSupported && nextPhoto.thumbnailPath && (
                    <img className="rating-preload" src={convertFileSrc(nextPhoto.thumbnailPath)} alt="" aria-hidden="true" />
                  )}
                </div>

                <div className={`rating-preview-meta ${ratingImmersive ? "rating-preview-meta--immersive" : ""}`}>
                  <div className="rating-preview-filename">{filename}</div>
                  <div className="rating-preview-path">{currentPhoto.path}</div>
                  <div className="rating-preview-specs">
                    {currentPhoto.formatName ?? currentPhoto.extension.toUpperCase()}
                    {currentPhoto.width && currentPhoto.height ? ` · ${currentPhoto.width}×${currentPhoto.height}` : ""}
                    {currentPhoto.qualityScore != null ? ` · 质量 ${currentPhoto.qualityScore.toFixed(1)}` : ""}
                  </div>

                  <div className="rating-stars-row">
                    {[1, 2, 3, 4, 5].map((star) => (
                      <button
                        key={star}
                        className={`rating-star ${(currentRating ?? 0) >= star ? "rating-star--filled" : ""}`}
                        onClick={() => {
                          const nextRating = currentRating === star ? 0 : star;
                          const fileInstanceId = currentPhoto.fileInstanceId;
                          setRatings((prev) => new Map(prev).set(fileInstanceId, nextRating));
                          void setRating({ fileInstanceId, rating: nextRating }).catch(() => {
                            setRatings((prev) => new Map(prev).set(fileInstanceId, currentPhoto.userRating ?? null));
                          });
                        }}
                        title={`${star} star`}
                      >
                        ★
                      </button>
                    ))}
                    {currentRating != null && currentRating > 0 && (
                      <button
                        className="rating-clear-btn"
                        onClick={() => {
                          const fileInstanceId = currentPhoto.fileInstanceId;
                          setRatings((prev) => new Map(prev).set(fileInstanceId, 0));
                          void setRating({ fileInstanceId, rating: 0 });
                        }}
                      >
                        清除
                      </button>
                    )}
                    <span className="rating-score-label">
                      {currentRating != null && currentRating > 0 ? `${currentRating} 星` : "未评分"}
                    </span>
                    <button
                      className="rating-clear-btn rating-delete-btn"
                      onClick={() => void handleRecycleCurrentPhoto(currentPhoto)}
                      title="移动到回收站并记为 0 分 (Delete)"
                    >
                      删除到回收站
                    </button>
                  </div>

                  <div className="rating-nav-row">
                    <button className="btn btn--ghost btn--sm" onClick={() => setRatingPhotoIdx((idx) => Math.max(idx - 1, 0))} disabled={ratingPhotoIdx === 0}>
                      ← 上一张
                    </button>
                    <span className="rating-nav-pos">{ratingPhotoIdx + 1} / {ratingPhotos.length}</span>
                    <button
                      className="btn btn--ghost btn--sm"
                      onClick={() => setRatingPhotoIdx((idx) => Math.min(idx + 1, ratingPhotos.length - 1))}
                      disabled={ratingPhotoIdx >= ratingPhotos.length - 1}
                    >
                      下一张 →
                    </button>
                  </div>
                </div>
              </>
            ) : (
              <div className="empty-state">
                <div className="empty-state-icon"><PhotoIcon /></div>
                <p className="empty-state-title">{ratingLoading ? "正在加载..." : "暂无照片"}</p>
                <p className="empty-state-body">
                  {!ratingLoading && "先扫描目录，照片会在这里显示"}
                </p>
              </div>
            )}
          </div>
        </div>
      </div>
    );
  }

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
    case "rating": return "评分";
    case "history": return "历史";
  }
}

function tabIcon(tab: AppTab): string {
  switch (tab) {
    case "scan": return "⊕";
    case "rating": return "★";
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










