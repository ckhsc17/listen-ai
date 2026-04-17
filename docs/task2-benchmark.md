# 任務 2：效能比較方法（儀表板）

## 目的

比較**不同資料規模**下，`POST /api/dashboard`（經 gateway 轉呼叫 stat `/stats`）的延遲。優化後路徑**不再於每次請求對全部貼文呼叫 NLP**，而是以資料庫內已儲存之情感與（可選）`post_tokens` 聚合。

## 環境

- 硬體與 Docker 資源請見 [task2-hardware.md](./task2-hardware.md)。
- 測試前請固定：`STAT_URL`、`NLP_URL`（僅匯入／backfill 需要）、同一組關鍵字與日期範圍。

## 準備資料集

1. **約 5,000 筆規模**：使用既有 [`data/posts.csv`](../data/posts.csv) 匯入（或當前 `listenai.db`）。
2. **至少 1,000,000 筆**：使用產生器（**合成情感欄位**，避免對 100 萬筆即時跑 NLP）：

```bash
python data/generate_scale_dataset.py --db ./data/listenai_1m.db --target 1000000
```

將 `docker-compose.yml` 中 stat 的 `SQLITE_PATH` 指到該檔案，或本機執行 stat 時設定 `SQLITE_PATH` 指向該檔案。

## 執行量測

在 **gateway + stat** 已啟動、且已登入可用的前提下：

```bash
python scripts/benchmark_dashboard.py --gateway http://localhost:8000 --runs 5
```

可調整 `--include-keywords` 與日期（腳本內預設與前端相近：含「機器人」、寬日期範圍）。

## 建議填寫結果表（作業用）

| 資料規模（約） | mean_ms | min_ms | max_ms | mentionCount（參考） |
|----------------|---------|--------|--------|------------------------|
| ~5,000 |  |  |  |  |
| ~1,000,000 |  |  |  |  |

**說明**：優化前若曾對每次儀表板呼叫 NLP，延遲會隨篩選後貼文數接近線性惡化；優化後主要成本為 **SQLite 篩選與聚合**，情感不再重複推論。

> 實際數字請在你自己的機器上跑完後填入；不同磁碟與 Docker CPU/RAM 上限會影響結果。
