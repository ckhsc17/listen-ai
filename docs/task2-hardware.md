# ListenAI 執行環境硬體規格（任務 2）

本文件記錄撰寫與量測時所用環境，實際數值以執行當下機器為準。

## 主機（macOS）

| 項目 | 值 |
|------|-----|
| 機型 / CPU | Apple M1 Pro |
| 實體核心 | 10 |
| 邏輯核心 | 10 |
| 記憶體 | 16 GiB（`hw.memsize` = 17179869184 bytes） |
| 架構 | arm64 |
| 作業系統 | Darwin 24.6.0 |

取得方式範例：`sysctl -n hw.memsize machdep.cpu.brand_string hw.physicalcpu hw.logicalcpu`、`uname -a`。

## 容器執行環境（Docker / Colima）

量測時若使用本專案 `docker-compose` 服務，容器所見資源可能受 VM 限制，與主機規格不同。

| 項目 | 值（範例） |
|------|------------|
| Docker Server | Colima（範例） |
| CPUs（容器環境回報） | 2 |
| Total Memory | 約 3.8 GiB |

取得方式範例：`docker info`（Server 區段之 CPUs / Total Memory）。

## 建議

- 效能數字應註明「主機規格」與「容器是否受限」。
- 比較 5k vs 100 萬筆時，固定同一執行環境（同一台機器、同樣的 Docker 資源設定），結果才有可比性。
