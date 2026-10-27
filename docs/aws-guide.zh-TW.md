# AWS 學習筆記：用這個專案理解 SQS、Lambda、DynamoDB 與 IaC

這份筆記以本專案（USDC Deposit Ledger）為例，說明幾件事：

- 用到的每個 AWS 服務是什麼
- 為什麼要這樣設計
- 什麼情況下適合用
- 平常怎麼操作

目標是讀完之後，能把同樣的思路套到其他系統上。

> 本專案的實際資源都在**雪梨區（`ap-southeast-2`）**，名稱都以 `usdc-ledger` 開頭。

---

## 目錄

1. [全貌：一筆入金怎麼走完整條路](#1-全貌一筆入金怎麼走完整條路)
2. [AWS 基本觀念](#2-aws-基本觀念)
3. [各服務詳解](#3-各服務詳解)
4. [核心設計原理：分散式系統的「不重複、不遺漏」](#4-核心設計原理分散式系統的不重複不遺漏)
5. [Terraform 與基礎設施即程式碼（IaC）](#5-terraform-與基礎設施即程式碼iac)
6. [GitHub Actions + OIDC：不用金鑰的自動部署](#6-github-actions--oidc不用金鑰的自動部署)
7. [操作手冊（日常會用到的指令）](#7-操作手冊日常會用到的指令)
8. [今天實際遇到的問題與解法](#8-今天實際遇到的問題與解法)
9. [靈活運用：什麼時候用這套架構、什麼時候不要](#9-靈活運用什麼時候用這套架構什麼時候不要)
10. [費用觀念](#10-費用觀念)
11. [面試常見問題](#11-面試常見問題)
12. [名詞對照表](#12-名詞對照表)

---

## 1. 全貌：一筆入金怎麼走完整條路

```
 Base 區塊鏈
     │  ① 有人轉 USDC 給我們監聽的地址
     ▼
 base-usdc-indexer（Go，跑在任何地方）
     │  ② 等到區塊 finalized，把轉帳和「事件」寫進同一個 PostgreSQL transaction（outbox）
     │  ③ relay 把 outbox 事件送到 SQS
     ▼
 SQS FIFO 佇列  usdc-ledger-transfers.fifo
     │  ④ Lambda 自動來拉訊息，一次最多 10 則
     ▼
 Lambda 函式  usdc-ledger-consumer（Go）
     │  ⑤ 每個事件用「一個 DynamoDB transaction」入帳
     ▼
 DynamoDB 資料表  usdc-ledger
        帳戶餘額 + 帳本明細 + 「已處理」標記

 失敗 5 次的訊息 ──▶ DLQ（死信佇列）──▶ CloudWatch 告警 ──▶ SNS ──▶ 你的 Email
```

**一句話說明每個元件：**

| 元件 | 一句話 | 在這裡的角色 |
|---|---|---|
| **SQS** | 存放待辦訊息的佇列 | 把「偵測到入金」和「入帳」兩件事拆開，中間做緩衝 |
| **Lambda** | 有事情才執行的函式，不用管伺服器 | 讀佇列、入帳 |
| **DynamoDB** | 全代管的 key-value 資料庫 | 存帳戶餘額和帳本 |
| **CloudWatch** | 日誌、指標、告警 | 看 Lambda 的 log，監控佇列有沒有塞住 |
| **SNS** | 推播通知 | 告警寄 Email |
| **IAM** | 權限系統 | 決定「誰」可以對「什麼」做「什麼事」 |
| **S3** | 檔案儲存 | 存放 Terraform 的狀態檔 |
| **Budgets** | 預算警報 | 超過 US$5 就寄信 |

---

## 2. AWS 基本觀念

### 2.1 Region（區域）與 Availability Zone（可用區）
- **Region** 是地理位置，例如 `ap-southeast-2` 是雪梨、`us-east-1` 是維吉尼亞。資料預設不會離開你選的區域。
- **選區域的原則**：
  - 離使用者近
  - 符合法規，例如澳洲的資料放在澳洲
  - 服務在這一區有提供
  - 價格（各區略有差異）
- 每個 Region 有多個 **AZ**（獨立機房）。SQS、Lambda、DynamoDB 都會**自動**跨 AZ 備援，不用自己處理。

### 2.2 帳號、root、IAM 使用者
| 身分 | 是什麼 | 用法 |
|---|---|---|
| **root** | 註冊時的 Email 帳號，權限無限 | 只拿來做帳單、關閉帳號這類事。**一定要開 MFA，平常不用** |
| **IAM user**（你的 `admin`） | 帳號裡建立的使用者 | 平常登入 Console 和 CLI 用 |
| **IAM role** | 一組權限，給「暫時扮演它的人或服務」使用 | Lambda、GitHub Actions 都是透過 role 取得權限 |

**為什麼 role 比 user 好？** role 給的是**臨時憑證**（幾分鐘到幾小時就過期），不需要長期保存金鑰，外洩的傷害也小很多。

### 2.3 IAM Policy（權限政策）
Policy 是一份 JSON，說明「允許或拒絕」「哪些動作（Action）」「作用在哪些資源（Resource）」。例如本專案 Lambda 的權限：

```json
{
  "Effect": "Allow",
  "Action": ["sqs:ReceiveMessage", "sqs:DeleteMessage", "sqs:GetQueueAttributes", "sqs:ChangeMessageVisibility"],
  "Resource": "arn:aws:sqs:ap-southeast-2:<帳號>:usdc-ledger-transfers.fifo"
}
```

- **最小權限原則（Least privilege）**：只給剛好夠用的權限。Lambda 只能讀這一個佇列、寫這一張表。程式就算有漏洞被利用，也碰不到別的資源。
- 一個 role 有兩種 policy：
  - **Trust policy（信任政策）**：**誰可以扮演**這個 role。例如「只有 Lambda 服務」、「只有某個 GitHub repo 的 main 分支」。
  - **Permission policy（權限政策）**：扮演之後**能做什麼**。

### 2.4 ARN（Amazon Resource Name）
每個 AWS 資源的唯一 ID，格式是：
```
arn:aws:<服務>:<區域>:<帳號ID>:<資源>
arn:aws:dynamodb:ap-southeast-2:123456789012:table/usdc-ledger
```
寫權限、互相串接資源時都會用到。

### 2.5 憑證怎麼來（重要）
| 方式 | 適用 | 本專案 |
|---|---|---|
| `aws login`（瀏覽器登入，臨時憑證） | 你自己在電腦上操作 | ✅ 你用的方式 |
| IAM role 自動提供 | 跑在 AWS 裡的服務（Lambda、ECS、EC2） | ✅ Lambda |
| OIDC 換臨時憑證 | GitHub Actions 這類 CI/CD | ✅ 部署流程 |
| Access Key（長期金鑰） | 盡量不要用 | ❌ 沒用 |

---

## 3. 各服務詳解

### 3.1 SQS（Simple Queue Service）— 訊息佇列

**是什麼**：一個可靠的「待辦箱」。生產者（indexer）把訊息放進去，消費者（Lambda）取出來處理，處理完再刪掉。

**為什麼要佇列，不直接呼叫？**
- **解耦**：indexer 不需要知道誰在處理、處理多久。Lambda 掛掉時，訊息會留在佇列裡等它恢復。
- **削峰**：一次湧入 1000 筆入金，佇列先接住，Lambda 照自己的速度慢慢消化。
- **重試**：處理失敗不刪除訊息，之後會自動再試。

**Standard 與 FIFO 的差別**

| | Standard | FIFO（本專案） |
|---|---|---|
| 順序 | 不保證 | 同一個 **message group** 內保證先進先出 |
| 重複 | 偶爾會重複投遞 | 5 分鐘內同一個 **deduplication ID** 只收一次 |
| 吞吐量 | 幾乎無限 | 較低（開了高吞吐模式後已經非常夠用） |
| 名稱 | 任意 | 必須以 `.fifo` 結尾 |

**本專案的關鍵參數（`terraform/app/main.tf`）**

| 參數 | 值 | 意義 |
|---|---|---|
| `MessageGroupId` | `chainId:txHash:logIndex` | 同一筆轉帳的「入帳」和「撤銷」一定按順序處理，不同轉帳可以平行處理 |
| `MessageDeduplicationId` | 事件 ID 的 SHA-256 | 5 分鐘內重送的同一個事件會被 SQS 丟掉 |
| `visibility_timeout_seconds` | 180（Lambda 逾時 30 秒 × 6） | 訊息被取走後隱藏 180 秒。沒在這段時間內刪除，就會重新出現讓別人重試 |
| `maxReceiveCount` | 5 | 同一則訊息失敗 5 次就移到 DLQ |
| `message_retention_seconds` | 4 天（DLQ 14 天） | 訊息最多保留多久 |
| `deduplication_scope` / `fifo_throughput_limit` | `messageGroup` / `perMessageGroupId` | 高吞吐 FIFO 模式 |

**Visibility timeout（可見性逾時）的原理**：SQS 不會在訊息「被拿走」時就刪除，而是先把它藏起來。消費者處理成功後要**主動刪除**；如果消費者在處理途中當機，訊息超過時限就會重新出現。所以 SQS 保證的是「**至少送達一次（at-least-once）**」，不是「剛好一次」，也因此消費者一定要做到冪等（見第 4 章）。

**DLQ（Dead-Letter Queue，死信佇列）**：一直處理失敗的訊息（例如格式錯誤的「毒訊息」）不能無限重試，否則會一直卡住、一直花錢。超過 `maxReceiveCount` 就移到 DLQ，等人來看。問題修好之後，可以 **redrive** 把訊息送回原佇列重新處理。

### 3.2 Lambda — 無伺服器函式

**是什麼**：你只上傳程式碼，AWS 在「有事件時」幫你執行，依**執行次數和時間**計費。沒事件就不花錢，也不用管作業系統、擴展和修補。

**本專案的設定**

| 設定 | 值 | 說明 |
|---|---|---|
| Runtime | `provided.al2023` | 自帶執行檔，Go 編譯成名叫 `bootstrap` 的 Linux 執行檔 |
| 架構 | `arm64`（Graviton） | 比 x86 便宜約 20%，效能也好 |
| 記憶體 | 128 MB | CPU 會跟著記憶體比例配給。Go 很省資源，128 MB 就夠 |
| 逾時 | 30 秒 | 一批訊息必須在 30 秒內處理完 |
| 最大並行 | 5 | 同時最多 5 個 Lambda 處理這個佇列 |

**Event Source Mapping（事件來源對應）**：這是 Lambda 服務內建的「拉取器」。它會一直 long-poll SQS，拿到訊息後**打包成一批（最多 10 則）**呼叫你的函式，再依照你回傳的結果刪除成功的訊息。你的程式完全不用寫 SQS 的拉取邏輯。`cmd/local` 的 poller 就是在本機模仿這個行為。

**Partial Batch Response（部分批次失敗）**：設定 `ReportBatchItemFailures` 之後，函式可以回傳「這一批裡只有第 3、7 則失敗」，SQS 只重試這兩則。不設定的話，只要一則失敗，整批 10 則都要重來，成功的那 9 則也會被重送。

**FIFO 的額外規則**：同一個 group 裡某一則失敗了，**後面同 group 的訊息也要一起回報失敗**，否則順序會亂掉（`internal/handler` 有實作）。

**Cold start（冷啟動）**：太久沒被呼叫時，下一次要先建立執行環境，會多花幾十到幾百毫秒。Go 的冷啟動很快，入金這種非同步場景也不在意這點延遲。

**什麼時候不適合 Lambda**：
- 單次執行超過 15 分鐘（上限）
- 需要長時間維持連線（WebSocket 伺服器）
- 流量持續很高、很穩定（這時 ECS、EC2 可能更便宜）

### 3.3 DynamoDB — NoSQL 資料庫

**是什麼**：全代管的 key-value / 文件型資料庫，不論資料多大都是毫秒級回應。依讀寫次數計費（On-demand），不用管伺服器。

**和 PostgreSQL 的思維差異**

| | PostgreSQL（關聯式） | DynamoDB |
|---|---|---|
| 設計方式 | 先設計正規化的表，查詢隨便寫 | **先想清楚要怎麼查**，再設計 key |
| 查詢 | JOIN、任意 WHERE | 主要靠 **PK（分割鍵）+ SK（排序鍵）** |
| 擴展 | 垂直擴展為主 | 自動水平擴展，幾乎沒有上限 |
| 交易 | 完整 ACID | 有 `TransactWriteItems`（最多 100 個項目） |

**Single-table design（單表設計）**：把不同類型的資料放在同一張表，用 key 的前綴區分：

| PK | SK | 存什麼 | 對應的查詢 |
|---|---|---|---|
| `EVENT#<eventId>` | `EVENT` | 已處理標記 | 「這個事件處理過了嗎？」 |
| `ACCT#<地址>` | `BALANCE` | 餘額 | 「這個帳戶現在多少錢？」（GetItem） |
| `ACCT#<地址>` | `ENTRY#<區塊>#<log>#...` | 帳本明細 | 「這個帳戶的最近 20 筆」（Query，依 SK 排序） |

同一個帳戶的餘額和明細共用同一個 PK，一次 Query 就能一起取回。SK 以區塊號碼開頭，所以明細天然按時間排序。

**本專案用到的關鍵功能**

1. **Conditional Write（條件寫入）**：`attribute_not_exists(PK)` 的意思是「只有在這筆資料不存在時才寫入」。這是防止重複入帳的核心。
2. **TransactWriteItems（交易寫入）**：「標記、明細、餘額」三件事要嘛全部成功，要嘛全部不做。其中任何一個條件失敗，整筆交易都會取消。
3. **`ADD` 更新**：`ADD netCredited :d` 是原子性的加減，不需要先讀出來再寫回去，也就沒有競態問題。
4. **TTL**：已處理標記的 `expiresAt` 到期後，DynamoDB 會自動刪除（免費）。設 30 天，比 SQS 最長的保留期 14 天還長。
5. **Point-in-time recovery**：可以把資料表還原到過去 35 天內的任一秒。
6. **強一致讀取**（`ConsistentRead`）：讀到的一定是最新寫入的值。預設是最終一致，會便宜一點但可能讀到舊值。

**限制要知道**：
- 單一項目最大 400 KB。
- Number 型別最多 38 位有效數字（USDC 夠用，所以程式裡有檢查）。
- 同一個項目每秒的寫入量有上限，叫做 **hot key（熱點）**。大量交易同時更新同一個帳戶的餘額時，就會互相衝突（見第 8 章）。

### 3.4 CloudWatch 與 SNS — 觀測與告警

- **CloudWatch Logs**：Lambda 的 `log/slog` 輸出會自動送到 `/aws/lambda/usdc-ledger-consumer`，保留 14 天。
- **CloudWatch Metrics**：各服務都會自動產生指標，例如佇列裡有幾則訊息、最舊的訊息等了多久、Lambda 錯誤次數。
- **CloudWatch Alarms**：指標超過門檻時就觸發。本專案有 3 個：

| 告警 | 條件 | 代表什麼 |
|---|---|---|
| `usdc-ledger-dlq-not-empty` | DLQ 有訊息 | 有入金處理失敗 5 次，需要人工介入 |
| `usdc-ledger-backlog-age` | 最舊的訊息等超過 5 分鐘（持續 5 分鐘） | 處理速度跟不上，或 Lambda 停了 |
| `usdc-ledger-consumer-errors` | Lambda 出錯 | 程式有 bug 或權限問題 |

- **SNS**：發布 / 訂閱的通知服務。告警送到 SNS topic，再由 topic 轉寄到 Email、簡訊、Slack（透過 Lambda）等。**Email 訂閱要先點確認信才會生效。**

### 3.5 S3 — 物件儲存（這裡用來放 Terraform state）
- 本專案只用 S3 存 Terraform 的狀態檔，開了**版本控制**（可以回復到舊版）、**加密**、**封鎖公開存取**。
- S3 的一般用途：靜態網站、使用者上傳的檔案、備份、資料湖。

### 3.6 Budgets — 預算警報
- 設每月 US$5 上限：預測會超過 80% 時寄一次信，實際超過 100% 時再寄一次。
- **它只會通知，不會自動停止服務**。真的要強制停掉，要另外設定 Budget Actions。

---

## 4. 核心設計原理：分散式系統的「不重複、不遺漏」

這一章是整個專案最值得帶走的觀念，跟你在永宙遇到的重複扣款問題是同一類。

### 4.1 為什麼一定會收到重複訊息？
分散式系統裡，「訊息剛好送到一次」幾乎做不到。只要網路、程式、機器有任何一環失敗，系統就只能二選一：

- **寧可重送**：at-least-once，可能重複。
- **寧可不送**：at-most-once，可能遺漏。

**錢的事情不能遺漏**，所以只能選「可能重複」，再讓**接收端自己去重**。這叫做**冪等（idempotency）**：同一件事做 1 次和做 10 次，結果一樣。

本專案裡，每一個可能造成重送的地方：

| 環節 | 為什麼會重送 |
|---|---|
| indexer relay | 送到 SQS 之後、標記「已送」之前當機，重啟後會再送一次 |
| SQS | at-least-once 本來就允許重複 |
| Lambda | 處理成功但刪除訊息前逾時，訊息會再出現 |
| SDK 重試 | 網路逾時之後自動重送請求 |

### 4.2 錯誤做法：先查再寫（Check-then-act）
```
if 這個事件沒處理過:      ← 兩個 Lambda 同時查，都得到「沒處理過」
    餘額 += 金額          ← 兩個都加了一次 → 重複入帳！
    記錄「已處理」
```
兩個執行緒**同時**通過檢查，這就是**競態條件（race condition）**。單機測試幾乎測不出來，上線後碰到高併發才會爆發。

### 4.3 正確做法：讓資料庫原子性地保證
```
一個 transaction:
    寫入「EVENT#事件ID」，條件：不存在      ← 第二個人一定會在這一步失敗
    寫入帳本明細
    餘額 ADD 金額
```
「檢查」和「寫入」在資料庫內部是**同一個不可分割的動作**，兩個請求不可能同時成功。測試 `TestConcurrentDuplicatesApplyOnce` 讓 20 個 worker 同時處理同一個事件，結果只入帳 1 次。

同一套思路在不同資料庫的寫法：

| 資料庫 | 寫法 |
|---|---|
| PostgreSQL | `UNIQUE` 限制 + `INSERT ... ON CONFLICT DO NOTHING`（indexer 就是這樣做） |
| DynamoDB | Conditional write + `TransactWriteItems`（本專案） |
| Redis | `SET key value NX` |

### 4.4 Transactional Outbox 模式（indexer 端）
問題：怎麼確保「資料庫寫入」和「送出訊息」兩件事一起成功？
- 先寫 DB 再送訊息：送之前當機，訊息就遺漏了。
- 先送訊息再寫 DB：寫 DB 失敗，就出現了沒有對應資料的幽靈訊息。

解法：把訊息**和資料寫進同一個資料庫 transaction**（outbox 表），再由另一支程式（relay）慢慢送出去。DB 的 transaction 保證兩者一致，relay 保證最終一定送達（可能重複，所以接收端要冪等）。

### 4.5 順序與「可交換性」
reorg 時會先收到「入帳 +700」再收到「撤銷 −700」。萬一順序顛倒呢？
- **FIFO message group**：同一筆轉帳放在同一個 group，保證按順序處理。
- **更根本的保險**：餘額只用**加法**更新（+700 和 −700 不管先後，總和都是 0）。只要運算可交換，順序就不影響最終結果。測試 `TestRevertsInAnyOrder` 驗證了這一點。

> 設計心法：**能用「累加」就不要用「覆寫」**。覆寫會怕順序錯亂，累加不怕。

### 4.6 失敗分兩種，處理方式不同
| 類型 | 例子 | 處理 |
|---|---|---|
| **暫時性**（transient） | 網路逾時、限流、`TransactionConflict` | **重試**（立刻重試加指數退避，再不行就交給 SQS 重送） |
| **永久性**（permanent） | 訊息格式錯誤、金額超過精度 | 重試沒用，**送進 DLQ 讓人處理** |

重試的時候要加**指數退避加隨機抖動（exponential backoff with jitter）**：等 20ms、40ms、80ms…，每次再隨機縮短一些。這樣大家不會在同一個時間點一起重試，又撞在一起。

---

## 5. Terraform 與基礎設施即程式碼（IaC）

### 5.1 為什麼不在 Console 用滑鼠點？
- **可重現**：換一個帳號或區域，一行指令就能重建一模一樣的環境。
- **可審查**：所有變更都在 git 裡，可以 code review，也能回溯是誰在什麼時候改了什麼。
- **可預覽**：`terraform plan` 會先列出「要新增、修改、刪除什麼」，確認後才執行。
- **不會漏**：IAM、告警這種容易忘記的設定都寫在程式碼裡。

### 5.2 Terraform 的運作原理
```
 你寫的 .tf（期望的狀態）
        │
        ▼
 terraform plan  ── 比對 ──  state 檔（Terraform 記得自己建了什麼）
        │                          │
        │                    真實的 AWS（目前的狀態）
        ▼
 「要新增 3 個、修改 1 個、刪除 0 個」
        │  你確認
        ▼
 terraform apply → 呼叫 AWS API 執行 → 更新 state 檔
```

- **Provider**：Terraform 透過 provider 外掛呼叫各家雲的 API（這裡是 `hashicorp/aws`）。
- **State（狀態檔）**：Terraform 的「記憶」，記錄每個資源對應到 AWS 上的哪個實體。**弄丟了 Terraform 就不認得自己建過的東西。** 所以 app 的 state 放在 S3 並開了版本控制，同時用 lockfile 防止兩個人同時執行 apply。
- **宣告式**：你只描述「想要什麼」，不用描述「怎麼做」。改一個參數，Terraform 會自己算出要修改還是重建。

### 5.3 為什麼分成 bootstrap 和 app 兩層？
「雞生蛋」問題：app 的 state 要放在 S3，但 S3 bucket 本身也要用 Terraform 建立。

| Stack | 內容 | 誰執行 | state 放哪 | 多久改一次 |
|---|---|---|---|---|
| `terraform/bootstrap` | state bucket、OIDC、部署角色、預算 | **你**（admin 權限），只做一次 | 你電腦上的 `terraform.tfstate`（**不要刪**） | 幾乎不改 |
| `terraform/app` | SQS、Lambda、DynamoDB、告警 | **GitHub Actions** | S3 | 每次部署 |

這樣分還有一個安全上的好處：GitHub Actions 的部署角色**沒有權限修改自己的權限**。就算 CI 被入侵，攻擊者也沒辦法提升權限。

### 5.4 常用指令
```bash
terraform init      # 下載 provider、連接 state 後端
terraform fmt       # 統一格式
terraform validate  # 檢查語法
terraform plan      # 預覽變更（不會動到任何東西）
terraform apply     # 執行變更
terraform output    # 顯示輸出值（例如 queue_url）
terraform state list  # 列出 Terraform 管理的資源
terraform destroy   # 全部刪除（小心！）
```

---

## 6. GitHub Actions + OIDC：不用金鑰的自動部署

### 6.1 傳統做法的問題
把 AWS Access Key 存在 GitHub Secrets 裡，金鑰是長期有效的。一旦外洩，例如被惡意的第三方 Action 讀走，攻擊者可以一直用到你發現為止。

### 6.2 OIDC 的流程
```
 GitHub Actions 執行中
   │ ① 向 GitHub 要一張「身分證」（JWT token），上面寫著：
   │    我是 repo BrianLiu0531/usdc-deposit-ledger，分支 main
   ▼
 AWS STS（AssumeRoleWithWebIdentity）
   │ ② 驗證這張 token 確實由 GitHub 簽發（OIDC provider）
   │ ③ 檢查 role 的 trust policy：sub 是否等於允許的 repo 與分支
   ▼
 拿到 1 小時後過期的臨時憑證 → 執行 terraform apply
```

**Trust policy 的關鍵條件**：
```hcl
condition {
  test     = "StringEquals"
  variable = "token.actions.githubusercontent.com:sub"
  values   = ["repo:BrianLiu0531@49802872/usdc-deposit-ledger@1435454456:ref:refs/heads/main", ...]
}
```
只有**這個 repo 的 main 分支**能換到憑證。別人 fork 你的 repo、或從其他分支發 PR，都換不到。新格式帶有帳號 ID 和 repo ID（`@49802872`、`@1435454456`），就算 repo 刪除後被別人用同樣名稱重建，ID 也不一樣，一樣換不到。

### 6.3 Secrets 與 Variables
| 名稱 | 類型 | 為什麼 |
|---|---|---|
| `AWS_DEPLOY_ROLE_ARN`、`TF_STATE_BUCKET` | **Secret** | 含有帳號 ID，公開 repo 的 Actions 日誌任何人都能看，Secret 會被自動遮蔽成 `***` |
| `WATCH_ADDRESSES`、`ALARM_EMAIL` | Variable | 本來就可以公開 |

---

## 7. 操作手冊（日常會用到的指令）

> 以下指令在 Git Bash 或 `!` 模式執行，`aws` 的路徑是 `"/c/Program Files/Amazon/AWSCLIV2/aws.exe"`。為了簡潔，下面一律寫成 `aws`。
> 想直接打 `aws`，可以先執行：`aws() { MSYS_NO_PATHCONV=1 "/c/Program Files/Amazon/AWSCLIV2/aws.exe" "$@"; }`
>
> **Git Bash 的陷阱**：它會把開頭是 `/` 的參數（例如 log group 名稱 `/aws/lambda/...`）自動轉成 Windows 路徑，導致 `InvalidParameterException`。前面加上 `MSYS_NO_PATHCONV=1` 就能解決（上面那行函式已經包含）。在 PowerShell 裡執行則沒有這個問題。

### 7.1 登入
```bash
aws login --region ap-southeast-2          # 開瀏覽器，用 IAM user（admin）登入
aws sts get-caller-identity                # 確認現在是誰（應該是 user/admin，不是 root）
aws logout                                  # 登出
```
臨時憑證 15 分鐘會自動更新。如果出現過期之類的錯誤，重新執行 `aws login` 就好。

### 7.2 部署 / 更新
1. 修改程式或 `terraform/app/main.tf`，推上 GitHub（CI 會先跑測試和 `terraform validate`）。
2. GitHub → **Actions** → **Deploy** → **Run workflow**。
3. 看日誌裡的 `Apply complete!` 和 outputs。

**改監聽地址**：GitHub → Settings → Secrets and variables → Actions → Variables → 修改 `WATCH_ADDRESSES`（JSON 陣列），再跑一次 Deploy。

### 7.3 看 Lambda 日誌
```bash
aws logs tail /aws/lambda/usdc-ledger-consumer --since 30m            # 最近 30 分鐘
aws logs tail /aws/lambda/usdc-ledger-consumer --follow               # 持續監看
aws logs tail /aws/lambda/usdc-ledger-consumer --since 1h --filter-pattern '"applying event failed"'
```
也可以在 Console → CloudWatch → Log groups 查看。

### 7.4 看佇列狀態
```bash
Q=$(aws sqs get-queue-url --queue-name usdc-ledger-transfers.fifo --query QueueUrl --output text)
aws sqs get-queue-attributes --queue-url "$Q" \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible
```
- `ApproximateNumberOfMessages`：等著被處理的訊息數。
- `ApproximateNumberOfMessagesNotVisible`：正在處理中，或失敗後等待重試的訊息數。
- 兩個都是 0 就代表全部處理完了。

### 7.5 查帳
```bash
# 某個帳戶的餘額
aws dynamodb get-item --table-name usdc-ledger --consistent-read \
  --key '{"PK":{"S":"ACCT#0xa849bbad82f9edafdf9a5550cc874276138779d9"},"SK":{"S":"BALANCE"}}'

# 某個帳戶最近 5 筆明細（新到舊）
aws dynamodb query --table-name usdc-ledger --consistent-read --no-scan-index-forward --max-items 5 \
  --key-condition-expression "PK = :pk AND begins_with(SK, :e)" \
  --expression-attribute-values '{":pk":{"S":"ACCT#0xa849bbad82f9edafdf9a5550cc874276138779d9"},":e":{"S":"ENTRY#"}}'
```
也可以在 Console → DynamoDB → Tables → `usdc-ledger` → **Explore table items** 用介面查詢。

### 7.6 處理 DLQ（收到 `dlq-not-empty` 告警時）
1. **看訊息內容**，找出失敗原因：
   ```bash
   D=$(aws sqs get-queue-url --queue-name usdc-ledger-transfers-dlq.fifo --query QueueUrl --output text)
   aws sqs receive-message --queue-url "$D" --max-number-of-messages 10 --attribute-names All
   ```
   搭配 7.3 的 Lambda 日誌，找到 `applying event failed` 或 `invalid message`。
2. **修好問題**：如果是程式 bug，修正後重新部署。
3. **重新處理**：Console → SQS → `usdc-ledger-transfers-dlq.fifo` → **Start DLQ redrive**，把訊息送回原佇列。因為消費者是冪等的，就算部分訊息其實已經處理過，重送也不會重複入帳。
4. 如果是永遠無法處理的垃圾訊息，確認後可以直接在 DLQ 刪除。

### 7.7 在本機跑 Terraform（這台電腦有 Avast）
這台電腦的 Avast 會攔截 Terraform 和 provider 之間的連線，所以改在 Docker 裡執行：
```bash
cd usdc-deposit-ledger
export CA_BUNDLE=~/.aws/ca-bundle.pem      # 已包含 Avast 根憑證

# bootstrap（state 在本機）
bash scripts/terraform-in-docker.sh terraform/bootstrap plan -var budget_email=brianliu2924@gmail.com

# app（state 在 S3，要先 init）
B=$(bash scripts/terraform-in-docker.sh terraform/bootstrap output -raw state_bucket)
bash scripts/terraform-in-docker.sh terraform/app init \
  -backend-config="bucket=$B" -backend-config="key=app/terraform.tfstate" -backend-config="region=ap-southeast-2"
bash scripts/terraform-in-docker.sh terraform/app state list
```

### 7.8 全部拆除（不想再付任何費用時）
順序很重要：**先拆 app，再拆 bootstrap**。bootstrap 裡有 app 的 state，順序反過來就沒辦法正確拆除 app。
1. **拆 app**（先完成 7.7 的 `terraform/app init`）：
   ```bash
   mkdir -p dist && touch dist/consumer.zip         # destroy 也需要這個檔案存在
   bash scripts/terraform-in-docker.sh terraform/app destroy -var 'watch_addresses=["0x0000000000000000000000000000000000000000"]'
   ```
2. **清空 state bucket**：bucket 開了版本控制，要先刪除所有版本。Console → S3 → bucket → **Empty**。
3. **拆 bootstrap**：
   ```bash
   bash scripts/terraform-in-docker.sh terraform/bootstrap destroy -var budget_email=brianliu2924@gmail.com
   ```
4. 最後可以在 Console → Billing 確認沒有任何費用。

---

## 8. 今天實際遇到的問題與解法

| 問題 | 現象 | 原因 | 解法 | 學到什麼 |
|---|---|---|---|---|
| **用 root 登入** | `get-caller-identity` 顯示 `:root` | 用註冊的 Email 登入 | 建立 IAM user `admin` 並重新登入 | root 只用來管帳單 |
| **憑證驗證失敗** | `CERTIFICATE_VERIFY_FAILED` | Avast 的 Web Shield 用自己的憑證攔截 HTTPS | 做一個包含 Avast 根憑證的 CA bundle，再設定 `ca_bundle` | 企業網路、防毒軟體的 TLS 攔截很常見，**不要直接關閉驗證** |
| **Terraform 外掛當掉** | `Failed to load plugin schemas` | Avast 連 Terraform 和 provider 在本機的 gRPC 連線也攔截 | 在 Docker 容器裡執行 Terraform | 先開 `TF_LOG=debug` 看真正的錯誤，不要用猜的（我一開始誤判成記憶體不足） |
| **OIDC 被拒** | `Not authorized to perform sts:AssumeRoleWithWebIdentity` | GitHub 對新 repo 改用帶 ID 的 `sub` 格式 | trust policy 加入新格式 | 用 `gh api repos/<owner>/<repo>/actions/oidc/customization/sub` 查實際格式 |
| **權限不足** | `ListTags ... 403` | 部署角色缺少讀取 event source mapping 標籤的權限 | 補上 `lambda:ListTags` | 最小權限要靠實際部署才能補齊，看錯誤訊息就知道缺什麼 |
| **交易衝突** | 32 筆裡有 2 筆延遲 3 分鐘才入帳 | 多個 Lambda 同時更新同一個帳戶的餘額項目（熱點） | 在程式裡立刻重試衝突，搭配退避和抖動 | **本機模擬器測不出這種併發問題，只有上了真實雲端才看得到** |

**TransactionConflict 的其他解法**（面試時可以拿來比較取捨）：
- **立刻重試**（本專案採用）：最簡單，衝突機率不高時就很夠用。
- **以帳戶為 message group**：同一個帳戶的事件依序處理，根本不會衝突，但同一個帳戶就失去平行處理的能力。
- **分片計數器**：把餘額拆成 N 個子項目，寫入時隨機選一個，讀取時再加總。適合超高頻寫入的熱門帳戶。
- **不在寫入時更新餘額**：只寫明細，再由 DynamoDB Streams 非同步彙總。寫入最快，但餘額會有短暫延遲。

---

## 9. 靈活運用：什麼時候用這套架構、什麼時候不要

### 9.1 這個模式能套用到哪些情境
「**事件 → 佇列 → 冪等消費者 → 資料庫**」是後端最常見的模式之一：

| 情境 | 事件來源 | 消費者做的事 | 冪等鍵 |
|---|---|---|---|
| 金流 webhook（Stripe、綠界） | 付款成功通知 | 訂單標記已付款、發貨 | 金流的交易 ID |
| 電商下單 | 訂單建立 | 扣庫存、寄確認信 | 訂單 ID |
| 寄 Email / 推播 | 註冊完成 | 寄歡迎信 | 使用者 ID + 信件類型 |
| 圖片處理 | S3 上傳事件 | 產生縮圖 | 檔案路徑 + 版本 |
| 遊戲派彩（永宙的情境） | 遊戲商結算通知 | 錢包加減款 | 注單號（TicketNum） |
| 報表 / 資料同步 | 資料庫變更（DynamoDB Streams、CDC） | 同步到搜尋引擎或資料倉儲 | 變更序號 |

**判斷原則**：只要「呼叫端不需要立刻知道結果」，而且「處理可能失敗、需要重試」，就適合放進佇列做非同步處理。

### 9.2 相近服務怎麼選
| 需求 | 選擇 |
|---|---|
| 一個任務只讓一個消費者處理，要能重試 | **SQS** |
| 一個事件要通知很多個訂閱者（扇出） | **SNS**（常見組合是 SNS → 多個 SQS） |
| 依事件內容路由到不同目標、排程觸發、串接 SaaS | **EventBridge** |
| 大量串流資料、要能重播、多個消費者各自讀取 | **Kinesis** 或 **MSK（Kafka）** |
| 多步驟流程、要等待、要人工審核、要補償 | **Step Functions** |

| 運算需求 | 選擇 |
|---|---|
| 事件驅動、短時間（< 15 分鐘）、流量不固定 | **Lambda** |
| 長時間執行的服務、穩定流量、需要容器 | **ECS Fargate**（例如把 indexer 放上 AWS 就適合這個） |
| 需要完全控制機器 | **EC2** |

| 資料需求 | 選擇 |
|---|---|
| 查詢模式固定、超大規模、key-value | **DynamoDB** |
| 複雜查詢、JOIN、報表、交易邏輯多 | **RDS / Aurora（PostgreSQL）** |
| 快取、排行榜、計數器 | **ElastiCache（Redis / Valkey）** |
| 檔案、大型物件 | **S3** |

### 9.3 什麼時候**不要**用這套
- 使用者在等結果（例如登入、查餘額）→ 直接同步呼叫 API。
- 流量很小、團隊不熟 AWS → 一台伺服器加 PostgreSQL，再用 `ON CONFLICT` 做冪等就夠了，不要過度設計。
- 需要跨很多表的複雜交易 → 關聯式資料庫比較適合。

### 9.4 延伸練習建議
1. **把 indexer 部署到 ECS Fargate**：學 ECR、ECS、VPC、Secrets Manager。
2. **加一個查詢 API**：API Gateway + Lambda 讀 DynamoDB，提供 `GET /accounts/{address}`。
3. **DynamoDB Streams**：入帳時觸發另一個 Lambda，寄 Email 或推播「入金成功」。
4. **X-Ray / OpenTelemetry**：追蹤一筆事件從 SQS 到 DynamoDB 的完整路徑和耗時。
5. **加一個 `destroy` workflow**：練習 GitHub Actions 的 environment 與人工核准（approval）。

---

## 10. 費用觀念

| 服務 | 計費方式 | 免費額度（長期） | 本專案預估 |
|---|---|---|---|
| Lambda | 請求數 + 執行時間 × 記憶體 | 每月 100 萬次請求、40 萬 GB-秒 | ≈ US$0 |
| SQS | 每 100 萬次請求 | 每月 100 萬次 | ≈ US$0 |
| DynamoDB（On-demand） | 讀寫次數 + 儲存量 | 25 GB 儲存 | ≈ US$0 |
| CloudWatch | 日誌量、告警數 | 5 GB 日誌、10 個告警 | ≈ US$0 |
| S3 | 儲存量 + 請求 | — | 幾美分 |

**原則**：
- Serverless 是「**用多少付多少**」，閒置幾乎不花錢。
- 最容易不小心產生費用的是：**NAT Gateway、跑著不關的 EC2 或 RDS、大量 CloudWatch 日誌、跨區傳輸**。本專案都沒用到。
- 一定要設 Budgets 警報（已設 US$5）。
- 每個月去 Console → **Billing and Cost Management** 看一次。

---

## 11. 面試常見問題

**Q：SQS 會重複送訊息，你怎麼避免重複入帳？**
A：靠兩層防護。第一層是 SQS FIFO 的 deduplication ID，處理 5 分鐘內的重送。第二層是 DynamoDB 的條件交易：每個事件寫入一個 `EVENT#id` 標記，條件是不存在，並且和餘額更新放在同一個 transaction。重複的事件會因為條件失敗而整筆取消。我刻意不用「先查再寫」，因為那有競態條件，我在前一份錢包工作中看過這種問題。測試用 20 個 worker 同時處理同一個事件，結果只入帳 1 次。

**Q：為什麼用 FIFO？用 Standard 不行嗎？**
A：FIFO 讓同一筆轉帳的「入帳」和「撤銷」保持順序，也提供 5 分鐘的去重。但更根本的是，我讓餘額只用加法更新，所以就算順序亂了，最後的總和也一樣。這等於多一層保險。如果吞吐量成為瓶頸，可以換成 Standard，正確性也不會受影響。

**Q：一則訊息一直處理失敗會怎樣？**
A：Lambda 回報 partial batch failure，SQS 只重試失敗的那一則。重試 5 次之後，訊息會移到 DLQ，觸發 CloudWatch 告警並寄信。問題修好之後用 redrive 把訊息送回來重新處理。因為消費者是冪等的，重送很安全。

**Q：部署到 AWS 時遇到什麼問題？**
A：最有價值的是 DynamoDB 的 TransactionConflict。多個 Lambda 同時更新同一個帳戶的餘額，本機模擬器完全測不出來。當時資料沒有錯，但部分訊息要等 3 分鐘的 visibility timeout 才會重試，所以我改成在程式內立刻重試，搭配退避和抖動。我也評估過其他做法，例如以帳戶當 message group、分片計數器、用 Streams 非同步彙總，最後選了最簡單、在目前流量下就足夠的方案。

**Q：CI/CD 怎麼存取 AWS？**
A：用 GitHub OIDC。GitHub Actions 拿到 GitHub 簽發的 token，向 AWS STS 換取 1 小時的臨時憑證。IAM role 的 trust policy 限定只有這個 repo 的 main 分支能使用，權限也只能操作名稱是 `usdc-ledger-*` 的資源。整個流程沒有任何長期金鑰。

**Q：為什麼用 Terraform，而不是在 Console 點？**
A：可重現、可審查、可預覽。我把它分成 bootstrap（state bucket、OIDC、部署角色，手動執行一次）和 app（由 CI 部署）兩層，解決 state 本身要放哪裡的問題，也讓 CI 沒有權限修改自己的權限。

---

## 12. 名詞對照表

| 英文 | 中文 | 一句話 |
|---|---|---|
| At-least-once delivery | 至少送達一次 | 保證送到，但可能重複 |
| Idempotency | 冪等 | 做 1 次和做 N 次結果一樣 |
| Race condition | 競態條件 | 多個執行緒同時操作，結果取決於誰先誰後 |
| Visibility timeout | 可見性逾時 | 訊息被取走後隱藏多久，沒刪除就重新出現 |
| DLQ / Redrive | 死信佇列 / 重新驅動 | 處理不了的訊息放這裡 / 修好後送回原佇列 |
| Message group | 訊息群組 | FIFO 裡保證順序的單位 |
| Event source mapping | 事件來源對應 | Lambda 自動從 SQS 拉訊息的機制 |
| Partial batch response | 部分批次回應 | 只回報一批中失敗的訊息 |
| Cold start | 冷啟動 | Lambda 閒置後第一次呼叫的額外延遲 |
| Partition key / Sort key | 分割鍵 / 排序鍵 | DynamoDB 的主鍵，PK 決定資料放在哪裡，SK 決定排序 |
| Conditional write | 條件寫入 | 符合條件才寫入 |
| Hot key / Hot partition | 熱點 | 太多請求集中在同一個 key |
| Exponential backoff + jitter | 指數退避加抖動 | 重試的等待時間倍增並隨機化 |
| Transactional outbox | 交易式外寄箱 | 把訊息和資料寫進同一個 DB transaction，再非同步送出 |
| IaC | 基礎設施即程式碼 | 用程式碼管理雲端資源 |
| State (Terraform) | 狀態檔 | Terraform 記錄自己建了哪些資源 |
| OIDC | OpenID Connect | 用簽章 token 證明身分，換取臨時憑證 |
| Trust policy | 信任政策 | 誰可以扮演這個 role |
| Least privilege | 最小權限 | 只給剛好夠用的權限 |
| ARN | 資源名稱 | AWS 資源的唯一識別碼 |
