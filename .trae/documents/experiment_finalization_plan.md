# 实验终止最终结果一致性（FINALIZING 终止流水线）实施计划

## 一、仓库调研结论

### 现状链路
- **状态机**：`Experiment.Status = WAITING(0)/RUNNING(1)/STOPPED(2)/FORCE_STOPPED(3)`（[experiment.proto](file:///Users/ding/Documents/swe/0910092/project-05/proto/experiment/experiment.proto)）。计划停止：batch 任务 [experiment_status_updater.go](file:///Users/ding/Documents/swe/0910092/project-05/pkg/batch/jobs/experiment/experiment_status_updater.go) 到点用 gRPC `UpdateExperiment(status=STOPPED)`；提前停止：UI 按钮发 `FORCE_STOPPED`（[experiment-state/index.tsx](file:///Users/ding/Documents/swe/0910092/project-05/ui/dashboard/src/pages/experiment-details/collection-loader/settings/experiment-state/index.tsx#L85-L95)）。两条路都直接落终止态。
- **入库链路**：subscriber 的 eval/goal DWH writer 攒批把事件写入 MySQL/Postgres/BigQuery（事件业务时间戳：客户端秒 → proto 微秒 → SQL DATETIME），**仓库中不存在任何 watermark 设施**；`evaluation_event` 表有入库时间列 `created_at` 但无人使用。
- **计算链路**：[experiment_calculate.go](file:///Users/ding/Documents/swe/0910092/project-05/pkg/batch/jobs/calculator/experiment_calculate.go) 每 30 分钟列出 RUNNING+STOPPED（stop_at 在近 2 天内）实验，用 Redis 分布式锁互斥，调 [experiment_calculator.go](file:///Users/ding/Documents/swe/0910092/project-05/pkg/experimentcalculator/experimentcalc/experiment_calculator.go) 重算并整体 upsert `experiment_result`（JSON），结束时间取 `listEndAt(start, stop, now)`。
- **读链路**：`EventCounterService.GetExperimentResult` 读 `experiment_result` 表（MySQL/PG 各一套 SQL，要求不改）。
- **实验缓存**：`{env}:experiment`（TTL=0），仅 subscriber 的 eval/goal writer 读取做事件归因；每分钟由 experimentCacher 全量重建，EXPERIMENT 领域事件由 cache_refresher 触发重建；列表为 RUNNING+STOPPED 且 stop_at≥now-2d。实验不参与 SDK 分流；feature flag 的编辑锁通过 `feature.existsRunningExperiment` + `Experiment.IsNotFinished` 实现。
- **领域事件**：`EXPERIMENT_UPDATED=211` 现用；`EXPERIMENT_FINISHED=209`、`ExperimentFinishedEvent` 枚举/消息已存在但未使用。
- **batch 编排**：k8s CronJob → gRPC `BatchService.ExecuteBatchJob(BatchJob enum)`（[proto/batch/service.proto](file:///Users/ding/Documents/swe/0910092/project-05/proto/batch/service.proto)，helm values 三处维护 job 列表）。
- **约束**：实验进行中禁止改 flag（`IsNotFinished`）；`DeleteGoal` 目前不校验实验引用；`UpdateExperiment` 禁止归档 RUNNING 实验。

### 核心设计
新增 **FINALIZING(4)** 中间态与持久化的**终止单（experiment_termination）**。提前/计划停止都只做「发起终止」（同一幂等身份），由新的 batch 任务 **ExperimentFinalizer** 按阶段推进：

```
RUNNING/WAITING --发起(幂等)--> FINALIZING
  阶段1 WAITING_WATERMARK : 固定配置快照 + eval/goal 两类事件水位，等待 subscriber 高水位达标（超时本轮跳过，保持 FINALIZING）
  阶段2 COMPUTING         : owner 租约持有者从同一快照输入计算最终统计 + 输入摘要；失锁后新 owner 用同一输入重算
  阶段3 RESULT_SAVED      : experiment_result upsert 成功（幂等）
  阶段4 CONFIRMED         : 经实验服务事务性确认：STOPPED/FORCE_STOPPED + final_result_id + 发 EXPERIMENT_FINISHED（解除与开关的运行期关联：flag 可编辑）
  阶段5 CACHE_PUBLISHED   : 重建并发布该环境实验缓存（已确认实验不再参与事件归因，缓存内容与快照一致），终止单 DONE
```

- **稳定终止身份**：终止单 `(environment_id, experiment_id)` 唯一；重复发起返回同一身份，不产生第二份结果。
- **编辑约束**：FINALIZING 期间拒绝 name/desc/period/archive 等草稿更新；feature 变体编辑（沿用 IsNotFinished）与被引用 goal 的删除在快照后一律拒绝。
- **不受影响项**：RUNNING 实时重算原样保留；无事件实验同样走流水线（空结果正常确认）；eventcounter 的 MySQL/PG 读取 SQL 不改；普通草稿（WAITING 未终止）更新不改；旧 STOPPED 数据（`final_result_id=''`）保留 2 天重算宽限。

## 二、文件与模块

### Proto（需重新生成）
- [proto/experiment/experiment.proto](file:///Users/ding/Documents/swe/0910092/project-05/proto/experiment/experiment.proto)：`Status` 增加 `FINALIZING = 4`；Experiment 增加 `string termination_id = 22; string final_result_id = 23;`。
- [proto/experiment/service.proto](file:///Users/ding/Documents/swe/0910092/project-05/proto/experiment/service.proto)：`ListExperimentsRequest.exclude_finalized = 16`；`UpdateExperimentRequest` 增加内嵌消息 `ConfirmFinalization{termination_id, final_result_id}` 与字段 `confirm_finalization = 17`。
- [proto/batch/service.proto](file:///Users/ding/Documents/swe/0910092/project-05/proto/batch/service.proto)：`ExperimentFinalizer = 22;`。
- 重新生成受影响的 `*.pb.go` / `*.pb.gw.go`（protoc v23.4，见「依赖与注意」）。

### 数据库迁移（Atlas，mysql + postgres 各一份）
- `experiment` 表加列：`termination_id VARCHAR(191) NULL`、`final_result_id VARCHAR(191) NULL`。
- 新表 `experiment_termination`：`id`(PK)、`environment_id`、`experiment_id`、`reason`(1计划/2提前)、`phase`、`snapshot`(JSON 固定配置)、`evaluation_watermark`/`goal_watermark`(bigint，微秒)、`input_summary`(JSON NULL)、`final_result_id`、`owner_id`、`owner_expires_at`、`attempts`、`last_error`、`created_at`、`updated_at`、`finalized_at`；`UNIQUE(environment_id, experiment_id)`。
- 新表 `event_watermark`：`(environment_id, event_type)` PK、`high_water_at`(bigint 微秒)、`updated_at`。
- 更新两库 `atlas.sum`。

### 领域层
- [pkg/experiment/domain/experiment.go](file:///Users/ding/Documents/swe/0910092/project-05/pkg/experiment/domain/experiment.go)：新增 `ErrExperimentFinalizing`；`EnterFinalizing()`、`ConfirmFinalized()`；`Update()` 在 FINALIZING 拒绝草稿/归档；`IsNotFinished()` 改为按状态显式判断（WAITING/RUNNING/FINALIZING=true）。
- 新增 `pkg/experiment/domain/termination.go`：`Termination`、阶段枚举、`Reason`、`ConfigSnapshot`、`InputSummary`（各 goal 的 eval/goal 分变体 user/event/value 计数 + 水位 + 计算时间）、owner 租约 `AcquireOwner/CAS/ExpireOwner`、阶段推进校验。

### 存储层（interface + mysql + postgres + 内嵌 SQL + mockgen）
- `pkg/experiment/storage/v2/experiment.go`：ListParams 加 `ExcludeFinalized`；新 `TerminationStorage`（Create 幂等冲突错误、Get、ListByPhases、CAS 改阶段、owner CAS、写 input_summary/final_result_id）。
- mysql/postgres 的 experiment.go 与 6 个 experiment SQL（insert/update/select×2 加两列；list 支持 `(status NOT IN (2,3) OR final_result_id='')`；summary 把 FINALIZING 计入 running）。
- 新增 termination 存储 mysql/postgres 实现与 SQL。
- 新增 `pkg/eventcounter/storage/v2/operational_database` 水位存储：`UpsertEventWatermark/GetEventWatermark`（mysql+postgres+mock），复用运营库客户端。

### 实验服务（web）
- [pkg/experiment/api/experiment.go](file:///Users/ding/Documents/swe/0910092/project-05/pkg/experiment/api/experiment.go) `UpdateExperiment`：
  - `confirm_finalization`：仅 FINALIZING 可确认；同事务校验终止单阶段=RESULT_SAVED 且 id 一致 → 置 STOPPED（计划）/FORCE_STOPPED（提前）+final_result_id+stopped_at，终止单→CONFIRMED，发 `EXPERIMENT_FINISHED`；重复确认幂等返回。
  - 普通 status=STOPPED/FORCE_STOPPED 且当前 WAITING/RUNNING：同事务创建终止单（快照+水位+termination_id）、状态→FINALIZING、发 `EXPERIMENT_UPDATED`；唯一键冲突时读回既有单，幂等成功。
  - FINALIZING 上的草稿更新/归档 → 拒绝。
- [goal.go](file:///Users/ding/Documents/swe/0910092/project-05/pkg/experiment/api/goal.go)：`DeleteGoal` 前查 FINALIZING 实验是否引用该 goal，引用则拒绝（新错误码）。
- feature 侧借 `IsNotFinished` 新语义自动拦截变体编辑（仅补测试，无需改流程代码）。

### Subscriber（水位采集 + 归因列表）
- [evaluation_events_dwh.go](file:///Users/ding/Documents/swe/0910092/project-05/pkg/subscriber/processor/evaluation_events_dwh.go) / [goal_events_dwh.go](file:///Users/ding/Documents/swe/0910092/project-05/pkg/subscriber/processor/goal_events_dwh.go)：注入 `WatermarkRecorder`；`AppendRows` 成功后按环境取本批成功事件最大时间戳 upsert `event_watermark`（best-effort，失败只打点记日志）；ListExperiments 加 FINALIZING 并置 `exclude_finalized=true`。
- [server.go](file:///Users/ding/Documents/swe/0910092/project-05/pkg/subscriber/cmd/server/server.go)：用既有运营库客户端构造水位存储并注入。

### 计算器
- [experiment_calculator.go](file:///Users/ding/Documents/swe/0910092/project-05/pkg/experimentcalculator/experimentcalc/experiment_calculator.go)：抽出 `compute(experiment, endAt)`，新增 `RunFinal(ctx, envId, snapshotExp, endAt)` 返回最终结果与 `InputSummary`；原 `Run`（实时，endAt=now）行为不变。

### 新 batch 任务
- 新增 `pkg/batch/jobs/finalizer/experiment_finalizer.go`（+metrics）：列环境→列 FINALIZING→按终止单阶段推进；owner=hostname+uuid、租约 TTL（复用 calculator 的 Redis lock 做同 pod 互斥，DB 租约做跨 pod owner 切换）；水位等待超时只结束本轮；各阶段失败下轮只补该阶段；CONFIRMED 后直接重建该环境实验缓存（复用 experimentCacher 的 Put 方式）。
- 接入 [pkg/batch/api/api.go](file:///Users/ding/Documents/swe/0910092/project-05/pkg/batch/api/api.go)（新 job 参数 + switch）、[batch/cmd/server/server.go](file:///Users/ding/Documents/swe/0910092/project-05/pkg/batch/cmd/server/server.go)（构造：termination 存储、水位存储、calculator 同款依赖、redis caches、30m timeout）、[jobs/metrics.go](file:///Users/ding/Documents/swe/0910092/project-05/pkg/batch/jobs/metrics.go) 常量 `JobExperimentFinalizer`。

### 缓存与既有计算任务
- [experimentcacher.go](file:///Users/ding/Documents/swe/0910092/project-05/pkg/batch/jobs/cacher/experimentcacher.go) 与 [cache_refresher.go](file:///Users/ding/Documents/swe/0910092/project-05/pkg/subscriber/processor/cache_refresher.go)：列表加 FINALIZING、置 `exclude_finalized`。
- [experiment_calculate.go](file:///Users/ding/Documents/swe/0910092/project-05/pkg/batch/jobs/calculator/experiment_calculate.go)：状态列表不变（RUNNING+STOPPED 2 天），Go 层跳过 `FinalResultId != ""` 的实验（冻结确认结果）；FINALIZING 自然不在列表中。

### UI（最小增量）
- [@types/experiment.ts](file:///Users/ding/Documents/swe/0910092/project-05/ui/dashboard/src/@types/experiment.ts) 加 `'FINALIZING'`；experiment-state 组件把 FINALIZING 视为进行中（禁用停止按钮、显示「结果生成中」）；列表状态映射与 i18n（en/ja）补一个标签。

### Helm
- batch cronjob 列表（charts/batch/values.yaml、charts/batch/values.dev.yaml、manifests/bucketeer/values.dev.yaml）增加 `experiment-finalizer / ExperimentFinalizer / "* * * * *"`。

## 三、实施步骤（依赖顺序）

1. **Proto 改造与代码生成**：3 个 proto 文件改动；用 Docker 跑 protoc v23.4 + pinned 插件执行 `make proto-all`（失败则手工等价修改生成文件）。
2. **迁移**：编写 mysql/postgres 新迁移；Docker 跑 `atlas migrate hash` 更新 atlas.sum。
3. **领域层**：experiment 状态机 + termination 领域类型与单测。
4. **存储层**：experiment 两列改造（mysql+pg+SQL+mock）、TerminationStorage、WatermarkStorage 与单测；`go generate mockgen`。
5. **实验服务**：UpdateExperiment 发起/确认分支、DeleteGoal 校验、错误码、api 单测。
6. **计算器**：final 模式与输入摘要抽取 + 单测。
7. **Subscriber**：水位上报 + 归因列表调整 + 单测；server 装配。
8. **Finalizer 任务**：状态机、owner、阶段补偿、缓存发布、metrics + 单测；batch api/server/proto enum/helm 接入。
9. **cacher/refresher/calculator job** 列表调整。
10. **UI**：状态类型、按钮态、i18n 标签。
11. 全量构建、gofmt、测试与自查。

## 四、依赖与注意事项

- 代码生成：宿主机无 protoc/atlas，Docker daemon 可用。优先容器化执行（镜像内下载 protoc-23.4 与 go.mod 锁定版本插件）；mockgen 用 `go install`/`go run` 锁定版本；底线方案是手工编辑生成文件并保证与 protoc 输出等价。
- 水位语义：subscriber 记录「已成功持久化事件的最大业务时间戳」（每环境每类型）。乱序迟到事件无法被 max 水位严格覆盖，属有意取舍：finalizer 固定水位 T=发起时刻，水位达标才计算；超时保持 FINALIZING 下分钟重试；确认摘关联后才到的残余事件在归因处丢弃（等同现有「2 天外丢弃」语义）。goal Redis 重试队列中超晚事件同理。
- BigQuery 部署：水位写运营库 MySQL/PG（subscriber 必有运营库），DWH 读取 SQL 零改动。
- 旧数据：已 STOPPED 实验 `final_result_id` 为空，2 天重算与缓存宽限行为完全保留，不做回填终止单。
- 迁移对存量行加可空列，不锁长表；新表独立。
- 时间单位：水位内部统一微秒（与入库 proto 一致），计算 endAt 仍用秒（与 eventcounter RPC 一致），边界处显式换算。

## 五、验证

- `go build ./...` 四个服务全部通过；`gofmt/goimports` 干净。
- 新增/修改表驱动单测：
  - domain：进入/确认转换、FINALIZING 拒绝草稿、IsNotFinished、终止单阶段守卫与 owner 过期接管；
  - api：发起幂等（重复触发同一身份）、确认校验与幂等、FINALIZING 编辑拒绝、goal 删除拒绝；
  - finalizer：水位未达/超时保持 FINALIZING、owner 失锁同输入重算、结果已存后确认 RPC 失败只补确认、缓存发布失败只补缓存、重启后按存储阶段续跑；
  - calculator：final 模式 endAt 固定与输入摘要内容；
  - subscriber：仅成功批推进水位、归因列表含 FINALIZING 排除已确认；
  - 运行 `go test ./pkg/experiment/... ./pkg/batch/... ./pkg/experimentcalculator/... ./pkg/subscriber/...`（DB 依赖测试无本地库时以编译+mock 测试为准）。
- UI：类型与分支自查（无 yarn 环境时保证 TS 改动语法/类型自洽）。
- 迁移文件通过 atlas 校验（容器内 `migration-validate`/hash 检查）。

## 六、风险与应对

- **protoc/atlas 不可得**：Docker 拉取工具链；再不行手工维护生成文件与 atlas.sum（仅枚举/字段增量，diff 可审）。
- **Stan 计算耗时/非确定性**：模型固定随机种子（现状已是 1234），同一快照输入结果可复现；final 计算与实时计算共用代码路径。
- **跨存储无事务（终止单在 batch、实验行在实验服务）**：以持久化阶段 + 幂等确认为边界，确认 RPC 内部同事务提交实验行/终止单/事件；任何中断由下轮按阶段补偿，不产生第二份结果（唯一键 + final_result_id 双重保证）。
- **FINALIZING 期间页面展示**：沿用最后一次实时结果行，状态展示「结果生成中」，确认后切换为冻结最终结果，避免残缺终态。
