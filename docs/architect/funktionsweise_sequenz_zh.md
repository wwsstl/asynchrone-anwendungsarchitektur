# 任务处理流程（第一层）

时序图：[`funktionsweise_sequenz_old.mmd`](funktionsweise_sequenz_old.mmd)，是概览图 [`architect/anwendungsarchitektur.mmd`](architect/anwendungsarchitektur.mmd) 的第一层细化。各节编号与图中的阶段一一对应。德文原文见 [`architect/funktionsweise_sequenz.md`](architect/funktionsweise_sequenz.md)。

## 1. 启动

用户通过 REST-API 启动一个任务。API 基于 Spring WebFlux 实现，它把调用放到一个虚拟线程（Virtual Thread）上转交给 TaskManager，以免阻塞事件循环（Event-Loop）。

启动分四步进行。每一步的"检查"和"占用"都在运行登记表（Laufregister，见第 7 节）中以一个原子操作完成。因此，即使两个启动请求同时到达、甚至落在不同的实例上，也不会彼此看漏。

1. **占用用户名额：** TaskManager 在运行登记表中新建一条状态为 `STARTING` 的运行记录。如果该用户已有一条 `STARTING` 或 `RUNNING` 的运行记录，这一步就会失败，启动被拒绝，返回 `409 Conflict` 和原因代码 `USER_TASK_RUNNING`。
2. **确定 BatchgenAuftrag：** TaskManager 通过 Cloud-API 3 查询该用户是否已有状态为"RUNNING"的 BatchgenAuftrag（中止后接着处理，见第 5 节）：

   | 结果 | 处理 |
   |---|---|
   | 有 BatchgenAuftrag | TaskManager 沿用它的编号（接着处理）。 |
   | 没有 BatchgenAuftrag，但 `pendingbox` 中有文件 | 这些文件属于一个已中止的 BatchgenAuftrag。删除第 1 步建立的运行记录，启动被拒绝，返回 `409 Conflict` 和原因代码 `PENDINGBOX_NOT_EMPTY`，并提示用户先选择接着处理（a）或重新开始（b），见第 5 节。 |
   | 没有 BatchgenAuftrag，`pendingbox` 为空 | TaskManager 通过 Cloud-API 1 新建一个 BatchgenAuftrag。 |

3. **占用任务号：** BatchgenAuftrag 的编号同时就是任务号。TaskManager 把它和下一个运行序号写入这条运行记录，把状态设为 `RUNNING`，并记下由哪个实例执行。如果该任务号下已有一次运行在进行，这一步就会失败：删除第 1 步建立的运行记录，启动被拒绝，返回 `409 Conflict` 和原因代码 `TASK_ALREADY_RUNNING`。
4. **启动 Sandbox：** TaskManager 以该任务号建立一个独立的 Sandbox（上下文、Status-Pool、Producer 线程和 Consumer 线程），登记到本实例的 TaskRegistry 中，然后启动 Producer 和 Consumer。API 立即返回 `202 Accepted`、任务状态 `RUNNING`，以及供后续查询状态的地址。

先占用用户名额，**再**调用 Cloud-API 1。否则，同一用户几乎同时发起的两次启动可能各自新建一个 BatchgenAuftrag，其中一个会闲置在云端无人使用。

原因代码放在 REST-API 的返回中，界面可以针对不同原因给出相应的提示。

同一任务号每启动一次，就是一次新的运行，运行序号依次递增（1、2……）。只统计仍在运行登记表中的运行；超过 `task-retention`（8 小时）的旧运行不再计入。

## 2. Producer：提交

Producer 首先恢复之前任务已经提交过的文件。这些文件带着 TaskId 标记，留在 `pendingbox` 中；它们的 TaskId 直接放回 Status-Pool，不会再次提交。

随后它按批次处理 `inbox`：

1. 把最多 `batch-size` 个文件移到 `pendingbox`。
2. 把这些文件的 JSON 数据连同 BatchgenAuftrag 编号，合成一个请求提交给 Cloud-API 1。BatchgenAuftrag 从启动时就已存在（第 1 节），该任务的所有批次都属于它。Cloud-API 1 返回 JSON 数据。
3. Cloud-API 1 返回的 JSON 数据说明：哪些文件已成功转换为数据生成任务（各自附带 TaskId），哪些没有。
4. 对成功转换的文件，把 TaskId 保存为标记，并登记到 Status-Pool 中。
5. 未能转换的文件直接移到 `errorbox`。

由于 Cloud-API 1 不是幂等的，每个文件最多只提交一次。每提交完一批，Producer 都会等 Status-Pool 降到 `pool-resume-threshold` 或以下（背压），才读取下一批。

## 3. Consumer：跟踪状态

Consumer 与 Producer 并行运行。它每隔 `sweep-interval` 从 Status-Pool 中取出到期的 TaskId，合成一个请求，批量向 Cloud-API 2 查询它们的状态：

| 状态 | 处理 |
|---|---|
| `SUCCESS` | 文件从 `pendingbox` 移到 `donebox`，删除标记。 |
| `ERROR` | 文件移到 `errorbox`，删除标记，错误计数加一。 |
| `PENDING` | 条目留在 Status-Pool 中，隔 `poll-interval` 后再查。 |

此外，Consumer 每轮都通过 Cloud-API 3 查询 BatchgenAuftrag 的状态。如果状态是"CANCELLED"，Producer 和 Consumer 随即结束（见第 5 节）。

Consumer 每轮还会把当前进度（已提交、成功、失败和仍在处理中的文件数）作为进度快照写入运行登记表中自己的那条运行记录，并续租（Lease，见第 7 节）。

## 4. 状态查询

用户可以随时查询状态。返回结果一律来自运行登记表，而不是某个实例的内存，因此任何实例都能回答查询：

- **运行中的任务：** 返回这次运行的进度快照，最多比实际晚一个 `sweep-interval`。
- **正在中止的任务：** 如果运行记录仍是 `RUNNING`，而 Cloud-API 3 显示 BatchgenAuftrag 已是"CANCELLED"，就返回 `CANCELLING`（正在中止）。这个状态是在查询时推算出来的，不在任何地方保存。
- **已结束的任务：** 返回最近一次运行的最终状态，保留时长为 `task-retention`（默认 8 小时）。

运行登记表按"任务号 + 运行序号"保存每一次运行。因此，接着处理一个已中止的任务时，被中止那次运行的最终状态会保留下来，不会被覆盖。

## 5. 外部中止

用户中止任务时，REST-API 直接把中止请求转给 Cloud-API 3：由于任务号同时就是 BatchgenAuftrag 编号，REST-API 直接把该 BatchgenAuftrag 标为"CANCELLED"，并返回 `CANCELLING`。这个过程不读写本实例的任何状态，所以无论任务在哪个实例上运行，任何实例都能受理中止请求。

Consumer 每轮都通过 Cloud-API 3 查询 BatchgenAuftrag 的状态。一旦发现已标为"CANCELLED"，Producer 和 Consumer 都会结束——最晚在一个 `sweep-interval` 之后。在此之前，状态查询返回 `CANCELLING`（第 4 节）。

之后，执行该任务的实例上的 TaskManager 把最终状态 `CANCELLED` 写入运行登记表，再把 Sandbox 从本实例的 TaskRegistry 中移除。这样，任务在任何时刻都能查到，而线程、Status-Pool 和上下文都会被释放。

已经提交的文件带着标记留在 `pendingbox` 中。之后，用户有两条路可选：

- **a) 接着处理：** 用户手动把已中止的 BatchgenAuftrag 改回"RUNNING"，再启动一个任务。TaskManager 在启动时找到这个运行中的 BatchgenAuftrag，沿用它的编号（第 1 节）。Producer 恢复 `pendingbox` 中带标记的文件，不会重复提交（第 2 节）。
- **b) 重新开始：** 用户手动清空 `pendingbox`，再启动一个新任务；系统会为它新建一个 BatchgenAuftrag。

## 6. 结束与注销

任务以下面两种方式之一结束：

- **`COMPLETED`：** Producer 已处理完，Status-Pool 已清空。
- **`CANCELLED`：** 超过最长运行时间（`task-timeout`）——此时仍未完成的文件会移到 `errorbox`——或者达到错误上限（`error-threshold`）。

**超时或达到错误上限**时，走与外部中止相同的机制（第 5 节）：

1. Sandbox 把这一情况报告给本实例的 TaskManager。
2. TaskManager 通过 Cloud-API 3 把 BatchgenAuftrag 标为"CANCELLED"。这一步发生在运行该 Sandbox 的实例上，不需要共享状态。
3. 在下一轮巡检中，Consumer 发现这一状态，Producer 和 Consumer 随即结束。
4. TaskManager 把最终状态 `CANCELLED` 写入运行登记表，再把 Sandbox 从本实例的 TaskRegistry 中移除。

之后，同样有第 5 节中的两条路：接着处理或重新开始。

**正常结束：** Producer 处理完、Status-Pool 已清空时，Sandbox 通知 TaskManager。TaskManager 通过 Cloud-API 3 把 BatchgenAuftrag 标为"COMPLETED"，然后结束 Sandbox、释放其资源：把最终状态 `COMPLETED` 写入运行登记表，再把 Sandbox 从本实例的 TaskRegistry 中移除。

无论哪种情况，任务在任何时刻都能查到，而线程、Status-Pool 和上下文都会被释放。

## 7. 共享状态与多实例运行

将来应用要能部署在多个实例（容器/Pod）上。到那时，一个请求可能落在与任务运行所在实例不同的实例上。因此，凡是一个实例需要告知另一个实例的信息，都不能依赖某个实例的内存。状态按下表划分：

| 状态 | 存放位置 | 原因 |
|---|---|---|
| 运行中的 Sandbox（线程、Status-Pool、上下文） | 执行该任务的实例本地的 TaskRegistry | 无法共享，也不需要共享 |
| 运行记录：用户、任务号、运行序号、状态、执行实例、租约、进度、最终状态 | 运行登记表（共享） | 启动检查、状态查询和历史记录在每个实例上都必须得到相同结果 |
| 中止请求 | Cloud-API 3 中 BatchgenAuftrag 的状态 | 本来就位于所有实例之外 |
| 文件 | 放在共享卷上的用户目录 | 启动检查和接着处理都要读取 `pendingbox` |

一次运行的状态有 `STARTING`、`RUNNING`、`COMPLETED` 和 `CANCELLED`。运行登记表只提供以下原子操作：

- 占用用户名额；
- 占用任务号；
- 释放运行记录；
- 续租；
- 写入和读取进度快照；
- 写入最终状态。

这样一来，"检查"和"占用"永远不会分成两步。

- **第一阶段（单台服务器）：** 运行登记表采用内存实现，例如使用 `putIfAbsent` 和 `compute` 的 `ConcurrentHashMap`，不需要租约。原来的 TaskHistory 并入运行登记表。
- **第二阶段（多个实例）：** 运行登记表改为一张数据库表，带两个部分唯一索引：
  - 对处于 `STARTING` 或 `RUNNING` 的运行，按用户建唯一索引——每个用户同时只能有一个任务；
  - 对处于 `RUNNING` 的运行，按任务号建唯一索引——同一个 BatchgenAuftrag 不会被运行两次。

  `task-retention` 变成一条删除规则。REST-API 和 TaskManager 都不用改，替换的只是这些操作背后的实现。
- **实例宕机：** 执行任务的实例每轮巡检都为自己的运行续租。实例宕机后，租约到期，这次运行不再算作运行中，用户可以重新启动。由于云端的 BatchgenAuftrag 仍是"RUNNING"，文件也带着标记留在 `pendingbox` 中，重新启动时会接着处理这个任务（第 1 节）。
