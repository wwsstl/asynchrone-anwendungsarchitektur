# 技术栈

说明测试数据流水线（Testdaten-Pipeline）使用了哪些技术，以及为什么使用。依据是本目录中的文档：需求（[`anforderungen.md`](anforderungen.md)）、处理流程（[`funktionsweise_sequenz.md`](funktionsweise_sequenz.md)）和各张图（[`anwendungsarchitektur.mmd`](anwendungsarchitektur.mmd)、[`anwendungsarchitektur_user.mmd`](anwendungsarchitektur_user.mmd)、[`funktionsweise_sequenz.mmd`](funktionsweise_sequenz.mmd)、[`sandbox_sequenz.mmd`](sandbox_sequenz.mmd)）。版本号以 `pom.xml` 为准。德文版见 [`technologie.md`](technologie.md)。

## 总览

| 领域 | 技术 | 用途 |
|---|---|---|
| 运行环境 | Java 21（LTS） | 整个应用 |
| 应用框架 | Spring Boot 4.1 | 独立运行的应用、配置、依赖管理 |
| REST-API | Spring WebFlux（基于 Netty） | 任务的启动、中止和状态查询 |
| 云端对接 | Spring `WebClient` | 调用 Cloud-API 1、2、3 |
| 并发 | Virtual Threads（虚拟线程） | 每个 Sandbox 的 Producer 和 Consumer；REST-API 对 TaskManager 的调用 |
| 内存状态 | `ConcurrentHashMap`、`AtomicBoolean`、`AtomicInteger` | TaskRegistry、TaskHistory、Status-Pool、任务上下文 |
| JSON | Jackson 3 | 发给 Cloud-API 1 的批量请求、各云接口的返回、REST 响应 |
| 文件访问 | Java NIO.2 | 每个用户的 `inbox`、`pendingbox`、`donebox`、`errorbox` 目录 |
| 文档 | Mermaid（`.mmd`） | 架构图和时序图 |

## 各项技术的选用理由

### Java 21

Java 21 是当前的长期支持（LTS）版本，虚拟线程在这个版本中成为正式特性（JEP 444）。需求中明确提到，"使用 Java 21 时"可以为生产者和消费者进程配置虚拟线程（性能效率，Leistungseffizienz／线程管理）。有了虚拟线程，"每个任务一个 Sandbox、配专属线程"的模型才不必担心资源消耗（见下文"虚拟线程"）。

### Spring Boot 4

需求规定，应用要作为"独立、轻量的 Spring Boot 应用，部署在单台服务器上"（部署，Bereitstellung）。Spring Boot 为此提供：

- 自带内嵌服务器的可执行包，不需要单独的应用服务器；
- 各组件（TaskManager、TaskRegistry、TaskHistory、云端客户端）的依赖注入；
- 从 `application.yml` 读取的类型安全配置，例如批大小、背压阈值、各项时限和保留时长；
- 内置的 Jackson 库，需求明确要求使用它（性能效率，Leistungseffizienz／JSON 处理）。

### 用 Spring WebFlux 实现 REST-API

REST-API 接收用户的指令：启动任务、中止任务、查询状态（`funktionsweise_sequenz.md` 第 1、4、5 节）。它会立即返回（例如 `202 Accepted`），任务本身则在后台异步继续执行。

选用 WebFlux，是因为调用云端的客户端（`WebClient`）本来就基于 WebFlux。这样整个应用只有**一套** Web 框架，不必让 Spring MVC 和 WebFlux 并存。

TaskManager 的操作会阻塞（读写文件、加锁、等待线程退出）。所以 REST-API 把每次调用都交给一个虚拟线程执行，确保 Netty 的事件循环永远不被阻塞（`funktionsweise_sequenz.md` 第 1 节）。

### 用 `WebClient` 调用云服务

需求规定"必须使用非阻塞的 HTTP 客户端（如 Spring WebFlux 的 WebClient，或 Java 11 起自带的 HttpClient）"，以免生产者线程被阻塞（性能效率，Leistungseffizienz）。这里选用 `WebClient`：

- **同一套框架：** 它和 REST-API 属于同一套框架，Spring Boot 已为它预先配置好 Jackson 编解码器。
- **按接口设置超时和重试：** 这一点可以直接表达出来。之所以重要，是因为 Cloud-API 1 不是幂等的（`funktionsweise_sequenz.md` 第 2 节）：向 Cloud-API 1 的提交从不自动重试（`submit-retries: 0`），对 Cloud-API 2 的状态查询则可以重试。
- **批量调用：** 三个云接口都由它负责。Cloud-API 1 每批接收一个 JSON 请求，整个任务只会生成一个 batchgenAuftrag；Cloud-API 2 每轮接收一次批量状态查询；Cloud-API 3 在结束时设置任务状态。

### 虚拟线程

每个任务有自己的 Sandbox，里面有一个 Producer 线程和一个 Consumer 线程（`sandbox_sequenz.mmd`）。这是需求的要求：

- **独立的监控线程：** 由单独的线程并行监控生成状态（功能需求，Funktionale Anforderungen）。
- **互相隔离的任务：** 各任务独立运行、互不影响（功能需求），并分配专属线程（可扩展性，Skalierbarkeit／Sandbox 模式）。

这两个线程几乎一直在等待——等云端返回、等背压解除，或等下一个查询周期。等待中的虚拟线程不占用操作系统线程，所以即使用户很多，每个任务两个线程也几乎没有开销。若改用线程池（`ThreadPoolTaskExecutor`，需求中给出的另一种做法），就得确定合适的池大小，而且处于等待中的任务会占着池里的线程。

### 内存中的 `ConcurrentHashMap` 和原子类

第一阶段不使用任何外部中间件，只用"JVM 标准库（如 ConcurrentHashMap……）"（部署，Bereitstellung）。它们用于：

- **TaskRegistry：** 运行中的任务。需求要求有一个集中的任务注册表，例如用 `ConcurrentHashMap` 实现（可扩展性，Erweiterbarkeit）。
- **TaskHistory：** 已结束任务的最终状态，在 `task-retention` 期间可以查询（`funktionsweise_sequenz.md` 第 4 节）。
- **Status-Pool：** 每个 Sandbox 各有一个池，存放已提交文件的 TaskId（可扩展性，Skalierbarkeit／Sandbox 模式）。
- **任务上下文：** 中止信号用 `AtomicBoolean`，错误计数用 `AtomicInteger`——与需求中的原文一致（可扩展性，Skalierbarkeit）。

这些结构本身是线程安全的，不需要额外加锁；Producer、Consumer 和 REST-API 会同时访问它们。Registry、History 和 Pool 都放在各自的接口后面，以后可以换成 Redis、消息队列或数据库表，调用方不用改（可扩展性，Skalierbarkeit／可替换的中间件组件）。

### Jackson 3

需求要求使用"Spring Boot 内置的 Jackson 库"进行快速、安全的 JSON 处理（性能效率，Leistungseffizienz）。Jackson 负责序列化和反序列化：

- 发往 Cloud-API 1 的批量请求（把多个文件的 JSON 数据合成一个请求）；
- 各云接口的返回，例如哪些文件已转换、对应的 TaskId 是什么；
- REST-API 返回的任务状态。

Spring Boot 4 自带 Jackson 3（包名为 `tools.jackson.*`）。

### Java NIO.2

需求要求用 NIO.2（例如 `Files.move()`）批量读取和移动文件（性能效率，Leistungseffizienz）。每个文件都在用户的目录之间流转：`inbox` → `pendingbox` → `donebox` 或 `errorbox`。

在同一个文件系统内，`Files.move` 通过重命名完成移动，不复制数据。因此一个文件在任何时刻都只在一个目录里，所在目录就代表它的状态。中止后的恢复依赖这一点：`pendingbox` 里带有 TaskId 标记的文件会被继续查询状态，而不是重新提交（`funktionsweise_sequenz.md` 第 2 节）。

### 用 Mermaid 画图

图以文本（`.mmd`）形式存放在仓库里，和代码一起做版本管理，在 Pull Request 中可以按差异阅读，GitHub 上也能直接显示成图。

## 有意不采用的技术

| 技术 | 原因 |
|---|---|
| Spring MVC / Tomcat | 一套 Web 框架就够了；WebFlux 反正是 `WebClient` 所必需的。 |
| Java `HttpClient` | 按需求同样允许使用；但 `WebClient` 与 Spring Boot 结合得更好（编解码器、配置、重试）。 |
| `ThreadPoolTaskExecutor` | 虚拟线程每个任务几乎没有开销，也不需要确定池大小。 |
| Spring Batch | 目前未使用；核心逻辑（读取、提交、移动）的划分方式保证以后仍能迁移过去（可扩展性，Erweiterbarkeit／框架演进）。 |
| 消息队列（RabbitMQ、RocketMQ）、Redis、数据库 | 第一阶段在单台服务器上运行，不用外部中间件（部署，Bereitstellung）。Status-Pool、Registry 和目录管理的接口为以后的替换留好了余地；第二阶段推荐用关系型数据库取代消息队列和 Redis（见"展望"）。 |

## 展望：第二阶段

为了部署到多个实例上（容器/Pod，如 Kubernetes 或 Docker Swarm），需求中预先规划了对内存结构的替换（可扩展性，Skalierbarkeit；部署，Bereitstellung）。推荐的做法是把它们全部放进**同一个关系型数据库**：

| 现在 | 需求原计划 | 推荐方案 |
|---|---|---|
| 用户目录 `inbox`、`pendingbox`、`donebox`、`errorbox` | 带状态字段（INBOX、DONE、ERROR）的关系型数据库表 | 同样用表，另外增加"已占用"和"已提交"两个状态值 |
| 内存中的 Status-Pool | 分布式消息队列 | 同一数据库中的 `datei` 表 |
| 内存中的 TaskRegistry | 分布式缓存（Redis） | 同一数据库中的 `batchauftrag` 表；运行中的 Sandbox 仍留在负责它的 Pod 的内存里 |
| 内存中的 TaskHistory | – | `batchauftrag` 表中带最终状态的行 |

### Status-Pool → `datei` 表

```sql
datei (
  task_id, dateiname,   -- 主键
  status,               -- INBOX | BEANSPRUCHT | UEBERMITTELT | DONE | ERROR
  cloud_task_id,        -- 取代 pendingbox 里的 TaskId 标记
  naechste_pruefung,    -- 取代 PollEntry.nextPollAt
  versuche
)
```

| Status-Pool 的职责 | 在表上的实现 |
|---|---|
| 登记 TaskId | 把该行设为 `UEBERMITTELT`，写入 `cloud_task_id` |
| 取出到期条目 | `WHERE naechste_pruefung <= now()`；多个 Pod 并行时加 `FOR UPDATE SKIP LOCKED`，确保每行只被一个 Pod 处理 |
| `PENDING`：稍后再查 | 更新 `naechste_pruefung` |
| 背压（池子大小） | 按任务和状态 `COUNT(*)` |

在这里，消息队列反而不合适：

- **稍后再查：** "状态没变，过会儿再查"只能靠延迟重投实现，每次得到 `PENDING` 都要多转一圈消息。
- **背压：** 消息无法按任务计数。
- **批量查询：** 消息无法一次取出"所有到期的"，而对 Cloud-API 2 的批量查询正需要这样。

当初否决用 `DelayQueue` 做 Status-Pool，也是出于同样的原因。

### TaskRegistry → `batchauftrag` 表加本地内存

TaskRegistry 现在保存的是带线程的、正在运行的 Sandbox，这些既放不进数据库，也放不进 Redis。因此把它拆成两部分：

- **放进数据库：** 任务状态、所属用户、开始和结束时间、中止请求（`cancel_requested`），以及由哪个 Pod 负责（租约，Lease）。
- **留在负责它的 Pod 的内存里：** 运行中的 Sandbox，和现在一样。

由此得到：

- **跨 Pod 中止：** REST-API 设置 `cancel_requested`，负责的 Pod 在每轮巡检（`sweep-interval`）时读取这一列。想更快响应，可以用 PostgreSQL 的 `LISTEN/NOTIFY` 等机制。
- **每个用户同时只能有一个任务：** 用部分唯一索引实现（例如对运行中任务的 `user_id`），取代内存里的锁。
- **TaskHistory：** 不再需要单独的结构。已结束的任务就是带最终状态的行；`task-retention` 变成一条删除规则，重启后也不会丢。

### 相比消息队列和 Redis 的优点

- **组件更少：** 一个数据库，取代消息队列加 Redis。
- **状态和 TaskId 在同一个事务里：** 标记文件的那些问题（孤立的或只写了一半的标记）随之消失。
- **可以直接查询：** 不经过流水线也能查到任务状态。

### 需要注意

- **查询压力：** 每个运行中的任务每个 `sweep-interval` 都要查询一次到期的行。需要在 `(status, naechste_pruefung)` 上建索引，并设置合适的间隔。
- **中止不是即时生效：** 要到下一轮巡检才生效，也就是几秒之后；Redis 发布/订阅会更快。对手动中止来说，这通常足够了。
- **无法和文件系统放在同一个事务里：** 只要文件还放在文件系统里，就需要固定的先后顺序和可重复执行的步骤（[`batchauftrag_verwaltung_durch_cloud_api.md`](../batchauftrag_verwaltung_durch_cloud_api.md) 第 2.8 节）。
- **需要修改需求：** 第一阶段仍然不用中间件。到第二阶段，需要修改 [`anforderungen.md`](anforderungen.md) 中"消息队列和 Redis"的表述；[`architekturalternative_abgleichsschleife.md`](../architekturalternative_abgleichsschleife.md) 第 6 节给出了修改建议。
- **云端自己记录文件状态时：** 如果云端服务自己记录任务和每个文件的状态（见 [`batchauftrag_verwaltung_durch_cloud_api.md`](../batchauftrag_verwaltung_durch_cloud_api.md) 中的草案），`datei` 表会更简单。

现有的划分——REST-API、TaskManager、包含 Producer 和 Consumer 的 Sandbox——保持不变；替换的只是接口背后的具体实现。
