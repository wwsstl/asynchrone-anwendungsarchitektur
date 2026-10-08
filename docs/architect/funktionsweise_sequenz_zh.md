# 一项任务的执行过程

## 基本原则

本应用只负责让云端生成数据，并根据云端返回的生成状态处理本地的文件：

- 云端的服务在BatchgenAuftrag的子表里生成数据。本地的每个文件对应子表里的一条数据。
- 子记录的状态由云端自己更新；BatchgenAuftrag的结束状态由应用通过Cloud-API 3写入（见第5、6节）。
- 不续跑：新任务不会接管之前的任务留在`pendingbox`中的文件（见第7节）。

| 概念 | 含义 |
|---|---|
| 任务号 | BatchgenAuftrag的编号。每个任务只生成一个BatchgenAuftrag，这个任务提交的所有批次都属于它。 |
| TaskId | 子表里一条数据的编号，对应一个本地文件；作为标记和文件一起保存在`pendingbox`中。 |
| `inbox` | 等待提交的文件 |
| `pendingbox` | 已经交给Cloud-API 1、还没有结果的文件 |
| `donebox` | 生成成功的文件 |
| `errorbox` | 没有转换成功或生成出错的文件 |

## 1. 启动

用户通过REST-API启动一项任务。REST-API用Spring WebFlux实现，它在一个虚拟线程上把调用转交给TaskManager，以免阻塞事件循环（Event-Loop）。每个用户同一时间最多只运行一项任务。

TaskManager按以下步骤启动任务：

1. **检查`pendingbox`：** 不为空说明还留有之前的任务没有处理完的文件（见第7节）。这时不新建BatchgenAuftrag，启动被拒绝，返回`409 Conflict`和原因代码`PENDINGBOX_NOT_EMPTY`，提示用户先处理这些文件。
2. **新建BatchgenAuftrag：** 通过Cloud-API 1在云端数据库中新建一个BatchgenAuftrag，初始状态为RUNNING，它的编号就是任务号。第一阶段云端不限制同一用户拥有的BatchgenAuftrag数量，即使之前的BatchgenAuftrag仍处于RUNNING，也可以新建。
   调用失败时不创建Sandbox，失败按第2节的方式分类：
   - **明确没有被处理**（连接失败、请求被拒绝）：云端不可用，启动被拒绝，返回`502 Bad Gateway`和原因代码`CLOUD_UNAVAILABLE`。
   - **结果不明**（超时：在`submit-timeout`之内没有应答、其他`5xx`、连接中断、应答无法解析或无效）：不再做任何操作，启动被拒绝，返回`504 Gateway Timeout`和原因代码`TIMEOUT`。原因里写明是哪个调用、出了什么问题，例如`Cloud-API 1 (BatchgenAuftrag anlegen): Ergebnis unklar, keine Antwort innerhalb von submit-timeout`。如果云端实际上已经新建了BatchgenAuftrag，它会保持RUNNING，需要人工修正（见第7节）。
3. **启动Sandbox：** TaskManager为这项任务构建一个独立的Sandbox（上下文、Status-Pool、Producer线程和Consumer线程），把它登记到TaskRegistry中，然后启动Producer和Consumer。REST-API立即返回`202 Accepted`、任务状态`RUNNING`以及之后查询状态用的地址。

## 2. Producer——提交数据

Producer分批处理`inbox`中的文件。每一批：

1. 把最多`batch-size`个文件移到`pendingbox`。Cloud-API 1不是幂等的，每个文件最多只提交一次，所以文件在调用之前就移出`inbox`。
2. 把这些文件的JSON数据连同任务号，通过一次请求提交给Cloud-API 1。
3. Cloud-API 1返回的JSON数据说明哪些文件已经成功转换为数据生成任务，哪些没有。每个成功转换的文件都带有子表里生成的数据的编号，也就是该文件的TaskId。
4. 成功转换的文件：把TaskId保存为标记，并把它登记到Status-Pool中。
5. 没有转换成功的文件：直接移到`errorbox`，错误计数加一；它们和第3节中状态为`ERROR`的文件一样，都计入错误阈值。
6. 等到Status-Pool中的数量降到`pool-resume-threshold`或更低（反压），再读取下一批。

Producer等待Cloud-API 1的应答时是阻塞的，但时间很短：应答只说明哪些文件可以开始轮询，哪些直接转到`errorbox`。Producer运行在虚拟线程上，这段等待不占用平台线程。即使应答很慢，Producer也会在`submit-timeout`之内一直等到应答回来，之后才继续处理这一批并读取下一批。

调用Cloud-API 1失败时，这一批文件还没有TaskId标记，按失败的类型处理：

| 失败类型 | 例子 | 处理 |
|---|---|---|
| 明确没有被处理 | 连接失败、请求被拒绝 | 文件移回`inbox`。Producer等待一段时间之后（例如一个`sweep-interval`，连续失败时等待时间逐渐变长）会重新读到它们并再次提交，因此Cloud-API 1暂时不可达时，同一个任务就能自动恢复。如果Cloud-API 1一直不可用，任务最终以`TIMEOUT`结束（见第6节）；这些文件从来没有提交成功，因此留在`inbox`中，下一次启动任务时照常提交。 |
| 结果不明 | 超时（在`submit-timeout`之内没有应答）、其他`5xx`、连接中断、应答无法解析 | Cloud-API 1可能已经处理了这批文件。为了不重复提交，它们不带TaskId标记留在`pendingbox`中，由用户处理（见第7节）。除此之外不再做任何操作：任务立即以`TIMEOUT`结束（见第6节），Producer不再读取下一批，其余文件留在`inbox`中。 |

## 3. Consumer——跟踪状态

与Producer并行，Consumer在每个`sweep-interval`从Status-Pool中取出到期的TaskId，把它们合并成一次请求，批量向Cloud-API 2查询状态：

| 状态 | 结果 |
|---|---|
| `SUCCESS` | 文件从`pendingbox`移到`donebox`；标记被删除。 |
| `ERROR` | 文件移到`errorbox`；标记被删除，错误计数加一。 |
| `PENDING` | 条目留在Status-Pool中，`poll-interval`之后再次检查。 |

状态查询失败时（`cloud.retries`次重试之后仍然失败），这些条目按`PENDING`处理，`poll-interval`之后再次查询。Cloud-API 2只读不写，重试没有副作用，所以任务继续运行。如果Cloud-API 2一直不可用，任务以`TIMEOUT`结束（第6节）；如果最后一次状态查询失败了，结束原因里会写明这次调用及其原因。

## 4. 状态查询

用户可以随时查询状态，不需要TaskHistory组件：

- **云端（随时可查）：** BatchgenAuftrag的状态，以及子记录的状态（SUCCESS、ERROR、PENDING各有多少）。BatchgenAuftrag的状态也包括应用写入的结束状态`COMPLETED`、`TIMEOUT`、`ERROR`和`TERMINATED`（见第5、6节），因此任务结束后也能查到它是怎样结束的。
- **本地（只在任务运行期间，即Sandbox还在TaskRegistry中）：** 内存中的状态，包括各状态的数量；设置了结束状态之后还包括结束原因（例如哪个Cloud-API的哪次调用出了问题；原因同时写入日志，但不写入云端）。它只包括Consumer已经处理过的结果，可能比云端的数字稍晚一些。任务结束后，只能从云端查询。

## 5. 外部中止

用户中止一项任务时：

1. TaskManager设置中止信号，Producer和Consumer随之结束。已经提交的文件带着标记留在`pendingbox`中。
2. TaskManager通过Cloud-API 3把BatchgenAuftrag的状态设为`TERMINATED`。
3. TaskManager把Sandbox从TaskRegistry中移除，线程、Status-Pool和上下文都被释放。

留在`pendingbox`中的文件由用户在下一次启动任务之前处理（见第7节）。

## 6. 结束与注销

任务自行结束有以下四种方式。TaskManager都会通过Cloud-API 3把相应的结束状态写入BatchgenAuftrag：

| 结束方式 | 条件 | BatchgenAuftrag的状态 | `pendingbox` |
|---|---|---|---|
| 正常结束 | Producer已经完成（`inbox`中的文件都已提交），并且Status-Pool为空 | `COMPLETED` | 为空 |
| 超时 | 超过了最长运行时间（`task-timeout`） | `TIMEOUT` | 还没有结果的文件留下 |
| Cloud-API 1结果不明 | 提交某一批的结果不明，例如直到`submit-timeout`都没有应答（第2节） | `TIMEOUT` | 还没有结果的文件留下；另外还有结果不明的那一批，没有TaskId标记 |
| 达到错误阈值 | 错误计数达到`error-threshold` | `ERROR` | 还没有结果的文件留下 |

还没有结果的文件在云端还没有结果，并不一定出错，因此不移到`errorbox`，而是连同TaskId标记留在`pendingbox`中（见第7节）。

两个线程都结束之后，最后结束的那个线程向TaskManager注销这项任务：TaskManager把Sandbox从TaskRegistry中移除，线程、Status-Pool和上下文都被释放。

## 7. 遗留文件与人工处理

以下情况会在`pendingbox`中留下文件：

| 来源 | 留下的文件 |
|---|---|
| 超时、达到错误阈值、Cloud-API 1结果不明（第6节） | 已经提交、还没有结果的文件，带TaskId标记 |
| 外部中止（第5节） | 已经提交、还没有结果的文件，带TaskId标记 |
| 应用崩溃 | 已经提交、还没有结果的文件，带TaskId标记；崩溃时正在提交的那一批没有标记 |
| 调用Cloud-API 1结果不明（第2节） | 这一批文件，没有TaskId标记 |

这些文件由用户在下一次启动任务之前自己处理。启动时的检查（第1节）保证这一点：`pendingbox`不为空时，启动被拒绝（`PENDINGBOX_NOT_EMPTY`）。

以下情况下，BatchgenAuftrag会保持RUNNING，需要人工修正：

- 应用崩溃，没有机会写入结束状态。
- 结束状态通过Cloud-API 3写入失败（例如云端暂时不可达）。
- 启动时调用Cloud-API 1结果不明（例如超时）、而云端实际上已经新建了BatchgenAuftrag（第1节）。
