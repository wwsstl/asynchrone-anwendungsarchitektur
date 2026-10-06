# 一项任务的执行过程

本应用只负责让云端生成数据：云端的服务在BatchgenAuftrag的子表里生成数据，并由云端自己保持状态更新。应用根据云端返回的生成状态处理本地的文件。只有云端无法知道的结束状态（正常结束、超时、达到错误阈值、人工中止）才由应用通过Cloud-API 3写入BatchgenAuftrag（见第5、6节）。

## 1. 启动

用户通过REST-API启动一项任务。REST-API用Spring WebFlux实现，它在一个虚拟线程上把调用转交给TaskManager，以免阻塞事件循环（Event-Loop）。

TaskManager首先检查该用户的`pendingbox`是否为空。不为空说明还留有之前的任务没有处理完的文件（超时、达到错误阈值、外部中止、应用崩溃，或者提交结果不明，见第2节），这时不新建BatchgenAuftrag，启动被拒绝，返回`409 Conflict`和原因代码`PENDINGBOX_NOT_EMPTY`，提示用户先处理`pendingbox`中的文件。

`pendingbox`为空时，TaskManager通过Cloud-API 1在云端数据库中新建一个BatchgenAuftrag，初始状态为RUNNING。它的编号就是任务号；每个任务只生成一次BatchgenAuftrag，这个任务提交的所有批次都属于它。第一阶段云端不限制同一用户拥有的BatchgenAuftrag数量，即使之前的BatchgenAuftrag仍处于RUNNING，也可以新建。

云端连不上（Cloud-API 1不可达或调用失败）时，启动被拒绝，返回`502 Bad Gateway`，不创建Sandbox。如果调用超时、而云端实际上已经新建了BatchgenAuftrag，这个BatchgenAuftrag会保持RUNNING，同样需要人工修正（见第6节）。

TaskManager为这项任务构建一个独立的Sandbox（上下文、Status-Pool、Producer线程和Consumer线程），把它登记到TaskRegistry中，然后启动Producer和Consumer。REST-API立即返回`202 Accepted`、任务状态`RUNNING`以及之后查询状态用的地址。每个用户同一时间最多只运行一项任务。

第一阶段不续跑：新任务不会接管之前的任务留在`pendingbox`中的文件；启动时的检查保证这些文件已经由用户处理完（见第5、6节）。

## 2. Producer——提交数据

Producer分批处理`inbox`中的文件：

1. 把最多`batch-size`个文件移到`pendingbox`。
2. 把这些文件的JSON数据连同BatchgenAuftrag的编号，通过一次请求提交给Cloud-API 1。Cloud-API 1返回JSON数据。
3. Cloud-API 1返回的JSON数据说明哪些文件已经成功转换为数据生成任务（每个都带有一个TaskId），哪些没有。
4. 对于成功转换的文件，把TaskId保存为标记，并把它登记到Status-Pool中。
5. 没有转换成功的文件直接移到`errorbox`，错误计数加一；它们和第3节中状态为`ERROR`的文件一样，都计入错误阈值。

Producer等待Cloud-API 1的应答时是阻塞的，但时间很短：应答只说明哪些文件可以开始轮询（登记到Status-Pool），哪些直接转到`errorbox`。Producer运行在虚拟线程上，这段等待不占用平台线程。

由于Cloud-API 1不是幂等的，每个文件最多只提交一次。因此文件在调用Cloud-API 1之前就移到`pendingbox`；调用失败时，按失败的类型处理这一批文件（它们还没有TaskId标记）：

- **明确没有被处理**（例如连接失败、请求被拒绝）：把这批文件移回`inbox`。Producer等待一段时间之后（例如一个`sweep-interval`，连续失败时等待时间逐渐变长）会重新读到它们并再次提交，因此Cloud-API 1暂时不可达时，同一个任务就能自动恢复。如果Cloud-API 1一直不可用，任务最终以`TIMEOUT`结束（见第6节）；这些文件从来没有提交成功，因此留在`inbox`中，下一次启动任务时照常提交。
- **结果不明**（例如超时、没有应答）：Cloud-API 1可能已经处理了这批文件。为了不重复提交，它们留在`pendingbox`中，由用户在下一次启动任务之前自己处理（见第1、6节）。

每提交一批之后，Producer会等到Status-Pool中的数量降到`pool-resume-threshold`或更低（反压），然后才读取下一批。

## 3. Consumer——跟踪状态

与Producer并行，Consumer在每个`sweep-interval`从Status-Pool中取出到期的TaskId，把它们合并成一次请求，批量向Cloud-API 2查询状态：

| 状态 | 结果 |
|---|---|
| `SUCCESS` | 文件从`pendingbox`移到`donebox`；标记被删除。 |
| `ERROR` | 文件移到`errorbox`；标记被删除，错误计数加一。 |
| `PENDING` | 条目留在Status-Pool中，`poll-interval`之后再次检查。 |

## 4. 状态查询

用户可以随时查询状态：

从云端读取BatchgenAuftrag的状态以及子记录的状态（SUCCESS、ERROR、PENDING各有多少），不需要TaskHistory组件。BatchgenAuftrag的状态也包括应用写入的结束状态`COMPLETED`、`TIMEOUT`、`ERROR`和`TERMINATED`（见第5、6节），因此任务结束后也能查到它是怎样结束的。

## 5. 外部中止

用户中止一项任务时，TaskManager设置中止信号，Producer和Consumer随之结束，已经提交的文件带着标记留在`pendingbox`中。TaskManager通过Cloud-API 3把BatchgenAuftrag的状态设为`TERMINATED`。

最后，TaskManager把Sandbox从TaskRegistry中移除。同时线程、Status-Pool和上下文都被释放。

留在`pendingbox`中的文件由用户在下一次启动任务之前自己处理。

## 6. 结束与注销

任务自行结束有以下三种方式：

- **`COMPLETED`：** Producer已经完成（`inbox`中的文件都已提交），并且Status-Pool为空。TaskManager通过Cloud-API 3把BatchgenAuftrag的状态设为`COMPLETED`。
- **`TIMEOUT`：** 超过了最长运行时间（`task-timeout`）。TaskManager通过Cloud-API 3把BatchgenAuftrag的状态设为`TIMEOUT`。
- **`ERROR`：** 达到了错误阈值（`error-threshold`）。TaskManager通过Cloud-API 3把BatchgenAuftrag的状态设为`ERROR`。

`COMPLETED`时`pendingbox`为空，唯一的例外是提交结果不明的文件（第2节）。其他结束方式下，`pendingbox`中可能留有文件：超时（`TIMEOUT`）、达到错误阈值（`ERROR`）、外部中止（第5节）和应用崩溃时，还没有结果的文件都连同TaskId标记留在`pendingbox`中，由用户在下一次启动任务之前自己处理（启动时的检查见第1节）。这些文件在云端还没有结果，并不一定出错，因此不移到`errorbox`。应用崩溃时没有机会写入结束状态，BatchgenAuftrag保持RUNNING，需要人工修正；结束状态通过Cloud-API 3写入失败时（例如云端暂时不可达）也是如此。

两个线程都结束之后，最后结束的那个线程向TaskManager注销这项任务：TaskManager把Sandbox从TaskRegistry中移除，线程、Status-Pool和上下文都被释放。
