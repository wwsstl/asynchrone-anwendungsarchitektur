# 补充要求：BatchgenAuftrag、CleanMassenanlageAuftrag与容器化

本文档约定BatchgenAuftrag的生命周期、启动和清理任务的规则，以及在多个POD上运行（容器化）时需要满足的前提。

按这些约定，应用不需要自己的数据库、运行注册表或跨POD的锁。所有需要在POD之间共享的状态都在云端（BatchgenAuftrag、子记录、lastSeen）和共享卷（用户目录）上；POD本身不保存任何需要共享的状态。

## 1. 基本概念

- **BatchgenAuftrag**：用户每启动一个任务，API1就在云端数据库中新建一个BatchgenAuftrag，初始状态为RUNNING。它的编号就是任务号。
- **子记录**：BatchgenAuftrag的子表保存需要生成的数据记录，每条记录有自己的状态（SUCCESS、ERROR、PENDING）。API2把这些状态提供给Status-Pool。
- **不续跑**：一个任务只对应一个BatchgenAuftrag。任务中断后，应用不再继续处理pendingbox中的遗留数据，而是由CleanMassenanlageAuftrag应用清理；之后用户启动的是一个新任务，对应一个新的BatchgenAuftrag。
- **CleanMassenanlageAuftrag**：独立程序，不属于本应用（见第6节）。

## 2. BatchgenAuftrag的状态

| 情况 | 状态 | 由谁设置 | pendingbox中剩余的数据 |
|---|---|---|---|
| 启动任务 | RUNNING | 云端（API1新建时） | — |
| inbox中的数据都已处理完，pendingbox为空 | COMPLETED | 应用 | 没有 |
| 用户中止 | CANCELED | 应用的中止接口（通过API3） | 留在pendingbox，由CleanMassenanlageAuftrag应用清理 |
| 达到错误阈值（error-threshold） | CANCELED | 应用（通过API3） | 留在pendingbox，由CleanMassenanlageAuftrag应用清理 |
| 超时（task-timeout） | ERROR | 应用（通过API3） | 应用把它们移到errorbox，不需要CleanMassenanlageAuftrag应用 |
| 应用意外退出（POD崩溃、OOM、节点故障等） | 保持RUNNING | — | 留在pendingbox，lastSeen超时后由CleanMassenanlageAuftrag应用清理 |
| 清理完成 | ERROR | CleanMassenanlageAuftrag应用 | 已经分配到最终目录 |

规则：

- ERROR只能由CleanMassenanlageAuftrag应用设置，唯一的例外是超时。
- “处理完”指inbox中的每条数据都有了最终结果。个别数据可以是ERROR（进入errorbox），只要没有达到错误阈值，状态仍然是COMPLETED。
- RUNNING或CANCELED的BatchgenAuftrag会挡住同一用户的新任务。只有当它变为COMPLETED或ERROR之后，用户才可以启动新的任务（见第3节）。

## 3. 启动任务

### 3.1 唯一性由云端保证

同一用户在同一时刻最多只能拥有一个状态为RUNNING或CANCELED的BatchgenAuftrag。API1新建BatchgenAuftrag时，如果该用户已经有这样的BatchgenAuftrag，云端在同一个原子操作中拒绝新建，并返回已有BatchgenAuftrag的编号和状态（例如409 {jobId, status}）。在云端数据库中相当于：UNIQUE (user_id) WHERE status IN ('RUNNING', 'CANCELED')。

原因：容器化之后，同一用户的多次启动请求（例如双击、客户端超时后重试、多个浏览器标签页）可能被分配到不同的POD，各POD里的TaskManager互相看不到对方。只有云端能在所有POD之间保证唯一。

### 3.2 步骤

1. 检查pendingbox是否为空。按第2节的约定，COMPLETED或ERROR之后pendingbox应该是空的，这一步是保险措施。
   - 为空->第2步。
   - 不为空->不调用API1，返回409。为了给出正确的原因代码，通过API3查询该用户状态为RUNNING或CANCELED的BatchgenAuftrag：RUNNING->USER_TASK_RUNNING，CANCELED->CLEANUP_REQUIRED，都没有->PENDINGBOX_NOT_EMPTY（见第7节）。这次查询只用来给出提示，唯一性仍然只由API1保证。
2. 调用API1新建BatchgenAuftrag。成功->以返回的编号启动Sandbox，返回202。
3. API1拒绝新建->返回409，原因代码取决于已有BatchgenAuftrag的状态：RUNNING->USER_TASK_RUNNING，CANCELED->CLEANUP_REQUIRED。

### 3.3 特殊情况

下面两种情况都按应用意外退出处理：BatchgenAuftrag保持RUNNING，但没有Sandbox在处理它；lastSeen超时后由CleanMassenanlageAuftrag应用清理。

- API1调用超时，但云端实际上已经新建了BatchgenAuftrag：应用返回502。
- BatchgenAuftrag新建成功之后、Sandbox启动之前POD崩溃。

## 4. 任务运行期间：中止检测和心跳（lastSeen）

Consumer在每一轮（sweep-interval）通过API3：

1. 读取BatchgenAuftrag的状态。不再是RUNNING（CANCELED、ERROR或COMPLETED）->停止任务。
2. 更新lastSeen。lastSeen由云端在收到更新时以云端时间写入，避免各POD时钟不一致。

lastSeen用来区分“任务还在某个POD上运行”和“应用已意外退出”，避免CleanMassenanlageAuftrag应用和仍在运行的Sandbox同时处理pendingbox：

- 心跳超时时间可配置（例如3个sweep-interval），应用和CleanMassenanlageAuftrag应用使用相同的值。
- 如果Consumer连续无法更新lastSeen（例如API3不可达），它要在心跳超时之前就自己停止任务（例如连续2个sweep-interval更新失败就停止）；BatchgenAuftrag保持RUNNING，按应用意外退出处理。这样lastSeen一旦超时，就可以确定没有Sandbox还在处理这个BatchgenAuftrag。
- 判断lastSeen是否超时必须以云端时间为准：API3直接返回距离lastSeen已经过去的时间（或者同时返回云端的当前时间），不能用其他机器的时钟去和lastSeen比较。
- lastSeen是单独的字段，不用BatchgenAuftrag的Modified字段代替：否则Consumer每一轮都要重写状态，可能覆盖同时发生的中止（CANCELED）。

## 5. 状态查询

API2/API3可以按BatchgenAuftrag汇总子记录的状态（SUCCESS、ERROR、PENDING各有多少）。状态查询因此完全从云端读取：BatchgenAuftrag的状态（RUNNING、CANCELED、COMPLETED、ERROR）、这些汇总数量和lastSeen。

- 任何一个POD都可以回答状态查询，应用自己不需要保存快照、历史或终态。
- 用户根据lastSeen判断RUNNING的任务是否还在运行。
- 只在本地存在的计数（例如文件移动失败）不包含在结果中。
- 跨POD无法得知Sandbox是否还在收尾，因此不提供CANCELLING，直接返回云端的CANCELED。

## 6. CleanMassenanlageAuftrag应用

独立程序，清理某个用户状态为RUNNING或CANCELED的BatchgenAuftrag：

1. lastSeen仍然新鲜->拒绝处理，并提示“任务仍在运行，请稍后再试”。只有lastSeen已经超时，才继续下一步。
2. pendingbox不为空->根据API2的最新数据状态，把剩下的数据分配到最终目录（donebox或errorbox），直到pendingbox为空，然后把BatchgenAuftrag标为ERROR。
3. pendingbox为空->直接把BatchgenAuftrag标为ERROR。

特例：pendingbox无法清空（例如有数据在云端一直没有最终状态）->人工清空pendingbox，然后再运行一次CleanMassenanlageAuftrag应用；这时pendingbox为空，它直接把BatchgenAuftrag标为ERROR。

## 7. 原因代码

| 代码 | HTTP | 含义 | 给用户的提示 |
|---|---|---|---|
| USER_TASK_RUNNING | 409 | 已有状态为RUNNING的BatchgenAuftrag：任务仍在运行，或者应用已意外退出 | 查看状态（含lastSeen）；lastSeen已超时的话启动CleanMassenanlageAuftrag应用 |
| CLEANUP_REQUIRED | 409 | 已有状态为CANCELED的BatchgenAuftrag | 启动CleanMassenanlageAuftrag应用；它拒绝处理的话（Sandbox还在收尾），稍后再试 |
| PENDINGBOX_NOT_EMPTY | 409 | 没有RUNNING或CANCELED的BatchgenAuftrag，pendingbox却不为空。例如超时后有文件没能移到errorbox，或者有人在云端直接修改了状态 | 人工清空pendingbox |
| CLOUD_UNAVAILABLE | 502 | API1或API3不可达，或者调用超时 | 稍后再试；如果之后得到USER_TASK_RUNNING，按第3.3节处理 |

## 8. 部署

应用不续跑，部署（滚动更新、重启POD）时正在运行的任务会按应用意外退出处理。因此部署时要确保没有任务在运行：

1. 先停止接受新的任务：启动接口暂时拒绝请求（例如返回503）。可以在应用中实现维护模式，也可以在Ingress上暂时屏蔽启动接口。否则在检查和部署之间仍可能有用户启动新任务。
2. 等到云端没有正在运行的任务：通过API3确认没有状态为RUNNING且lastSeen仍然新鲜的BatchgenAuftrag。
3. 部署，然后重新开放启动接口。

非计划的重启（例如OOM、节点故障）无法避免，仍按应用意外退出处理（见第2节）。

部署时需要一个共享卷（ReadWriteMany，例如NFS或PVC）存放用户目录，所有POD和CleanMassenanlageAuftrag应用都挂载它。文件在这个卷上的移动必须是原子的。
