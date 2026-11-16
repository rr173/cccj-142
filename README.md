# iterflow — 本地迭代数据流运行时（受控反馈边 + 逻辑轮次终止判定）

一个用 Python 标准库 + SQLite 实现的本地数据流运行时。使用者在**加工图**中声明
source / map / filter / union / sink 节点与**受控反馈边**，运行时按携带
`(epoch, round)` 逻辑时间的消息持续执行，并对「某一轮 / 某个 epoch 是否真的
完成」给出可证明的判定，而不是靠队列暂时为空、工人闲置、睡眠或哨兵记录来猜。

## 模型

- **消息**：`(id, epoch, round, value, parent)`。`id` 即路径身份（如
  `A>m[0]~m->f_loop>m[0]`），map 输出可重复且保留派生链；袋语义，不按业务 id 消重。
- **边**：普通边保留 `(epoch, round)`；**feedback 边**只把 `round+1`，不改 epoch。
  每个环必须经过 feedback 边；去掉反馈边后必须是 DAG（启动前校验，非法无反馈环直接报错）。
- **source**：调用方送入有限批记录，从 `round=0` 进入。epoch 必须在运行前声明，
  运行中不能新增；多个 source 各自独立 `close_epoch(e)`，全部关闭后该 epoch 输入封闭；
  关闭后再发送会被明确拒绝（`SourceStateError`）。
- **map**：同步一入零/多出纯转换，每次调用输出数有可配置上限（超限即失败）。
  图文件里只能用内置声明映射（`dict_lookup` / `duplicate` / `identity`），
  Python API 可注册纯回调；**不会从文件执行任意代码**。
- **filter**：只决定是否继续。**union**：袋汇合，保留重数。
- **sink**：收到记录立即交给消费者；消费者 `ack()` 后该记录义务才解除。
  某 epoch 的 `epoch_complete` 只在「所有 source 关闭该 epoch 且任意轮次都不可能
  再产生消息」时发出**一次**。

## 终止判定（核心）

每条消息实例从创建到 sink 确认 / 被 filter 丢弃 / 被转换为下游输出，都是一个
**义务（obligation）**。转换在**同一把锁**内原子地「退役输入 + 创建全部输出」，
因此任何 `(epoch, round)` 的在途计数不会因为消息正在节点间移动而瞬时归零。

> `(e, r)` 完成 ⟺ 所有 source 已关闭 e 且 e 的所有 `round ≤ r` 在途计数为 0。

轮次沿边单调不减（普通边保持、反馈边 +1），所以 `round ≤ r` 的消息只能由
`round ≤ r` 的消息派生；一旦上述条件成立就永远成立——完成判定是单调的，
恰好通知一次。`epoch_complete` 是该条件对所有轮次成立（在途总数为 0）的特例。

`rt.frontier(epoch)` 公开当前未完成逻辑时间集合的**最小边界**及来源说明，
类别与运行事实一致：

| 类别 | 含义 |
|---|---|
| `source_open` | 尚未关闭该 epoch 的 source（不是消息，但会注入 round 0） |
| `queued` | 普通边有界内存缓冲中的记录（定位到边） |
| `transforming` | 工人手持：正在处理的输入、已算出尚未交给普通边的输出、source 发送途中 |
| `feedback_pending` | 反馈边在途，细分为 `held`（已分配 round+1、发送前，如被屏障拦住）/ `buffered`（内存）/ `spilled`（SQLite 邮箱） |
| `sink_unacked` | 已交给消费者、尚未确认 |

## 背压、缓冲与谁持有未发送的消息

- 每个 map/filter/union/sink 节点一个工人线程；source 由调用方线程驱动。
- 普通边 = 容量可调到 1 的有界内存队列；**发送方在队列满时阻塞**（带取消检查）——
  慢 sink 由此让上游受控停下。去掉反馈边后是 DAG，等待图无环，故阻塞发送不会死锁。
- **反馈边发送永不阻塞**：内存缓冲满了就溢写进 SQLite 邮箱（`spill` 表），
  接收方先取内存再取溢写。这保证「所有工人都阻塞在向满队列发送」在反馈环上不会发生。
- 未发送的消息由发送方持有：工人手持（`transforming`，每工人至多 `1 + max_outputs` 条）、
  反馈待发送（`feedback_pending/held→buffered/spilled`）。它们既参与进度计数，
  也参与缓冲限制。
- 内存上界 = Σ边容量 + Σ工人数×(1+输出上限) + sink 数，**不随消息总量增长**；
  超出部分只存在于磁盘溢写邮箱。没有无限 asyncio Task、无界列表或逐轮全量物化。

## SQLite 的角色

每次运行一个 SQLite 文件（默认临时文件，`--db` 可指定），只存本次运行的
**运行时资料**：`spill`（反馈边溢写邮箱）、`progress_events`（轮次/epoch 完成与
失败事件）、`seen_ids`（调用方提供的消息身份查重）、`run_meta`。每次运行开始即
重建表——**这不是跨作业恢复协议**：第一版进程退出则本次运行失败，不做崩溃接手。

## 失败语义

- 可配置 `max_round` 与 `max_derived`（总派生消息数）；超限是**失败**而非正常收敛。
- map 抛错、map 输出超上限、sink 拒收/抛错、调用者 `cancel()`：停止全图，
  释放所有等待者（订阅句柄拿到 `RunFailedError` / `RunCancelledError`），
  不再发出任何 complete。
- sink 已消费的数据不撤销；失败状态与实际计数（每边 sent/received/spilled、
  真实峰值占用、derived、各节点 processed/dropped）在 `stats()` / `RunResult` 中可见。

## 命令

```bash
# 运行示例图（字典 a→b→c；分叉复制保留重数；反馈环）
python3 -m iterflow run examples/normalize.graph.json \
    --send src:0:a --send src:0:z --max-round 16 --db /tmp/run.db

# 独立单线程模拟器给出同一图的可对照预期（不复用正式进度跟踪）
python3 -m iterflow simulate examples/normalize.graph.json \
    --send src:0:a --send src:0:z

# 可复做断言（32 个测试，标准库 unittest）
python3 -m unittest discover -s tests -v
```

进度记录持久化在运行数据库中：

```bash
sqlite3 /tmp/run.db 'SELECT kind, epoch, round FROM progress_events ORDER BY seq'
# round_complete|0|0  →  round_complete|0|1  →  epoch_complete|0|  →  run_finished||
```

## Python API 速览

```python
from iterflow import Graph, Runtime, ManualConsumer

g = Graph("normalize")
g.add_source("src"); g.add_union("u")
g.add_map("m", spec={"kind": "dict_lookup", "dict": {"a": "b", "b": "c"},
                     "default": "self"})
g.add_filter("f_fin", spec={"kind": "not_in", "values": ["a", "b"]})
g.add_filter("f_loop", spec={"kind": "in", "values": ["a", "b"]})
g.add_map("dup", spec={"kind": "duplicate", "count": 2})
g.add_sink("sink")
g.add_edge("src", "u"); g.add_edge("u", "m")
g.add_edge("m", "f_fin"); g.add_edge("m", "f_loop")
g.add_edge("f_fin", "dup"); g.add_edge("dup", "sink")
g.add_feedback_edge("f_loop", "u", capacity=1)

rt = Runtime(g, max_round=16, max_derived=10000, default_capacity=8)
rt.declare_epoch(0)                      # epoch 运行前声明
h0 = rt.subscribe_round(0, 0)            # 轮次完成通知
he = rt.subscribe_epoch(0)
rt.start()
rt.source("src").send(0, "a", msg_id="A")
rt.source("src").close_epoch(0)          # 该 source 的 epoch 0 输入结束
h0.wait(5); he.wait(5)                   # 义务真正解除后才返回
result = rt.wait(5)                      # FINISHED / FAILED / CANCELLED
rt.frontier(0)                           # 未完成集合的最小边界与来源
rt.stats()                               # 每边 sent/received/spilled/峰值、derived
rt.shutdown()
```

测试钩子：`rt.feedback_edge("f_loop->u").gate = threading.Event()` 可把反馈消息
拦在发送前（消息已分配 `(epoch, round+1)` 并计入 `feedback_pending/held`）；
`ManualConsumer` 让测试手工控制 sink 确认时机。

## 目录

```
iterflow/            运行时包（graph / progress / runtime / db / simulator / graphfile / cli）
examples/            normalize.graph.json + 手写预期轨迹 expected_trace.md
tests/               32 个可复做断言（unittest）
```

测试覆盖：手写轨迹逐条断言、屏障停反馈消息、sink 确认暂停、容量 1 分叉汇合反馈环、
反馈溢写、双 source 双 epoch 交错延迟关闭、空 epoch、无输出转换、合法环/非法无反馈环、
轮次超限、派生超限、输出上限、转换抛错、sink 拒收、慢 sink 取消、重复结束声明、
关闭后发送、未声明 epoch、运行中声明 epoch、独立模拟器交叉验证、图文件入口与 CLI。
