# 手写预期轨迹：normalize 图（epoch 0）

图：`src -> u -> m -> {f_fin, f_loop}`；`f_fin -> dup -> sink`；`f_loop -feedback-> u`。
字典 `a->b, b->c`；`f_fin` 放行不在 `{a,b}` 中的值（c、z 为已规范）；`f_loop` 放行在 `{a,b}` 中的值。
`dup` 把每个终值复制两份（分叉），`u` 汇合 source 与反馈流。输入：`A=("a")`、`Z=("z")`，均由 source 在 round 0 注入。

## 消息身份与轮次（id 即路径身份）

| 消息 id | epoch | round | 值 | 说明 |
|---|---|---|---|---|
| `A` | 0 | 0 | a | source 注入，经 `src->u->m` |
| `A>m[0]` | 0 | 0 | b | map 输出（广播基底） |
| `A>m[0]~m->f_fin` | 0 | 0 | b | 广播副本，被 f_fin 丢弃 |
| `A>m[0]~m->f_loop` | 0 | 0→1 | b | f_loop 放行，经反馈边 round+1 |
| `A>m[0]~m->f_loop>m[0]` | 0 | 1 | c | 第二轮 map 输出（广播基底） |
| `A>m[0]~m->f_loop>m[0]~m->f_loop` | 0 | 1 | c | 广播副本，被 f_loop 丢弃 |
| `A>m[0]~m->f_loop>m[0]~m->f_fin` | 0 | 1 | c | f_fin 放行 |
| `A>m[0]~m->f_loop>m[0]~m->f_fin>dup[0]` | 0 | 1 | c | dup 第 1 份 → sink |
| `A>m[0]~m->f_loop>m[0]~m->f_fin>dup[1]` | 0 | 1 | c | dup 第 2 份 → sink |
| `Z` | 0 | 0 | z | source 注入 |
| `Z>m[0]` | 0 | 0 | z | map 恒等（不在字典，default=self） |
| `Z>m[0]~m->f_loop` | 0 | 0 | z | 被 f_loop 丢弃 |
| `Z>m[0]~m->f_fin` | 0 | 0 | z | f_fin 放行 |
| `Z>m[0]~m->f_fin>dup[0]` | 0 | 0 | z | dup 第 1 份 → sink |
| `Z>m[0]~m->f_fin>dup[1]` | 0 | 0 | z | dup 第 2 份 → sink |

## sink 结果（袋语义，重数保留）

| id | epoch | round | value |
|---|---|---|---|
| `A>m[0]~m->f_loop>m[0]~m->f_fin>dup[0]` | 0 | 1 | c |
| `A>m[0]~m->f_loop>m[0]~m->f_fin>dup[1]` | 0 | 1 | c |
| `Z>m[0]~m->f_fin>dup[0]` | 0 | 0 | z |
| `Z>m[0]~m->f_fin>dup[1]` | 0 | 0 | z |

## 轮次完成次序（订阅每轮通知）

1. `(0, 0)` round_complete —— 全部 round-0 义务解除（含 z 两支 sink 确认、b 的广播副本一弃一转）且 source 已关闭。
2. `(0, 1)` round_complete —— c 的两份 sink 确认之后。
3. `epoch 0` epoch_complete —— 该 epoch 任意轮次都不可能再产生消息。

## 运行事实（与进度解释一致）

- 每边发送计数：`src->u:2, u->m:3, m->f_fin:3, m->f_loop:3, f_fin->dup:2, f_loop->u:1, dup->sink:4`。
- 派生消息总数 `derived = 16`（非 source 节点放到边上的消息实例数：u:3, m:6, f_fin:2, f_loop:1, dup:4）。
- 所有边 `sent == received`；内存峰值占用 `peak_buffered <= capacity`；反馈边 `f_loop->u` 在本例不溢写（spilled=0）。

以上预期由 `tests/test_runtime.py::ExampleTraceTest` 逐条断言，并由 `tests/test_simulator_and_file.py` 用独立模拟器交叉验证。
