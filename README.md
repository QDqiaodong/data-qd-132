# data-qd-132 · 调用链耗时分解与离线占用仓库

Nested trace validation、exclusive interval latency accounting，以及一个可修订、
可撤销、可重复提交的离线 SQLite 服务占用分析仓库。业务输入、计算和当前边界详见
[数据契约](docs/contract.md)。

## 安装与演示

Python >=3.11，运行和测试仅用标准库（含 `sqlite3`）；先确认 `python3 --version`。
每仓建立独立环境，构建工具锁版本。首次安装无公共缓存时可能联网；业务运行不访问网络。

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
.venv/bin/python -m pip check
# 旧的单 JSON 分析入口，结果格式保持不变
.venv/bin/trace-latency --source fixtures/input.json --output run/report.json
# 新的 SQLite 仓库入口
.venv/bin/trace-latency warehouse-apply --db run/warehouse.db --source batch.json --output run/apply.json
.venv/bin/trace-latency warehouse-query --db run/warehouse.db --source query.json --output run/query.json
.venv/bin/python -m unittest discover -s tests -v
```

本地已有wheel时，安装构建工具可加 `--no-index --find-links <公共wheel缓存目录>`。
修改源码后重新安装。本仓不共享其他仓环境，不依赖Docker、外部账号或持续服务。

输入JSON最多8MB，拒绝重复键、非有限数值和未定义字段；源字节SHA256随结果保留。
输出是可独立读取的JSON；成功后原子替换，输入/计算失败退出2并保留旧报告。
输入和输出不得相同，输出符号链接拒绝。测试使用临时目录，运行产物在忽略的run/。

src/trace_latency/model.py 为旧业务模型，warehouse.py 为 SQLite 仓库，相关算法模块
独立组织；common.py 提供受限JSON及报告发布，cli.py 衔接两条数据链路。
fixtures 为明确自制样例，代码与样例采用MIT许可。

## 旧 JSON 分析入口（保持不变）

`trace-latency --source X --output Y` 与 Python API `model.analyze` 完全保留：
重叠子调用按区间并集扣除，样例根耗时20、直接子覆盖13、独占7；所有span累计39并不
等于墙钟时间。旧测试 `tests/test_business.py` 不改。

## 离线仓库：批次修订与服务占用

性能人员不断修正遥测：每批事件以全库连续 `seq` 提交，trace 可在草稿与封存之间
反复修订；只有**已封存** trace 进入占用分析，查询回答“某服务在某时间窗内同时被
多少条 trace 占用”。

### Python API

```python
from trace_latency import apply_batch, query

apply_batch(db_path, {
    "batch_id": "b-001",
    "events": [
        {"seq": 1, "trace_id": "t1", "op": "upsert",
         "span": {"id": "r", "parent": None, "start": 0, "end": 10, "service": "api"}},
        {"seq": 2, "trace_id": "t1", "op": "seal"},
    ],
})
query(db_path, {"service": "api", "start": 0, "end": 20})
```

`apply_batch` 返回 `{batch_id, seq_from, seq_to, events, replayed, max_seq}`；
`query` 返回 `{service, start, end, max_seq, histogram, peak, occupied_ticks,
trace_ticks}`。

### CLI

* `warehouse-apply --db DB --source JSON [--output JSON]`
* `warehouse-query --db DB --source JSON --output JSON`

旧的 `--source/--output` 无子命令用法继续工作。两个仓库命令的成功报告同样保留源字节
SHA256（`source_sha256`），任何校验/计算失败退出 2，旧输出文件原样保留。

### 批次与幂等规则

* 批次严格为 `{batch_id, events}`，`events` 是非空数组，每项含 `seq`、`trace_id`、
  `op`；每批最多 5000 个事件，仍受 8MB、重复 JSON 键、未知字段拒绝规则约束。
* 全库 `seq` 从 1 连续递增；`seq` 与时间整数都用 `type(x) is int` 校验，**布尔值
  一律拒绝**，时间范围 0..10^12。
* `batch_id` 首次提交后：同一解析内容重试（键顺序不影响判等，按规范化 JSON 的
  SHA256 比较）返回原 `seq_from/seq_to`、`replayed=true` 且不写入；内容改变必须
  拒绝。
* `op=upsert` 另带 `span:{id,parent,start,end,service}`（service 沿用 ident 校验），
  替换同 trace 下同 id 的完整 span；`op=delete` 另带 `id`，删除不存在的 id 是错误；
  `op=seal` 不允许其他字段。每条 trace 最多 5000 个 span。
* 新 trace 是草稿：草稿允许父引用暂缺、成环、子区间暂时越界，但每个字段仍须逐项
  合法。`seal` 必须通过仓库现有的单根、完整引用、可达无环、区间包含规则后才转为
  已封存；对已封存 trace 的任何 upsert/delete 会先把它重新打开为草稿（整条贡献
  立即撤销），再次 seal 才重新进入分析。
* 批内任何错误撤回整批：包括更早事件产生的 span 状态、trace 状态、已用 seq 与
  batch_id；重新打开数据库后行为相同。

### 占用区间语义

每个已封存 trace 中，span 的占用区间是自身半开区间 `[start,end)` 减去**直接子
span** 的区间并集——仓库持久化的是片段位置而不是一个 `exclusive_ticks` 标量。
随后把同 trace、同 service 的全部片段取并集：并行/重叠的同服务兄弟 span 只让该
trace 贡献一次，而不是按 span 数计数。跨 trace 才累加并发数。零长度片段不贡献；
半开区间端点相接不会产生瞬时峰值。

例：父 `r[0,10)` 为 api、子 `a[2,8)` 为 db 时，api 只占 `[0,2)` 与 `[8,10)`。

### 查询语义

查询严格为 `{service,start,end}` 且 `start<end`。直方图按并发数递增列出
`{concurrency,ticks}`，只保留正时长桶；窗口包含没有 trace 的空闲时间，因此所有桶
的 ticks 之和恒等于 `end-start`：

* `occupied_ticks` = 非零并发桶时长之和；
* `trace_ticks` = Σ(concurrency × ticks)；
* `peak` = 窗口内最大并发；无该服务时返回全窗零并发桶。

一次查询运行在单个 SQLite 只读快照（`BEGIN`…`COMMIT`）内，`max_seq`、封存集合与
索引来自同一快照，读者不可能看到提交一半的状态。

## 持久化索引布局与复杂度

所有表在同一个 SQLite 文件内，WAL 日志；每个批次的 span 写入、贡献撤销/写入、
前缀聚合、`max_seq` 与幂等行在单个 `BEGIN IMMEDIATE` 事务里共同提交。

| 表 | 作用 |
| --- | --- |
| `meta` | `max_seq` 等单行状态，随批次原子推进 |
| `batches` | `batch_id → seq_from/seq_to/规范化内容SHA`，支撑幂等重试与换内容拒绝 |
| `traces` | trace 状态（draft/sealed）、修订版本号、最后更新 seq |
| `spans` | 每个 trace 的完整 span（id,parent,start,end,service），按 `(trace_id,span_id)` 主键替换 |
| `contrib_endpoints` | 已封存 trace 的可撤销贡献端点 `(service,trace_id,pos,delta)`，delta ∈ {+1,-1}；按 trace 可整体删除，按 `(service,pos)` 可窗口范围扫描 |
| `bit_index` | 稀疏 Fenwick 树（1-based 节点 `node=pos+1`，域 `U=10^12+1`），保存每服务前缀基数聚合 |

查询一个 `(service,[start,end))` 窗口时：

1. 用 Fenwick 前缀和求窗口左端基数 `C(start)`，最多 `ceil(log2 U) ≤ 40` 次
   主键点查；**不假设窗口起点并发为 0**，因此窗口从已开始占用的区间中段切入也
   正确。
2. 只扫描该服务窗内的实际端点
   （`service=? AND pos>=start AND pos<end`，同 tick 用 `GROUP BY pos` 合并），
   设窗内端点数为 K，则读 K 行聚合结果，逐段累计直方图。
3. 读放大为 O(K + log U) 次索引访问，额外内存 O(K + 5000)（5000 为单 trace
   span/片段上限）；不扫描 `spans`、不扫描窗外端点、不逐 tick 展开。

更新一个批次时只重算本批触及的 trace：封存/重封时先按 trace_id 删除旧
`contrib_endpoints` 并对 Fenwick 做反向增量（撤销），再写入新端点与正向增量；
其他 trace 的贡献行与聚合节点从不重建。归零的 Fenwick 节点在提交前清理，保证
稀疏表不随反复修订无限增长。进程在 COMMIT 前终止时事务整体回滚，`batches`
中没有该批次，重试仍只生效一次。

## 业务与样例结果

重叠子调用按区间并集扣除：样例根耗时20、直接子覆盖13、独占7；所有span累计39并不
等于墙钟时间。仓库联动样例：封存 `t1(api:[0,10))`、`t2(api:[5,15))` 后查询
`api[0,20)` 得到 0 并发 5 tick、1 并发 10 tick、2 并发 5 tick。
