# data-qd-132 · 调用链耗时分解与离线占用仓库

Nested trace validation、exclusive interval latency accounting，以及一个可修订、
可撤销、可重复提交的离线 SQLite 服务占用分析仓库；另有把乱序遥测 JSONL 转成
可恢复尾部采样数据集的离线采集管道。业务输入、计算和当前边界详见
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
# 乱序遥测 JSONL 的尾部采样采集管道
.venv/bin/trace-latency sample-traces --source fixtures/traces.jsonl \
    --state run/sample-state --output run/sample-out --policy fixtures/policy.json
# 定向退役：catalog 绑定一个仓库 + 一个采样状态/输出资源组
.venv/bin/trace-latency retire-traces --catalog run/retire-catalog \
    --db run/warehouse.db --state run/sample-state --output run/sample-out \
    --source run/retire-request.json
# 持久 SLO 告警管道：初始化、快照、离线通知领取与确认
.venv/bin/trace-latency slo-init --db run/warehouse.db --source run/slo-config.json
.venv/bin/trace-latency slo-status --db run/warehouse.db
.venv/bin/trace-latency slo-claim --db run/warehouse.db --source run/claim.json
.venv/bin/trace-latency slo-ack --db run/warehouse.db --source run/ack.json
.venv/bin/python -m unittest discover -s tests -v
```

本地已有wheel时，安装构建工具可加 `--no-index --find-links <公共wheel缓存目录>`。
修改源码后重新安装。本仓不共享其他仓环境，不依赖Docker、外部账号或持续服务。

输入JSON最多8MB，拒绝重复键、非有限数值和未定义字段；源字节SHA256随结果保留。
输出是可独立读取的JSON；成功后原子替换，输入/计算失败退出2并保留旧报告。
输入和输出不得相同，输出符号链接拒绝。测试使用临时目录，运行产物在忽略的run/。

src/trace_latency/model.py 为旧业务模型，warehouse.py 为 SQLite 仓库，sampler.py 为
尾部采样采集管道，相关算法模块独立组织；common.py 提供受限JSON及报告发布，cli.py
衔接三条数据链路。fixtures 为明确自制样例，代码与样例采用MIT许可。

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

## 离线尾部采样采集管道

`sample-traces --source INPUT.jsonl --state STATE_DIR --output OUTPUT_DIR --policy
POLICY.json` 把乱序到达的遥测 JSONL 转成可恢复的数据集：整条调用链完成后做尾部
采样，错误/慢请求必保留，普通请求按根服务限制存储名额。仅用 Python 标准库
（含 `sqlite3`），单进程运行，不要求并发写或断电恢复。Python API 为
`trace_latency.sample_traces(source, state_dir, output_dir, policy)`，返回与 CLI
stdout 相同的报告（`pending`、`watermarks_published`、`last_watermark`、
`kept/dropped/quarantined` 等）。

### 输入契约

* policy 严格为 `{normal_budget, slow_ticks}`：`normal_budget` 整数 0..10000，
  `slow_ticks` 整数 0..10^12，布尔一律拒绝；走仓库共用的受限 JSON 读取（8MB、
  重复键、非有限数拒绝），原始字节 SHA256 记入每个水位清单。
* source 是不可变 UTF-8 JSONL：每行一个 JSON 对象、最多 1 MB（1 000 000 字节），
  末行允许没有换行；空行、重复 JSON 键、未知字段、非有限数（NaN/Infinity/溢出）
  一律退出 2。
* 记录严格按 kind 限定字段：
  * `{"kind":"span","trace_id":…,"span":{id,parent,start,end,service,error}}`；
    `error` 为 bool，其余字段沿用仓库 span 规则（ident、tick 0..10^12 整数、
    end>=start、布尔拒绝）。
  * `{"kind":"finish","trace_id":…,"root_id":…,"end":…,"span_count":…}`：声明预期
    根、根结束 tick 与 1..5000 个唯一 span。
  * `{"kind":"watermark","value":…}`：value 为严格递增的 tick。
* span 与 finish 可任意先后到达；同一 trace 的同 id span 或 finish 只有解析内容
  完全相同（键顺序不影响判等）才算重复并忽略，内容冲突退出 2。每条 trace 最多
  5000 个 span，超出即输入错误。

### 组装语义

水位 W 只终结“已收到 finish 且 finish.end<=W”的 trace；没有 finish 的继续等待。
终结时核对唯一 span 数、预期根与根 end，并复用仓库 `validate_sealed` 的单根、
完整引用、可达无环、区间包含语义；失败进 quarantine 而非参与采样，稳定错误码：
`span_count_mismatch`、`root_count`、`unknown_parent`、`interval_containment`、
`cycle`、`root_mismatch`、`root_end_mismatch`（`empty_trace`/`invalid_tree` 为
防御性兜底）。隔离只发生一次，后续补齐不能倒改已发布结果：终结后新 span 或不同
内容的记录退出 2，精确重复仍忽略；新 trace 首个 finish 的 end 不大于已发布水位
也退出 2（禁止追溯补入）。EOF 不是隐式水位：未终结内容持久保留并报告 pending
数，同一文件重跑不会重复消费。

### 采样语义

每个水位内，合法 trace 任一 span.error=true 或根耗时>=slow_ticks 必保留（reason
`error` 优先于 `slow`），不占普通名额；其余按根 service 各选 normal_budget 条。
普通候选按 SHA256(trace_id 的 UTF-8 字节) 完整摘要字节升序、再按 trace_id 字节序
取前 B 条，同水位段内输入顺序不影响结果。只有组装在水位完成完整树校验并给出根
service、根耗时与错误状态后，采样器才决定候选所属服务与是否占预算；隔离项不占
名额，等待态不提前输出。

### 输出布局

每个水位产出 `watermark-W/` 目录：

* `kept.jsonl`：`{trace_id,service,reason,spans}`，reason ∈ error/slow/normal，
  spans 保留所有原始 span 字段并按 id 字节序排列；
* `dropped.jsonl`：`{trace_id,service,reason:"budget"}`；
* `quarantine.jsonl`：`{trace_id,reason}`，reason 为上述稳定错误码；
* `manifest.json`：`{watermark, source_sha256, policy_sha256, files}`，files 含
  三个文件的 SHA256 与条数。

三类 JSONL 均按 trace_id 的 UTF-8 字节序排列；即使没有终结 trace 的水位也生成
空文件与清单。所有行是紧凑 UTF-8 JSON（键排序），数据集可字节级比对。

### 状态与崩溃恢复

state 目录内是 SQLite（WAL）：`meta`（源/policy 身份 SHA256、消费字节偏移、已
发布水位）、`traces`（waiting/finalized 与 finish 声明）、`spans`（等待内容）、
`evidence`（已终结 trace 每 span 的规范化内容 SHA256）。重跑先流式核对源文件与
policy 身份，不符退出 2 且保留有效数据集；随后从已提交偏移继续，不重复消费。

发布与消费进度按下述协议协调：每个水位先把四个文件写进 `.staging-W`，再原子
rename 为 `watermark-W`，最后在同一个 SQLite 事务里推进偏移/水位并清理已终结
等待内容。进程在“文件全部写好 / 目录对外可见 / 状态推进”三个边界任一处被终止，
重跑都不重复、不漏掉 trace 决策：staging 残留先清理；已可见但未提交的水位目录
逐字节核对后**保留原样**（绝不替换其他已确认有效水位），状态补齐；已提交的水位
直接跳过，未发布水位不会被跳过。输入错误退出 2：只保留错误前已发布的水位，
未发布部分不留可见结果。测试用故障注入钩子
`TRACE_LATENCY_SAMPLE_CRASH=files|rename|commit:N`（N 为本次运行第 N 个水位）
在三个边界杀进程，仅测试使用。

### 资源代价（组装索引、预算选择、状态清理）

* 组装索引：每条记录只做 O(log n) 次索引访问——traces 主键定位、spans 主键
  (trace_id,span_id) 判重、finish 主键更新；终结扫描用部分索引
  `(finish_end) WHERE status='waiting' AND finish_end IS NOT NULL`，只触及本水位
  可终结的行，绝不在每条输入后遍历全部等待 trace。等待内容全部落 SQLite，
  主/临时页缓存各限 8 MiB。
* 预算选择：候选写入临时表，`ORDER BY service,digest,trace_id` 由 SQLite 外排序
  （temp_store=FILE 落盘），逐行流式判定名额；输出再按 trace_id 外排序写出。
  单水位候选集可以超过内存，排序缓冲不随候选数增长。
* 已完成状态清理：终结在同一事务里把 trace 置为 finalized、删除其等待 spans、
  并把每 span 的规范化内容 SHA256 写入 evidence、finish 声明留在 traces——这是
  重放时区分“精确重复（忽略）”与“冲突/新增（退出 2）”的必要证据，用磁盘换取
  绝不为省空间接受过去冲突。
* 除处理一条 trace（≤5000 span）所需内存外，其余工作内存（含数据库缓存与排序
  缓冲）最多 64 MiB，不随等待 trace 数或本次候选数增长：支持 100 万条交错等待
  trace、每条最多 5000 span、超过内存的单水位候选集。测试以 ru_maxrss 增量
  验证该上界。

## 离线 trace 定向退役（retire-traces）

运维人员收到一批 trace_id 后，`retire-traces` 清除其在纳管仓库、采样状态
和已发布采样数据集中的业务载荷，并阻止原始遥测重入。外部只读
JSON/JSONL 归档永不改写；这不是法证级磁盘擦除，只保留退役 trace_id、
请求摘要、必要计数，以及整批幂等摘要/序号和 source/policy 文件级 SHA。
仅用 Python 标准库，单进程维护。Python API 为
`trace_latency.retire_traces(catalog, db, state_dir, output_dir, source)`。

### catalog 绑定与入口管控

catalog 管理且只管理一组三个资源：warehouse DB、`STATE/sampler.db` 与
采样输出根。首次使用校验三者确为已有合法资源（清单与文件摘要/条数一致、
输出最大水位与采样 `last_watermark` 一致、无已发布水位时没有 watermark
目录），再把 catalog 的规范绝对路径登记进两个 SQLite 的
`meta.retirement_catalog` 和输出根的 `.trace-latency-retirement` 标记，
catalog 的 `binding.json` 记录这三个路径。禁止交叉绑定、别名让一个资源
加入两个 catalog、把源文件或 catalog 当目标；初始化中断可用同组路径恢复，
部分绑定时不会激活退役请求。

### 控制平面与清理

`retire.db` 持久登记 request_id、按 UTF-8 字节序规范化的请求集合摘要、
单调序号、active/done 状态、只增不减的累计屏蔽集合和每步骤的首次计数。
同 request_id 同集合（顺序无关）重试继续或返回原序号，换集合拒绝，未知
trace 也进入屏蔽集合；新请求只在上一请求完成后接收。清理职责：

* 仓库删除命中 trace 的 draft/sealed 行与 span，sealed 撤销原
  contrib_endpoints 与 Fenwick 贡献并清理归零节点，保留其他 trace、
  原 max_seq 与 `batches` 幂等证据；
* 采样状态删除命中的等待/终结 trace、span、finish 与逐 span 判等证据，
  保持 source/policy 身份、消费偏移和水位；
* 逐个 watermark 从 kept/dropped/quarantine 流式移除命中记录，幸存行
  字节与顺序不变，更新 manifest 摘要和条数，保留 source/policy SHA 与
  水位；无命中目录的全部文件字节不变。

### 入口驱动、诚实恢复与规模

只要存在未完成请求，warehouse-apply、warehouse-query、sample-traces
（含 Python API）一律先退出 2，不读业务结果、不接新批次、不推进消费位置
或水位；省略参数无法绕过，catalog 丢失/绑定不符同样关闭入口。完成后
warehouse 批次只要含屏蔽 ID 就整批拒绝（旧 batch_id 重试也不例外），
sampler 对屏蔽 ID 的合法 span/finish 直接跳过（允许内容不同、不冲突、
不占名额）但照常推进消费，未纳管资源的旧契约不变。

请求先持久激活再清理，全部完成才标记完成；每步骤先持久化首次计数再动手，
仓库/采样按块事务提交、输出逐文件原子替换且 manifest 最后替换，任意边界
被终止后跨进程重试都继续同一请求、首次计数不少报、不从归档恢复载荷。
工作内存上限 64 MiB：请求集合至多 1 万 id 全量驻留，其余全部流式，支持
10 万仓库 trace、100 万采样等待 trace 与超过内存的输出。测试钩子
`TRACE_LATENCY_RETIRE_CRASH=accept|warehouse_commit|sampler_commit|
wmbefore:W|wmafterfiles:W|wmdone:W|complete` 仅测试使用，在对应边界杀进程，
由 `tests/test_retirement_crash.py` 用子进程故障注入覆盖恢复与阻断。

## 可修订遥测上的持久 SLO 告警管道（slo-init / slo-status / slo-claim / slo-ack）

SLO 管道在已有离线仓库上按**根服务最近完成的请求**判断延迟是否持续违约，
并让离线通知消费者以租约 + 确认的方式可靠领取告警。它与仓库共享同一个
SQLite 文件和同一套写事务；占用直方图不参与本业务（请求慢率来自根 span，
不是并发占用），尾部采样也不参与。Python API 与 CLI 同名：

```python
from trace_latency import slo_init, slo_status, slo_claim, slo_ack

slo_init(db_path, {
    "capacity": 1000,
    "policies": [
        {"service": "api", "slow_ticks": 100, "short_n": 100, "long_n": 1000,
         "open_ppm": 500000, "close_ppm": 100000},
    ],
})
slo_status(db_path)
# {'max_seq':..., 'unacked':n, 'services':[{'service','state','requests'}, ...]}
claim = slo_claim(db_path, {"consumer": "oncall-a", "now": 100,
                            "lease_ticks": 60, "limit": 50})
# {'consumer','now','returned', 'items':[{'event_id','notification',
#   'token','expires_at'}, ...]}
slo_ack(db_path, {"consumer": "oncall-a", "now": 120, "items": [
    {"event_id": claim["items"][0]["event_id"],
     "token": claim["items"][0]["token"]}]})
# {'consumer','now','acked','newly_acked'}
```

### CLI 与严格 JSON 规则

* `slo-init --db DB --source CONFIG`、`slo-claim --db DB --source REQUEST`、
  `slo-ack --db DB --source REQUEST` 读取 8MB 上限的受限 JSON（重复键、未知
  字段、非有限数拒绝，布尔不冒充整数），成功报告带 `source_sha256`；
  `slo-status --db DB` 无输入文件。四个命令均向 stdout 打印严格 JSON，任何
  校验/计算错误退出 2。
* CONFIG 严格为 `{capacity,policies}`：`capacity` 是 1..10000 的未确认通知
  容量；`policies` 是 1..100 个不同 service 的
  `{service,slow_ticks,short_n,long_n,open_ppm,close_ppm}`，满足
  `1<=short_n<=long_n<=10^6`、`0<=close_ppm<open_ppm<=10^6`、时间
  0..10^12，所有整数 `type(x) is int`（bool 拒绝）。
* 配置初始化后不可更改：同解析内容（键序无关，按规范化 JSON SHA256 比较）
  重试返回 `replayed=true` 且不写入；不同内容退出 2。可在已有未纳管仓库上
  初始化，以当前封存集合单次流式扫描建立初态；初始化失败不留下 enabled
  标记或部分通知。SLO 与 retirement catalog 互斥，互斥检查先于任何绑定/清理。

### 请求窗口与告警语义

每条 sealed trace 只由唯一根 span 贡献一个请求事实（service=根服务、
完成时间=根 end、慢当且仅当根 `end-start>=slow_ticks`），按
`(end 升序, trace_id UTF-8 字节序升序)` 全序排列。每服务最后 `short_n` /
`long_n` 条事实分别构成短窗/长窗，**保留恰好 N 条**而非整个同 tick 组，
不足 N 用全部现有请求；draft 不贡献，重开/改根/再封按旧事实撤销、新事实
补位处理，只改子 span 不影响根请求归属与慢率。状态机：每服务初始
healthy，长窗满且两窗慢率均 `>=open_ppm` 转 firing；firing 时任一窗慢率
`<=close_ppm` 或长窗不足转 healthy；比例一律整数交叉相乘。每次翻转恰好
一条不可变通知（含翻转瞬间的 `max_seq` 与两窗 bad/total），`event_id`
全库从 1 连续分配，失败不占号。

### 容量反向约束与投递

warehouse 批次只有在“当前未确认数 + 本批最终新通知数 <= capacity”时才与
请求事实、占用索引、max_seq、batch_id、告警状态、通知在**同一事务**提交，
否则整批退出 2、全部旧状态保留；无翻转批次在容量满时仍可提交，普通入口与
Python API 无法靠省略新参数绕过。claim 按 event_id 领取未确认且未租用
（或租约到期）的通知，每次重新领取换发新 token，空结果也推进全库单调时钟；
只有 `now<expires_at` 且 consumer/token 匹配才能 ack，过期/旧 token/消费者
不符/未知 ID 使整次 ack 失败，多项 ack 全有或全无；相同 consumer/token 重复
ack 幂等（即使已过期），确认释放容量但通知与幂等证据永久保留。

### 两窗索引、通知保留与恢复代价

* 两窗索引：每服务一棵持久 **AVL 顺序统计树**（表 `slo_requests`），每事实
  一行，节点缓存子树大小与慢数；事实修订是“删旧节点 + 插新节点”的
  O(log M) 操作，同 tick 次序由 `(end, trace_id UTF-8 字节)` 确定，支持
  删除与边界补位，**不是只追加队列**。窗口慢数由秩定位 + 前缀/后缀子树聚合
  在 O(log M) 次索引访问得到，不按 N 扫描窗口、不扫其他 trace 的 span、不
  全量重建服务集合；空间 O(M)。
* 通知保留：`slo_notifications` 永久保存全部 open/close（已确认也不删），
  `slo_leases` 保存当前租约，`slo_ack_evidence` 永久保存确认用的
  consumer/token 作为幂等证据；空间随历史通知数线性增长，容量只约束未确认
  部分。节点 id 每服务单调递增不复用。
* 恢复：所有 SLO 写都在 warehouse 的 `BEGIN IMMEDIATE` 事务内，DDL 也参与
  事务；COMMIT 前进程终止则整体回滚（无已入库丢通知，也无幽灵通知），重开
  后精确批次重试只生效一次。测试钩子
  `TRACE_LATENCY_SLO_CRASH=init_commit|batch_commit|claim_commit|ack_commit`
  仅测试使用，在对应 COMMIT 前杀进程，由 `tests/test_slo_crash.py` 子进程
  故障注入覆盖。SQLite 页缓存限 16 MiB、临时排序落盘，SLO 工作内存（含缓存）
  最多 32 MiB，不随 M 或 N 增长，支持每服务百万封存根请求与 100 个策略。

## 业务与样例结果

重叠子调用按区间并集扣除：样例根耗时20、直接子覆盖13、独占7；所有span累计39并不
等于墙钟时间。仓库联动样例：封存 `t1(api:[0,10))`、`t2(api:[5,15))` 后查询
`api[0,20)` 得到 0 并发 5 tick、1 并发 10 tick、2 并发 5 tick。
