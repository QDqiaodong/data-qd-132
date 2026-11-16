# Local-clock trace accounting v1

面向性能分析人员，输入trace_id和1..5000个span（id,parent,start,end）；parent根为null。
整数tick范围0..10^12，允许零时长；必须且仅一个根、引用完整、全部可由根到达且无环。
子区间必须包含于父区间。假设同一可信单调时钟域；不同主机时钟未校准数据不支持。
每span inclusive=end-start；exclusive=inclusive-直接子span时间区间并集长度。
允许兄弟重叠/嵌套，以并集合并避免重复扣减；不是inclusive减子duration之和。
输出根wall duration与所有span duration之和，后者可能大于wall，exclusive之和也不能当作wall。
样例根20、A8、B8、C3；根子并集13、exclusive7，A exclusive5；总span39。
输出按id排序，与输入顺序无关。该模型估算未被子span覆盖的时间，不推断CPU消耗、阻塞原因、
调用概率、服务关键路径或分布式时钟修复；不采集真实遥测、不提供在线存储。

# Warehouse revision & occupancy contract v2

离线 SQLite 仓库在 v1 校验规则之上增加持久化修订与服务占用分析，仅用 Python 标准库。

## 批次输入

严格为 `{batch_id, events}`；events 为非空数组，每批最多 5000 项。每项含
`seq`、`trace_id`、`op`，且按 op 限定字段：

* `upsert`：另有 `span:{id,parent,start,end,service}`，替换该 trace 下同 id 的完整
  span；id 与 service 走同一 ident 校验（非空、去边空白、<=128 字符、无控制字符）。
* `delete`：另有 `id`；删除不存在的 id 为错误。
* `seal`：无其他字段。

`seq` 与 start/end 必须是 Python int（bool 拒绝），全库 seq 从 1 连续递增，
tick 为 0..10^12 的整数，允许零时长，end 不得小于 start。沿用 8MB 上限、重复
JSON 键拒绝、未知字段拒绝。

## 幂等

`batch_id` 首次提交后记录规范化 JSON（键排序）的 SHA256 与分配到的 seq 区间。
同内容重试返回原序号、`replayed=true` 且不写入；内容变化（哪怕一个字节的语义
差异）必须拒绝。对象键顺序不影响判等。

## trace 生命周期

新 trace 为草稿。草稿允许父引用暂缺、成环、子区间暂时越界，但每个 span 字段仍须
逐项合法；每条 trace 最多 5000 个 span。seal 时复用 v1 的单根、完整引用、可达无环、
区间包含规则，通过后才变为已封存并写入分析索引。对已封存 trace 的任何
upsert/delete 先将其重新打开为草稿并撤销整条 trace 的旧贡献；再次 seal 通过后才
重新写入。批内任意错误整批回滚（span、trace 状态、seq、batch_id、索引全部撤回），
跨进程重开数据库后同样成立。

## 占用与查询

每个已封存 trace 中，span 占用区间 = 自身半开区间 − 直接子 span 区间并集（保留
片段位置）；同 trace 同 service 的片段再取并集，重叠兄弟只计一条 trace。跨 trace
累加并发；零长度片段不贡献，端点相接不产生瞬时峰值。

查询严格为 `{service,start,end}` 且 start<end，返回 service、start、end、max_seq、
histogram（按并发数递增的 {concurrency,ticks}，只列正时长桶）、peak、
occupied_ticks（非零桶时长和）、trace_ticks（Σ concurrency×ticks）。histogram 的
ticks 之和恒为 end-start；无该服务返回全窗零并发。查询取自一个 SQLite 一致读快照，
max_seq、封存集合与索引严格对应。

## 规模与持久化

支持累计 10 万条 trace、百万贡献端点。查询只读取该服务窗内 K 个实际端点，加至多
O(log U)（U=10^12+1，<=40）个 Fenwick 聚合节点取得左端基数，不扫描 span 表、
窗外端点或逐 tick 展开；额外内存 O(K+5000)。更新只重算本批触及 trace 的贡献与
相关索引，不重建其他 trace。批次状态、可撤销贡献、前缀聚合、max_seq 在同一事务
提交；COMMIT 前进程终止则整批回滚，重试仍只生效一次。

# Tail-sampling collection pipeline contract v3

离线尾部采样采集管道把不可变 UTF-8 JSONL 遥测流转成可恢复数据集，仅用 Python
标准库，单进程运行。入口：`trace-latency sample-traces --source INPUT.jsonl
--state STATE_DIR --output OUTPUT_DIR --policy POLICY.json`；任何契约、身份或
冲突违规退出 2。

## policy

严格为 `{normal_budget, slow_ticks}`：分别为整数 0..10000 与 0..10^12，布尔
拒绝；沿用 8MB、重复键、非有限数拒绝。原始字节 SHA256 记入每个水位清单。

## source（JSONL）

每行一个 JSON 对象，最多 1 MB（1 000 000 字节），末行允许没有换行；空行、
重复键、未知字段、非有限数（含 1e999 类溢出）拒绝。记录按 kind 严格限定字段：

* `span`：`{kind,trace_id,span}`；span 严格为 `{id,parent,start,end,service,
  error}`，`error` 为 bool，其余沿用仓库 span 字段规则（ident、tick 0..10^12、
  end>=start、布尔拒绝）。
* `finish`：`{kind,trace_id,root_id,end,span_count}`；声明预期根、根结束 tick
  与 1..5000 个唯一 span。
* `watermark`：`{kind,value}`；value 为严格递增 tick。

span 与 finish 可先后倒置；同一 trace 的同 id span 或 finish 仅解析内容完全
相同（键顺序不影响判等）才算重复并忽略，冲突退出 2。每 trace 最多 5000 span。

## 组装

水位 W 只终结已收到 finish 且 finish.end<=W 的 trace；无 finish 继续等待。
终结时核唯一 span 数、预期根、根 end，并复用 validate_sealed 的单根、完整
引用、可达无环、区间包含语义；失败隔离（quarantine），稳定错误码：
`span_count_mismatch`、`root_count`、`unknown_parent`、`interval_containment`、
`cycle`、`root_mismatch`、`root_end_mismatch`（`empty_trace`、`invalid_tree`
为防御性兜底）。隔离只发生一次；终结后新 span 或不同内容记录退出 2，精确
重复忽略；新 trace 首个 finish 的 end 不大于已发布水位退出 2。EOF 不是隐式
水位：未终结内容持久保留并报告 pending 数，重跑不重复消费。

## 采样

任一 span.error=true 或根耗时>=slow_ticks 的合法 trace 必保留（reason `error`
优先于 `slow`），不占名额；其余按根 service 各选 normal_budget 条，候选按
SHA256(trace_id UTF-8 字节) 摘要字节升序、再按 trace_id 字节序取前 B 条，
输入顺序不影响结果。隔离项不占名额，等待态不提前输出。

## 输出

每个水位产出 `watermark-W/`：`kept.jsonl`（`{trace_id,service,reason,spans}`，
spans 含全部原始字段按 id 字节序）、`dropped.jsonl`
（`{trace_id,service,reason:"budget"}`）、`quarantine.jsonl`
（`{trace_id,reason}`），三类均按 trace_id UTF-8 字节序；`manifest.json` 含
watermark、源文件与 policy 原始字节 SHA256、三个文件的 SHA256 与条数。无终结
trace 的水位也生成空文件与清单。

## 状态与恢复

SQLite 状态记录输入消费偏移、等待内容、重复判等证据（每 span 规范化内容
SHA256 与 finish 声明，永久保留，不得为省空间接受过去冲突）与已发布水位。
重跑先核源文件与 policy 身份，不符退出 2 并保留有效数据集。发布协议：staging
目录写齐四个文件 → 原子 rename 对外可见 → 同一事务推进状态；进程在三个边界
任一处被终止，重跑不重复不漏决策、不跳过未发布水位、不替换已确认有效水位
（已可见目录逐字节核对后保留）。输入错误退出 2，只保留错误前已发布水位。
EOF 时尚未终结内容持久保留并报告 pending 数。

## 规模

支持 100 万条交错等待 trace、每条最多 5000 span、超过内存的单水位候选集；
除处理一条 trace 所需内存外，其余工作内存（含数据库缓存与排序缓冲）最多
64 MiB，不随等待 trace 数或候选数增长；允许临时落盘，不整文件加载，不在
每条输入后遍历全部等待 trace。

# Targeted trace retirement contract v4

离线 trace 定向退役在不改动外部只读 JSON/JSONL 归档的前提下，清除一批
trace_id 在纳管仓库、采样状态与已发布采样数据集中的业务载荷，并阻止原始
遥测重入。仅用 Python 标准库，单进程维护，不要求并发运行或断电恢复。
入口：`trace-latency retire-traces --catalog C --db DB --state STATE_DIR
--output OUTPUT_DIR --source REQUEST.json`；契约违规一律退出 2。这不是
法证级磁盘擦除：允许保留退役 trace_id、请求摘要、必要计数，以及不可分解成
逐 span 证据的既有整批幂等摘要/序号与 source/policy 文件级 SHA；退役完成
后（含维护暂存文件）不得保留任何 span 内容、service、时间或原 span 摘要。

## 请求

严格为 `{request_id, trace_ids}`，走仓库共用的受限 JSON 读取（8MB、重复
键、非有限数拒绝）。`request_id` 走 ident 校验；`trace_ids` 为非空数组、
至多 10 000 个互异 ident，按 UTF-8 字节序规范化后计算请求集合摘要（规范化
JSON 的 SHA256 再取 SHA256），因此同 request_id 同集合的重试与键/数组顺序
无关。

## catalog 与绑定

catalog 管理且只管理一组三个资源：warehouse DB、`STATE_DIR/sampler.db`
与采样输出根目录。首次使用必须确认三者都是已有合法资源：

* warehouse 是含 `meta`/`warehouse_version` 的合法仓库；
* sampler.db 含 source/policy 身份元数据；
* 输出根目录只允许 `watermark-W` 目录（外加 catalog 标记文件），每个目录
  恰含 kept/dropped/quarantine 三个 JSONL 与 manifest.json；manifest 的
  watermark、逐文件 SHA256 与条数必须与磁盘一致；目录最大水位必须等于
  采样状态的 `last_watermark`，无已发布水位时不得存在 watermark 目录。

各资源登记所绑定 catalog 的规范绝对路径：两个 SQLite 文件在 `meta` 写
`retirement_catalog`，输出根写 `.trace-latency-retirement` 标记，catalog
在 `binding.json` 登记这三个规范路径与首次校验时的 watermark/manifest
摘要。禁止交叉绑定、符号链接别名让一个资源加入两个 catalog、把请求源或
catalog 自身当作目标，也禁止 catalog 与目标互相嵌套。首次初始化被中断
可用同一组路径恢复；只绑定部分资源时不能激活退役请求，绑定到不同 catalog
的资源一律拒绝。

## 控制平面

catalog 内的 `retire.db`（WAL）持久登记：

* `requests(request_id, seq, set_sha256, trace_count, status, report_json)`：
  单调请求序号、进行中(active)/完成(done)状态与完成报告；
* `blocked(trace_id)`：累计屏蔽集合，只能增加；
* `progress(request_id, step, status, detail_json)`：每个可重复步骤的
  prepared/done 状态与首次计数/目标摘要。

新请求只在上一请求完成后接收。同 request_id 同集合（顺序无关）重试继续或
返回原序号与同一份报告；同 request_id 换集合拒绝；未知 trace 也进入屏蔽
集合。控制库不保存任何 span、service、时间或 span 摘要列。

## 留存清理

* 仓库：删除命中 trace 的 draft/sealed 行与 span；sealed 先按原
  contrib_endpoints 撤销 Fenwick 贡献并清理归零节点，再删端点行。其他
  trace、原 max_seq 与 `batches` 整批幂等证据原样保留。
* 采样状态：删除命中的等待/终结 trace、其 span、finish 与逐 span 判等
  evidence；source/policy 身份、消费偏移与水位保持不变。已退役记录之后
  只按 trace_id 忽略，不再比较内容、不占名额。
* 已发布数据集：逐个现有 `watermark-W` 从 kept/dropped/quarantine 流式
  移除命中记录，幸存行原始字节与顺序不变，不重新采样、不补回过去丢弃的
  候选；对应 manifest 的逐文件 SHA256 与条数随之更新，source/policy SHA
  与水位保留。无命中目录的全部文件字节不得改变（manifest 也不重写）。

## 纳管入口的控制状态驱动

只有请求已持久激活且屏蔽集合可查询后才开始清理；只要存在未完成请求，
warehouse-apply、warehouse-query 与 sample-traces（含其 Python API）必须
先退出 2，不得读取业务结果、接受新批次、推进消费位置或发布水位。无法靠
省略参数绕过：纳管资源自行根据 `retirement_catalog`/标记定位并校验
catalog；catalog 丢失、绑定不符或标记缺失同样关闭入口。完成后：

* warehouse 批次只要含屏蔽 ID 就整批拒绝，即使 batch_id 是旧成功重试；
  不含屏蔽 ID 的批次继续遵守原幂等规则；
* sampler 对结构合法且 trace_id 已屏蔽的 span/finish 直接跳过，允许其
  内容与过去不同（不报历史冲突、不建 trace、不占名额），消费偏移照常
  推进，其他记录继续遵守旧规则。

未纳管资源（无 catalog 标记）的既有契约完全不变。

## 跨资源诚实性与恢复

跨 SQLite 文件与目录不要求物理同时提交，但成功与恢复必须诚实：先持久
接受请求（序号、摘要、active、屏蔽集合），再以可重复、可核验的步骤清理，
全部完成后才写完成标记并重新开放入口。每个破坏性步骤先把**首次运行**
计数与目标文件摘要写入 progress(prepared) 再动手；仓库/采样按 500 行
分块在独立事务提交，输出目录逐文件先暂存后原子替换、manifest 最后替换。
在任意目标事务提交后、单个目录文件替换后或完成标记前终止，重试继续同一
请求：首次计数不因部分已删除而少报，也不会从原始归档恢复已退役载荷；
部分替换的 watermark 树依据持久 prepared 详情修复，绝不整目录删除重建，
也不靠捕获异常后无条件完成。暂存目录 `staging-<request_id>` 仅在请求
进行中存在，完成后清除。

## 完成报告

`{request_id, seq, status:"done", trace_count, warehouse_traces,
sampler_waiting, published_kept, published_dropped,
published_quarantined}`，分别为请求 trace 数、实际清除的仓库 trace 数、
采样等待 trace 数与已发布三类文件各自移除的记录数。完成后重试报告计数
不变。

## 规模与内存

支持 10 万条仓库 trace、100 万条采样等待 trace、超过内存的输出总量与每条
至多 5000 span。请求集合至多 10 000 个 id 全量驻留；除此之外所有扫描按
分块/逐行流式进行，SQLite 页缓存受限并允许临时落盘，任一时刻只物化一条
trace 或一行 JSONL，工作内存（含数据库缓存）上限 64 MiB，不随目标总数或
输出总量增长。

# Revision-aware persistent SLO alerting contract v5

在已有离线仓库之上增加按根服务的持续延迟违约（SLO）告警管道与离线通知投递
队列。仅用 Python 标准库，与仓库共享同一个 SQLite 文件与同一套写事务。
采样选择不参与本业务，占用直方图不能替代请求慢率。入口：`slo-init`、
`slo-status`、`slo-claim`、`slo-ack`，均输出严格 JSON；任何契约违规退出 2。
SLO 与 retirement catalog 互斥：`slo-init` 拒绝已绑定 catalog 的仓库，
`retire-traces` 初始化拒绝已启用 SLO 的仓库，且两类互斥检查都发生在任何
绑定或清理写之前。

## 配置与初始化

CONFIG 严格为 `{capacity,policies}`。`capacity` 为 1..10000 的未确认通知
容量（整数，bool 拒绝）；`policies` 为 1..100 个不同 service 的策略，每项
严格为 `{service,slow_ticks,short_n,long_n,open_ppm,close_ppm}`：时间整数
0..10^12，`1<=short_n<=long_n<=10^6`，`0<=close_ppm<open_ppm<=10^6`，所有
整数拒绝 bool，未知字段/重复 JSON 键拒绝。配置按解析内容计算规范化 SHA256：
初始化后不可更改，同解析内容重试幂等（`replayed=true`，不写入），不同内容
退出 2。可在已有未纳管（无 catalog）仓库上初始化，以其当前封存集合单次
流式扫描计算初态；初始化失败整体回滚，不留下 enabled 标记、策略行或部分
通知。enabled 标记是启用事务内最后一个写入。

## 请求事实与两窗索引

每条 sealed trace 只由其唯一根 span 提供一个请求事实：service=根服务、
完成时间=根 end、慢请求当且仅当根 `(end-start)>=slow_ticks`。draft 不贡献；
旧 trace 重开、改根时间/服务、再次 seal 必须撤销旧事实并按正确位置补入。
事实按 `(end 升序, trace_id 的 UTF-8 字节序升序)` 全序排列，每服务一棵
持久 AVL 顺序统计树（节点缓存子树大小与慢数），支持旧事实删除、同 tick
确定次序与边界补位——不是只追加队列。各服务最后 `short_n`、`long_n` 条
事实分别形成短窗/长窗，保留恰好 N 条而非整个同 tick 组；不足 N 时用现有
全部请求。只改子 span 的 service/耗时时根事实归属与慢率不变。窗口慢数是
O(log M) 的顺序统计查询（秩定位 + 前缀/后缀子树聚合），不按 N 扫描窗口、
不扫描其他 trace 的 span、不全量重建服务请求集合；每触及 trace 的事实秩
维护与两窗取值为 O(log M) 最坏。

## 告警状态机

每服务初始 healthy。长窗已满且短/长两窗慢率均 `>=open_ppm` 时转 firing；
firing 时任一窗慢率 `<=close_ppm` 或长窗不足即转 healthy，其余保留原状态。
所有比例用整数交叉相乘精确比较（不引入浮点）。每次状态翻转恰好追加一条
通知，初始化也按同一规则产生初始 open。通知严格含 `event_id、service、
transition(open/close)、max_seq、short_bad、short_total、long_bad、long_total`，
保存翻转瞬间的值，之后任何修订都不改变已保存通知。`event_id` 全库从 1
连续分配，失败事务不占号。

## 容量反向约束

只有“当前未确认通知数 + 本批最终所需新通知数 <= capacity”时，warehouse
批次才可在同一事务共同提交请求事实、占用索引、max_seq、批次身份、告警状态
与全部通知；否则整批退出 2 并保留所有旧状态（span、贡献、窗口、max_seq、
batch_id 全部回滚）。已确认通知不再占容量但永久保留为幂等证据，不得覆盖
未确认通知、吞掉翻转或提前推进数据。无状态翻转的合法批次在容量已满时仍可
提交；普通 warehouse 入口与 Python API 无法靠省略参数绕过。一个批次内含
两个服务翻转但只剩一个槽位时，两个都不提交。

## 投递：claim / ack

claim 请求严格为 `{consumer,now,lease_ticks,limit}`：`now` 为 0..10^12
整数，`lease_ticks` 为 1..10^6 且 `now+lease_ticks<=10^12`，`limit` 为
1..100。ack 严格为 `{consumer,now,items}`，items 是 1..100 个互异的
`{event_id,token}`，token 为领取时返回的非空字符串。两操作的 `now` 在全库
单调不倒退，同 tick 允许；契约错误不推进时钟。claim 按 event_id 选择尚未
确认且未被租用（或租期已到）的通知，返回原通知、新 token 与 `expires_at`；
每次重新领取必须换 token，空结果也推进时钟。仅当 `now<expires_at` 时才
允许持有该 token 的 consumer 确认；过期、被重新领取的旧 token、consumer
不符或未知 event_id 使整次 ack 失败（多项中任一非法则任何项都不确认）。
已成功确认的相同 consumer/token 再次确认幂等（即使此后过期），其他 token
不能确认已确认项。确认释放容量并保留幂等证据，领取不释放容量。claim/ack
各自原子提交，进程在 COMMIT 前后终止重开后既无已入库却丢通知，也无未入库
幽灵通知。

## 状态快照

`slo-status` 在单个只读快照返回 `max_seq`、按 service UTF-8 字节序排列的
当前计数与 healthy/firing 状态，以及未确认通知总数；快照内各值严格对应。

## 空间与恢复代价

两窗索引为每服务每事实一行 AVL 节点（含父/左右孩子、高度、子树大小、慢数、
end、trace 字节与慢标志），空间 O(M)；节点 id 每服务单调不复用。通知行
永久保留（含已确认），lease 与 ack 幂等证据各一行，空间分别随未投递/已
确认通知数线性增长；容量只约束未确认部分。所有写入与 warehouse 批次同
事务，COMMIT 前终止整体回滚，重开后重试精确幂等；SQLite 页缓存受限
（16 MiB）、临时排序落盘，SLO 工作内存（含缓存）最多 32 MiB，不随 M 或 N
增长，支持每服务百万封存根请求与 100 个策略。
