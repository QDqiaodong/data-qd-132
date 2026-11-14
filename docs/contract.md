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
