# 重复输出列投影修复设计

## 问题

`ReadRowProjector` 为减少隐藏分布键读取中的行对象分配，改为预先按列名计算
`outputIndexes`，再从文本字段直接构造最终 `InternalRow`。

当 `transferSchema` 与 `outputSchema` 完全相同并包含重复列名时，例如
`x, x`，两次 `indexWhere` 都返回第一个 `x`，索引变成 `[0, 0]`。因此输入
`1, 2` 会错误输出为 `1, 1`。改造前的 identity 路径按字段位置解析，不存在
这个问题。

## 采用方案

恢复 identity 场景的位置语义：

- `transferSchema == outputSchema` 时，`outputIndexes` 直接使用
  `outputSchema.fields.indices.toArray`；
- 两个 schema 不同时，保留当前的精确名称优先、大小写不敏感兜底的映射；
- `projectText()` 继续直接调用带索引的 `textToInternalRow()`，不恢复中间
  transfer `InternalRow`。

该方案只修复本次优化引入的回归，不改变隐藏分布键路径的既有行为。

## 未采用方案

### 按同名列出现次数匹配

该方案可以让非 identity schema 中的重复列也逐个匹配，但这属于扩大旧行为：
改造前的非 identity 路径同样使用 `indexWhere`。本次不顺带改变这部分语义。

### 拒绝重复列名

连接器支持把自定义 SQL 作为读取源，现有用户可能依赖重复列标签。直接拒绝会
引入新的兼容性破坏。

## 测试

在 `ReadSchemaPlanTest` 增加 identity 重复列回归：

1. transfer/output schema 都是两个 Long 类型的 `x`；
2. 文本输入为 `11, 22`；
3. 输出第一个字段必须是 `11`，第二个字段必须是 `22`；
4. 现有重排、隐藏分布键、NULL、空输出和字段数错误用例保持不变。

## 成功标准

- identity 重复列按位置保留各自的值；
- 隐藏分布键与重排投影逻辑不变；
- 每个输入文本行仍只创建一个 `SpecificInternalRow`；
- 不修改 COUNT、RMI、写入或流式 offset 路径。
