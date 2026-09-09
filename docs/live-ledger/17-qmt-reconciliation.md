# QMT同步与账实对账

`SQLiteReconciliationService`每轮接收一次券商账户、持仓、订单和成交快照。QMT/券商是订单与成交事实来源；StrategyLedger仍是策略资金、持仓归属和指标的本地权威账本。

## 流程

1. 采集券商可用资金、持仓、订单和成交。服务同时提供同步broker和异步server adapter采集函数。
2. 交易开关开启时校验适配器已经通过`strategy_ledger_v1`能力探针；未证明稳定订单号、成交号、费用、状态和lookback时直接BLOCKED。交易关闭时允许只读账本完成对账，但不能派发订单。
3. 优先用broker order ID映射本地策略订单；提交响应丢失时，允许用券商原样返回的完整`client_tag`备注认领唯一的`SUBMIT_UNKNOWN`订单。无法匹配本策略订单标记的委托和成交视为共享账户中的外部活动，只统计忽略数量，不写入策略账本。
4. 已知真实成交按现有FillBooking事务入账；重复成交自动no-op，买入lot按T+1处理。
5. 处理撤单/拒单并释放对应冻结资金。
6. 检查券商可用资金是否覆盖全部策略已分配可用资金；按证券检查券商总持仓是否覆盖策略归属持仓。总量覆盖但可卖不足只记录`broker_sellable_limits`和`deferred_broker_sellable_shortages`，不阻断整账户；API快照和卖出规划取策略/QMT可卖的较小值，派发前再次检查，待卖时仍先卖后买。券商额外现金、额外持仓以及无关的负数持仓不参与策略净值。详见[可卖容量修复](30-sellable-capacity.md)。
7. 写入`reconciliation_runs`并返回READY/BLOCKED及差异摘要。

## 最小调用

```python
snapshot = collect_broker_snapshot(qmt_broker)
result = reconciliation.synchronize(
    account_id="good-etf",
    physical_account_id="qmt-main",
    snapshot=snapshot,
)
if result.state is not ReconciliationState.READY:
    # 保持只读，不创建新订单
    log.error("账实对账阻断: %s", result.details["blockers"])
```

BulletTrade server内部使用`collect_async_broker_snapshot(adapter, account_context)`采集相同结构，并兼容MiniQMT adapter的`{"dtype":"dict","value":...}`账户包装。

READY只表示本轮策略归属资产仍由券商物理账户覆盖，且本策略已知订单和成交已经入账。共享账户中其他策略或人工交易不会自动归属于本策略；如果外部卖出或占用导致物理现金/持仓不足以覆盖策略账本，状态仍会变为`BLOCKED`。它只会解除此前的`RECONCILIATION_BLOCKED`，不会清除人工`TRADING_BLOCKED`、CLOSED或其它kill switch。当前MiniQMT/BigQMT预置能力仍为`PROBE_REQUIRED`，真实环境探针通过前不会放行下单。
