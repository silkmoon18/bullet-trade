# 聚宽实盘策略账本改造文档

本目录是 `feat/joinquant-live-ledger` 分支的工程事实来源。实现顺序固定为：先更新文档，再实施一个 slice，随后测试和代码审查；评审通过后才进入下一个 slice。

## 文档索引

1. [当前架构、依赖与状态](00-current-state.md)
2. [完整修改计划](01-modification-plan.md)
3. [架构与工程决策](02-decisions.md)
4. [当前 session](03-session.md)
5. [实施 slices](04-slices.md)
6. [bt_quant迁移清单](05-migration-manifest.md)
7. [聚宽本地开发与兼容矩阵](05-joinquant-development.md)
8. [聚宽校验与导出](06-joinquant-export.md)
9. [当前状态与使用指南](07-status-and-usage.md)
10. [StrategyLedger领域模型与SQLite Schema](08-strategy-ledger-schema.md)
11. [StrategyLedger事务Repository](09-transactional-repository.md)
12. [QMT券商能力合同](10-broker-capability-contract.md)
13. [持久幂等与Outbox](11-persistent-idempotency-outbox.md)
14. [真实现金校准与策略资金](12-capital-allocation.md)
15. [真实成交入账与持仓批次](13-fill-booking.md)
16. [原子估值与组合快照](14-valuation-snapshot.md)
17. [个人量化精简计划](15-lean-personal-plan.md)
18. [飞书交易卡片通知](16-feishu-trade-notifications.md)
19. [QMT同步与账实对账](17-qmt-reconciliation.md)
20. [目标规划与执行](18-target-planner-executor.md)
21. [策略API与聚宽真实组合视图](19-strategy-api-joinquant-view.md)
22. [本机部署、备份与小额验收](20-local-deployment-runbook.md)
23. [通用执行类型计划](21-generic-execution-plan.md)
24. [固定价格、价格笼子与T+0修改记录](23-fixed-limit-review.md)
25. [API19 JQ分阶段调仓历史记录（已撤回）](28-jq-rebalance-continuation.md)
26. [API20恢复JQ原生执行](29-restore-jq-native-execution.md)

`archive/`：历史审查记录归档（S01逐轮REWORK与S01至S03逐轮冻结明细）；其中各轮候选结论均已失效，仅作历史记录，不作为放行证据。

## 权威边界

- QMT/券商：物理账户、委托和成交事实来源。
- BulletTrade `StrategyLedger`：策略级现金、冻结、持仓归属、净值与绩效的权威账本。
- 聚宽：数据、信号、调度与真实指标展示；原生模拟账本不是实盘权威来源。
