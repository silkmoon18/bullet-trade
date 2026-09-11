# 当前状态与使用指南

2026-09-12：合入上游 `0.10.0b3`，新增可选完整 QMT 回调桥接。现有聚宽策略/helper 不变；详见[使用与迁移](../big-qmt-bridge.md)。新桥接已完成离线测试，尚未在客户端部署验证；下文关于 BigQMT 回调未实现的描述仅指旧 HTTP gateway。

> **结论：L01至L05的仓库代码已实现，但当前分支仍不能直接用于真实资金。** 真实QMT能力证明、聚宽JQ/QMT模拟和小额人工验收只能在用户目标环境完成；服务端交易开关默认关闭。

## 1. 现在已经具备什么

- `bt_quant`的有效策略内容已迁入统一BulletTrade仓库，旧仓库保留为迁移来源。
- 本地可通过`jqdata.pyi`、helper类型声明和严格mypy/pyright配置获得代码提示及静态检查。
- `good_etf.py`是仓库内唯一受控策略源码；导出器原样复制策略、helper和示例profile，并生成确定性manifest。
- 私有profile只读校验不会执行、复制、hash或输出其中的host/token等值。
- BACKTEST使用聚宽历史撮合；模拟交易可独立启用JQ、QMT或两者。两者同时启用时共享选股决策，但资金、持仓、成本和风控分别计算。
- 已具备真实券商可用现金校准、初始1万元单次分配、按订单冻结/释放和显式增减资的事务服务。
- 已具备按真实买卖成交更新现金、订单冻结、持仓lot、费用和已实现盈亏的事务服务，重复fill不会重复入账；费用字段缺失时保留`UNKNOWN`，不伪造为0。
- 已具备基于新鲜行情的现金、持仓市值、总资产、NAV、费用和盈亏原子快照，可作为后续聚宽组合视图数据源。
- MiniQMT包装账户快照已统一解包；提交响应未知的订单可按完整`client_tag`自动认领。
- 目标权重按总资产计算，现金缓冲只在实际买入检查时执行；GoodETF调仓卖出为市价IOC，买入沿用聚宽开盘后取得的last_price与原0.2%偏移生成固定限价。ETF直接申报固定价格；股票仅等待该价格进入对应板块笼子，不按盘口重定价。
- 直连xtquant时，QMT快照优先使用`get_full_tick`并保留交易端原始行情时间；`get_last_quote`和1分钟K线只作回退，不得用服务器当前时间冒充行情时间。缺少时间、超过`QMT_STRATEGY_MAX_AGE_SECONDS`或异常来自未来的QMT mark/tick均不可用于估值、风控或条件下单；QMT持仓和风控日志同时打印行情时间与来源。QMT tick先在本地账本判断价格条件，命中后才查询真实账户并下单；兼容`subscribe_quote`真实返回的`{证券: [tick, ...]}`批量结构，批内短暂触发不会被后续价格覆盖，并为每个标的记录一次“首次收到执行行情”。活动意图结束后自动退订不再需要的标的。订单、成交、错误和断连由QMT原生回调推进，不增加固定1秒轮询。BigQMT HTTP gateway尚未桥接这些原生回调，不能把该适配器视为同等完成。

## 2. 还缺什么

下列外部验证与部署能力尚未完成，因此不能宣称满足实盘要求：

1. 在用户实际MiniQMT/BigQMT上完成订单备注、稳定order/trade ID、费用、状态和跨日查询能力探针。
2. 聚宽JQ、QMT模拟和小额实盘的人工运行证据；真实资金必须再次明确批准。

完整依赖顺序见[实施 slices](04-slices.md)。只有仓库修复完成且真实交易日人工验收逐级通过，才能启用真实资金。

## 3. 当前推荐用法

### 本地开发

按[聚宽本地开发与兼容矩阵](05-joinquant-development.md)建立专用环境。编辑：

```text
strategies/joinquant/good_etf.py
```

不要维护第二份聚宽策略源码，也不要把本地profile、导出目录或凭据提交到Git。

### 聚宽回测

同一份策略自动识别回测，不再切换字符串MODE：

```python
VALIDATE_REMOTE_DURING_BACKTEST = True
STRATEGY_ID = 'good_etf_remote'
```

v18 helper在所有账户组合均须上传。远程预检为`True`时还必须上传私有配置；它只读取真实快照，历史回测仍使用聚宽原生订单。离线回测可临时设为`False`，此时helper不读取配置、不连接服务器。

先运行校验：

```powershell
python -X utf8 scripts/export_joinquant.py --validate-only
```

再导出到一个尚不存在的目录：

```powershell
python -X utf8 scripts/export_joinquant.py --output E:\temp\good_etf_joinquant
```

按[聚宽校验与导出](06-joinquant-export.md)上传产物。上传后不得手工修改；否则聚宽文件已不再对应manifest和已审查源码。

### 账户开关

私有`jq_runtime_config.py`使用两个bool：`jq_account_enabled`和`qmt_account_enabled`。仅JQ开启时，聚宽正常原生下单、撤单和模拟撮合，不叠加QMT执行规则，并维持现有JQ目标计划通知。缺少策略键时默认JQ开、QMT关。

仅QMT开启时，聚宽原生交易函数被阻断，策略只提交StrategyLedger目标。两者同时开启时，helper把同一目标权重分别乘各账户自己的总资产；止盈止损也分别读取两个账户自己的持仓成本。QMT开启后通知归属固定为QMT，只发送QMT计划、委托和成交卡片，不重复发送JQ计划卡片。

回测仍固定BACKTEST，不受两个开关影响。QMT实际下单还受服务器`QMT_STRATEGY_TRADING_ENABLED`和`QMT_STRATEGY_ENABLED_IDS`控制。必须同时上传v18 helper、schema v3私有配置和策略。helper安装时自动完成QMT账户初始化和对账触发，策略不再显式调用readiness接口。仅QMT模式下，启动时QMT分配资金不足、账实差异或对账不新鲜仍会失败关闭；JQ+QMT并行模式下，QMT暂未就绪只暂停QMT分支，JQ继续运行，QMT会在实际调仓或风控前重新对账，通过后才恢复执行。总持仓不足等差异不会被绕过。账本在午夜进入新交易日后，QMT的可卖数量可能仍停留在券商夜间结算前状态；00:00至09:15之间仅由此产生的可卖量差异记为延迟校验，不阻断也不发送阻断卡片，09:15后仍不足才按真实执行风险阻断。完整操作见[本机部署runbook](20-local-deployment-runbook.md)。

helper设置的`set_order_cost`只用于BACKTEST/JQ模拟撮合。QMT在下单前按服务器费用缓冲预留现金；成交后只接受QMT/券商明确返回的实际费用，不用聚宽模拟佣金覆盖。费用缺失时显示为未知，不伪造为0。

### 每次调用的执行参数

模式只决定“在哪个账户执行”，订单语义由每次`submit_targets`调用的不可变`ExecutionRequest`决定：

| 执行类型 | 含义 |
|---|---|
| `LimitExecution` | 按参考价和偏移形成固定限价；ETF直接提交，股票若超出笼子则等待原价可申报；GoodETF买入与止盈使用此类型 |
| `ConditionalLimitExecution` | 对手价进入固定允许边界后才提交限价，供需要固定策略价格条件的其他调用使用 |
| `MarketExecution` | 使用QMT股票市价类型；GoodETF调仓卖出和止损使用此类型 |
| `MarketableLimitExecution` | 立即提交带保护边界的积极限价，不伪装成市价 |

`ExecutionRequest.style`是默认及买侧类型，`sell_style`可覆盖卖侧；执行协议保持schema v2，仍可读取v1。`FollowUpPolicy.NONE`表示终态后不补剩余量，`UNTIL_FILLED_TODAY`表示仅当日处理剩余量。`KEEP_ORIGINAL`始终沿用初次参考价及偏移，GoodETF使用此配置；仅调用方显式选择`RECOMPUTE`才在补单时更新参考价。价格笼子判断只决定原价能否申报，不是重定价指令。

GoodETF当前逐类委托如下：

| 场景 | BACKTEST/JQ | QMT_REMOTE |
|---|---|---|
| 09:30清理非目标持仓/减仓 | 聚宽`order_target(code, 0)`或`order_target_value(...)`，默认撮合，策略不做订单级追单 | 沪市五档即时成交剩余撤销、深市五档即时成交剩余撤销；终态回报后继续补剩余目标，卖出完成后才规划买入 |
| 09:30增持目标ETF | 聚宽`order_target_value(code, target_value)`，不传限价，策略不做订单级追单 | 初次聚宽last_price×1.002，按ETF 0.001元精度和QMT涨跌停规则处理后直接挂单；不等待卖一回落到买价，`UNTIL_FILLED_TODAY + KEEP_ORIGINAL` |
| 5%止损 | 现价严格低于成本95%时调用聚宽`order_target(code, 0)`；未成交且下次风控仍满足条件时再次调用 | 无最低价格阈值的QMT市价IOC，终态回报后在当日继续处理剩余清仓目标 |
| 10%止盈 | 现价严格高于成本110%时调用聚宽`order_target(code, 0)`；未成交且下次风控仍满足条件时再次调用 | 初次触发时的参考价×0.998直接挂限价；不改成买一价，`UNTIL_FILLED_TODAY + KEEP_ORIGINAL` |

这里的“追踪至当日完成”不是定时撤单改价：有效委托保持排队，部分成交只更新已成交量，上一笔进入终态后才按原价处理剩余量；市价IOC的原有补单行为不变。股票限价若明确因笼子拒绝，等待更新行情确认原定价格可申报后再以原价提交；非笼子拒单不盲目重试。ETF不走股票笼子重试。调仓仍先卖后买，各买入标的独立推进，午夜结束旧日目标。1.5%仍只是市价单估值/预留字段，不是新增的成交边界；同一批同时触发止损与止盈时仍优先用市价退出。

若风控触发时普通调仓仍活动，服务器先持久化取消请求：等待价格但尚未挂单的意图可立即结束；已有在途订单时先撤单并等待QMT确认，确认前不会提交第二个重叠目标，也不会在回调后恢复原调仓。

ETF没有股票动态价格笼子，不代表没有涨跌幅限制。服务器采用QMT当日`UpStopPrice/DownStopPrice`，不根据ETF名称或上市日期自行推断豁免；缺少上下限只表示无法做本地钳制，不代表确认该品种无限制。直连QMT通过`T+0基金`板块识别可当日回转品种，新买入成交据此记为当日可卖；其余/无法识别的品种按T+1。板块数据按日缓存，识别失败当日保守按T+1（重启可重试）；已记账的历史批次不自动改变可卖日。最终卖出仍不能超过策略归属可卖量和QMT实际可卖量。

本次实现、验证和升级顺序见[固定价格与申报时机修改记录](23-fixed-limit-review.md)。

## 4. good_etf与原策略的关系

选股和风控主逻辑保持：ETF池、港股关键词过滤、前一日成交额区间、单位净值、停牌/涨停过滤、折价排序、前三名、按折价绝对值分配，以及5%止损/10%止盈。

执行与配置并非原样保留，修改是有意的：

- 删除策略内硬编码host、token、Webhook、账户定位和模式变量；两个账户开关由私有配置按`strategy_id`选择。
- 删除旧同步追单扩展；持久幂等已在S07完成，异步执行状态机由L02实现。
- JQ与QMT可单独或同时启用；并行时共享策略决策但不共享账户状态。
- 组合目标由`available_cash * weight`改为`total_value * DEPLOY_RATIO * normalized_weight`，避免把目标仓位误当成本轮新增买入额并在每轮缩小已有持仓。
- QMT尾盘读取真实PortfolioView并通过自定义`record()`指标展示；聚宽内置收益曲线只代表JQ账户。

因此可以说“策略思想和选股规则基本一致”，不能说“执行、资金和持仓语义与原版完全一样”。

## 5. 仓库与上游更新

推荐远端布局：

```text
origin   -> 个人GitHub fork（可推送）
upstream -> BulletTrade/bullet-trade（只拉取，push URL为DISABLED）
```

同步上游时先保持工作树clean，然后：

```powershell
git fetch upstream --tags
git switch feat/joinquant-live-ledger
git merge upstream/main
```

发生冲突时不得机械覆盖策略、helper、schema、迁移或放行文档；解决后必须重跑当前slice及其依赖测试并重新审查。若希望线性历史，可在未与他人共享分支时使用rebase，但不要对已共享分支强制推送。

## 6. 权威事实来源

- 当前阶段和恢复点：[当前 session](03-session.md)
- 完整架构和实盘缺口：[当前架构、依赖与状态](00-current-state.md)
- 不可变设计决策：[架构与工程决策](02-decisions.md)
- 逐slice验收条件：[实施 slices](04-slices.md)
- 本地编辑/类型检查：[聚宽本地开发与兼容矩阵](05-joinquant-development.md)
- 聚宽导出：[聚宽校验与导出](06-joinquant-export.md)

若本文与提交后的实现不一致，以代码、测试和最新`03-session.md`共同核对，不应仅凭旧聊天记录放行。
