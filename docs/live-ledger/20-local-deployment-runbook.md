# 本机部署、备份与小额验收

## 当前放行结论

代码已具备本机运行入口、滚动日志、每日SQLite在线备份、恢复工具、飞书交易卡片和启动对账。真实QMT能力验证及JQ/QMT模拟/小额实盘仍必须在用户机器人工执行；mock测试不能替代这些证据。

在这些步骤完成前保持：

```dotenv
QMT_STRATEGY_TRADING_ENABLED=false
QMT_STRATEGY_ENABLED_IDS=good_etf_remote
```

## 1. 准备专用账户和目录

StrategyLedger只归属带有本策略订单标记的委托、成交和持仓，允许QMT物理账户存在人工交易或其他策略资产。若外部操作导致物理现金或持仓不足以覆盖策略账本，仍会阻断。当前统一把运行数据放在个人fork根目录下的`.data`；该目录已被Git忽略，禁止提交：

```text
E:\dev\Github\bullet-trade_baihua\
├─ bullet_trade\
├─ strategies\
├─ scripts\
└─ .data\
   ├─ .env
   ├─ strategy-ledger.db
   ├─ xtquant-capabilities.json
   ├─ logs\
   └─ backups\
```

复制`env.example`为`.data\.env`并填写QMT、server、StrategyLedger和可选飞书配置。私有文件不要提交Git。

## 2. 运行真实QMT探针

先启动QMT和BulletTrade，交易开关仍为false。执行：

```powershell
.\.venv\Scripts\python.exe -m bullet_trade.server.runtime_probe `
  --env-file E:\dev\Github\bullet-trade_baihua\.data\.env `
  --output-dir E:\dev\Github\bullet-trade_baihua\.data\runtime-probe `
  --trade-smoke
```

`trade-smoke`会产生真实小额委托，执行前必须确认标的、数量和模拟/小额账户。结合`probe_report.json`及QMT原始查询，跨进程、跨交易日人工核对：remark回显、稳定order/trade ID、trade-order关联、方向、明确费用、订单状态、当前与working查询、前一交易日lookback。

直连 xtquant 只能查询当日委托和成交。服务器启用 StrategyLedger 后会自动在同一
SQLite 中保存已收到的委托/成交回调及查询结果，对账自动合并这些本地历史；
`broker.orders` / `broker.trades` 的默认响应仍保持当日语义。SQLite 每日备份同时覆盖
这部分历史。它只能保存服务器实际观察到的数据，不能补回跨日停机期间遗漏的回报。

佣金以QMT/券商明确返回的数据为最终依据。迅投标准股票`XtTrade`提供`traded_id`、成交量、成交价和成交金额，但官方结构没有佣金字段；`used_commission`属于期货持仓统计字段，不能预设为股票逐笔成交佣金。部分柜台或扩展版本可能在成交或`query_data(..., data_type='deal')`中补充费用，服务器会兼容读取；买入费用缓冲只用于下单前现金预留，不是最终佣金。字段缺失时佣金和税费记为未知，不伪造为0，也不阻断委托与成交入账；此时现金、持仓和资产是暂估值，精确`fees/NAV/returns/PnL`保持未知。

模拟柜台连成交价、成交金额和可用委托价都缺失时，可在服务器`.data/.env`设置`QMT_STRATEGY_UNPRICED_FILL_POLICY=ZERO_FALLBACK`：按0价记录成交并更新实际成交数量，保留`price_known=false`，通知明确说明“非真实成交价”。卖出不增加本金现金，买入不扣本金，已知费用仍扣除，未知费用不变；因此资金、成本和收益失真，精确收益指标仍不可用，后续调仓资金及成本风控也可能受影响。默认`STRICT`和原`CONSERVATIVE_ORDER_PRICE`不变。详见[零价兜底说明](26-zero-price-fallback.md)，不建议用于要求精确账本的真实账户。

复制[直连xtquant能力证明模板](xtquant-capabilities.example.json)到`.data`，只把实际证明为真的字段改为`true`，填写真实报告路径和lookback天数。订单归属、稳定ID、成交关联、方向映射、状态与查询等执行必需项为false时服务器会拒绝加载；`fee_fields=false`允许运行，但绩效费用维持未知。不能靠代码猜值或补零。

## 3. 首次只读启动

```powershell
Set-Location E:\dev\Github\bullet-trade_baihua
.\.venv\Scripts\python.exe -m bullet_trade --env-file .\.data\.env server
```

服务器启动后会对已存在策略账户立即同步QMT；若QMT仍在启动，则保持`strategy_ledger_ready=false`并每5秒重试，成功后停止重试。第一次由聚宽调用`ensure_account`建立1万元策略账户。交易关闭时能力证明可以暂缓，账本仍可返回`READY`；将`QMT_STRATEGY_TRADING_ENABLED`改为`true`后，能力证明缺失会立即重新阻断。即使全局开关已打开，`QMT_STRATEGY_ENABLED_IDS`为空或不包含请求中的精确`strategy_id`时也不会创建委托。

日志已使用`RotatingFileHandler`，单文件5MB、保留3个历史文件。配置`QMT_SERVER_LOG_FILE`即可，无需另一套日志轮转程序。

## 4. Windows计划任务

在已经创建`.venv`后执行：

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\windows\register_strategy_tasks.ps1 `
  -ProjectDir E:\dev\Github\bullet-trade_baihua `
  -EnvFile E:\dev\Github\bullet-trade_baihua\.data\.env `
  -LedgerDatabase E:\dev\Github\bullet-trade_baihua\.data\strategy-ledger.db `
  -BackupDir E:\dev\Github\bullet-trade_baihua\.data\backups
```

注册两个当前用户、有限权限任务：登录时启动server（失败最多重启3次），每日18:00在线备份SQLite。QMT通常依赖登录桌面会话，因此不注册无人登录的SYSTEM服务。

## 5. 备份与恢复

手工备份：

```powershell
python scripts\strategy_ledger_backup.py backup `
  --database E:\dev\Github\bullet-trade_baihua\.data\strategy-ledger.db `
  --output-dir E:\dev\Github\bullet-trade_baihua\.data\backups
```

恢复前停止BulletTrade server，把当前数据库及`-wal/-shm`文件整体移到单独故障目录，然后恢复到不存在的目标；工具拒绝覆盖已有数据库：

```powershell
python scripts\strategy_ledger_backup.py restore `
  --backup E:\dev\Github\bullet-trade_baihua\.data\backups\strategy-ledger-YYYYMMDD-HHMMSS.db `
  --database E:\dev\Github\bullet-trade_baihua\.data\strategy-ledger.db
```

恢复后先保持交易关闭，启动server并确认启动对账READY，再恢复聚宽策略。

## 6. 飞书卡片

配置Webhook后自动发送：`JQ/QMT_REMOTE`目标买入计划，以及QMT_REMOTE的委托提交、部分成交、全部成交、撤单、拒单和对账阻断。计划卡片汇总标的、本轮新增买入数量和总金额；交易卡片包含标的、方向、状态、金额、数量、单价、时间，以及可用时的订单号/成交号。通知发送失败只记为通知失败，不回滚真实账本。

## 7. 人工验收顺序

1. 聚宽`JQ`至少观察若干完整交易日：选股、权重、时间、聚宽模拟订单/持仓/指标和目标计划卡片符合预期。
2. QMT模拟账户完成买入部分成交、撤单、拒单、卖出零成交、T+1、进程重启和同一调仓key重放。
3. 核对0重复订单、0未解释账实差异，备份恢复演练成功，飞书卡片字段正确。
4. 用户再次明确批准准确小额金额后，才把`QMT_STRATEGY_TRADING_ENABLED=true`用于专用小额账户。
5. 至少完成一个小额真实买卖闭环和日终对账后，再单独决定是否提高到1万元。

GoodETF验收：ETF按初次last_price×1.002买入、按止盈触发价×0.998卖出，保留原报价精度/涨跌停处理；盘口价格变化不改变该限价。股票在固定价格超出笼子时等待，进入笼子后仍按原价提交；明确笼子废单等待新tick再检查，不盲重发。有效单继续排队，终态剩余量仅在当日处理；市价调仓卖出、止损及先卖后买不变。行情/委托/成交由原生回调推进，启动、重连、午夜仍走原有生命周期管理。

本仓库不会因代码测试通过自动打开真实交易，也不会替用户批准资金。
