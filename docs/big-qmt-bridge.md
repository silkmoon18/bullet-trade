# Fork：聚宽 → BulletTrade → 大 QMT 本机桥接

适用于 miniQMT/直连 xtquant 不可用、但完整 QMT 可以运行 Python 交易策略的情况。
当前为**离线验证完成、客户端模拟账户待验收**的可选后端；未部署、未启用交易。
上游 HTTP 网关保留不变，本文的 `bridge` 是另一个传输选项，不要把两个 QMT 脚本同时运行。

## 职责和不变项

```text
聚宽 GoodETF 决策 → 原 JQ helper → BT 58620 → StrategyLedger / 执行器
                                                      ↕ localhost:9001
                                             大 QMT 通用单文件 → 券商
```

- 聚宽仍负责数据、选股、权重和风控决策，原 JQ 账户仍使用聚宽原生撮合。
- 服务器仍负责 QMT 策略归属持仓、资金分配、先卖后买、行情条件、追单、幂等、对账和飞书通知。
- 大 QMT 只调用原生查询、`passorder`、`cancel`，返回委托/成交/错误/账户/持仓/tick 回报；不选股，不建账本，不自行追单。
- GoodETF 的参考价和固定限价生成不变，不改为按卖一报价；服务器决定价格后原样传入 QMT。默认市价沿用原 xtquant 的五档即成剩撤（沪42/深47）；是否再发剩余量仍由原服务器执行规则决定。
- QMT 不需要下载 ETF PCF，也不调用 `get_etf_info`。行情、交易接口及订阅权限仍需实际券商客户端验证；本桥接不能绕过券商交易权限。

## 需要更新的文件

1. 服务器：安装此 fork 的 `codex/big-qmt-bridge` 分支，包括新增适配器与通信层。单独换 QMT 文件不够。
2. 大 QMT：复制 [helpers/big_qmt_bridge.py](../helpers/big_qmt_bridge.py) **整个文件**到新 Python 策略，例如 `BT_BRIDGE`。它是单文件，全部 ASCII，GBK/UTF-8 均可读，不需要安装 BulletTrade、xtquant 或额外 Python 库到 QMT 内部。
3. 聚宽：本次**不用更换** `good_etf.py`、`bullet_trade_jq_remote_helper.py` 或私有配置。继续使用 fork 的双账户门面，不要按上游其他教程替换为不同 API 的 helper。

`strategies/qmt/good_etf/good_etf.py` 是之前的“全策略在 QMT 内运行”备选，不是本桥接。保留，但不要与桥接执行同一策略。

## 顶部配置

大 QMT 单文件只需修改顶部配置，不改下面的执行代码：

| 字段 | 填法 |
|---|---|
| `ACCOUNT_ID` | 留空使用交易界面选定账户；识别不到时明确填同一个账户号 |
| `ACCOUNT_TYPE` | 当前只支持 `"STOCK"`，不支持信用/期货/多物理账户 |
| `BRIDGE_TOKEN` | 与服务器 `BIG_QMT_GATEWAY_PASSWORD` 一致；这是本机令牌，不是证券密码 |
| `BRIDGE_PORT` | 默认 `9001`，与服务器 `BIG_QMT_BRIDGE_PORT` 一致 |
| `ENABLE_TRADING` | 初次 `False`；仅在获准的模拟账户主动验证时改 `True` |
| `ENABLE_CANCEL` | 初次 `False`；主动验证时需同时设 `True` 才能撤测试委托 |
| `TIMER_MS` | 默认 `500`；非阻塞网络收发的定时周期，不是下单/追单间隔，也不是 tick 频率保证 |
| `MAX_SUBSCRIPTIONS` | 默认 `100`，仅订阅服务器需要的持仓和目标；按客户端配额调整 |

服务器配置示例在 [env.bigqmt-bridge.example](../env.bigqmt-bridge.example)。已有服务器应在**备份后的原 `.data/.env`**中增改：

```dotenv
QMT_SERVER_TYPE=big_qmt
BIG_QMT_TRANSPORT=bridge
BIG_QMT_BRIDGE_PORT=9001
BIG_QMT_GATEWAY_PASSWORD=填写本机令牌
QMT_STRATEGY_TRADING_ENABLED=false
```

保留原 `QMT_SERVER_TOKEN`、端口/公网访问限制、`QMT_ACCOUNT_ID`、账户 key、原 `QMT_STRATEGY_LEDGER_DB` 路径、通知和策略白名单。不要换一个空数据库或新策略 ID 来规避对账。

`QMT_STRATEGY_CAPABILITIES_FILE` 必须改为新后端实测确认的能力文件；**不得把 xtquant 的证明复制后改名为 BIG_QMT**。未验证时账本执行不放行是预期行为，不修改加载门禁来跳过验证。

## 启动和迁移顺序

1. 先关闭服务器策略交易，确认没有旧目标继续自动执行。停止旧 MiniQMT 执行服务/独立 GoodETF；检查并人工处理遗留委托，再备份原 `.data`（服务停止时备份数据库，保留 WAL/SHM 一并复制）。不要改旧上游仓库。
2. 在 fork 的服务器目录安装本版本，使用服务器原解释器，例如 `python -m pip install -e .`。不要仅 `pip install -U bullet-trade` 覆盖成官方包。
3. 按上面的差异修改私有配置。若原计划任务命令写死 `--server-type qmt`，同步改为 `big_qmt`，命令行参数优先于 `.env`。
4. 从 fork 根目录启动服务器：

   ```powershell
   python -m bullet_trade --env-file .data/.env server --server-type big_qmt
   ```

   `9001` 永远只监听本机 `127.0.0.1`，不用新增公网安全组。聚宽继续连接原 `58620`。

5. 登录 QMT 模拟资金账户，在**模型交易/策略交易**界面运行 `BT_BRIDGE`。不要使用历史回测或只在编辑器点击运行。QMT 的“实盘运行”表示调用所选账户的交易接口，不等于已确认它是模拟资金；须核实登录的资金账户确为模拟账户。
6. 初次脚本两个写开关都保持 `False`。预期出现 `[bt_bridge] ... waiting for BT`，随后 `connected | trading=False`；服务器健康中 `backend_type=big_qmt`、`transport=bridge`、`ready=true`。`ready` 说明当前连接和账户查询可用，不证明成交能力。
7. 完成下节验证后才讨论启用原策略白名单。当前任务没有执行服务器部署、主动测试单或任何开关变更。

## 验收清单

先只读，再明确批准模拟账户测试；QMT 原生写开关与服务器策略总开关不是同一层：允许探针下单时，服务器 `QMT_STRATEGY_TRADING_ENABLED` 仍保持 `false`，防止探针与 GoodETF 并发。

- 只读：账户、持仓、当日委托/成交能查询；目标 ETF 的最新价、买卖一档、原始时间和价格边界有效；服务器最后行情时间正常更新。
- 原生回报：在交易时段用最小可交易数量验证限价买入、精确撤单、市价成交及清理；确认 client_tag、稳定委托号/成交号、成交与原委托关联、方向、状态。佣金/税费缺失继续标为未知。
- 恢复：重启 QMT 桥接，服务器变为未就绪再恢复；订阅和已有目标能重新对账。测试持仓与活动委托都必须清理；不要用修改账本来伪造清理成功。
- 旧账迁移：核对旧 xtquant `order_id` 与原生 QMT `m_strOrderSysID` 的对应关系。两者不能默认相同；若旧在途委托/历史成交的关联对不上，先只读核对并制定映射迁移，不删除旧记录、不改 ID 强行放行。本次未对你的历史库执行 ID 迁移。
- 策略：最后才仅开放获准的策略 ID。先观察一次完整交易日的计划、委托、成交、持仓和日终快照，再决定是否继续。切换完整 QMT 不代表真资金已获准。

现有 `python -m bullet_trade.server.runtime_probe --env-file .data/.env --output-dir .data/bridge-inspect` 默认只读，可复用；配置需要本机 `QMT_SERVER_HOST`。该旧探针也会测试历史 K 线，本桥接刻意不提供 `data.history`，这些项失败不能当作下单故障，亦不能把整份报告冒称全绿。主动 `--trade-smoke` 必须另外批准；此文不自动执行。

## 边界和恢复语义

- 一个物理 STOCK 账户可以承载多个服务器策略账本；不支持一个 QMT 桥接同时管理多个物理账户。
- 500ms 定时器只发送一次命令，价格/tick/成交回报驱动原服务器状态机。初次下单短暂等待委托号（默认3秒），不是等待全部成交。行情由 QMT 推送，不保证每次交易所价格变化都收到。
- `passorder` 的 `0`、`None` 或其他返回值不当作订单号；查询/回调里稳定的真实委托号才可确认。断线或超时的写入按未知态处理，查询认领后继续，不盲目重发。
- 服务器复用原 SQLite 委托/成交历史，跨日返回的是**曾实际观察并保存**的数据，不代表 QMT 原生历史接口可查任意日期。断线跨越交易日、且未收到过的记录不能凭空恢复，需人工核对。
- `stop()` 仅断开本机连接和退订行情，不自动撤券商委托。交易回路停止后仍须核对在途委托。
- 暂不支持本地历史数据策略、PCF、期货/信用、跨机器桥接。旧 `BIG_QMT_TRANSPORT=http` 仍可用，但不具有本桥接的原生回调接入链路。

## 依据和验证记录

- 原生定时器、停止回调和历史回测限制：[迅投系统函数](https://dict.thinktrader.net/innerApi/system_function.html)。
- QMT 单线程环境，不在策略中创建 Python 线程或阻塞服务循环：[迅投使用须知](https://dict.thinktrader.net/innerApi/user_attention.html)。
- 53=部撤、54=已撤、55=部成，沪深市价类型：[迅投枚举](https://dict.thinktrader.net/innerApi/enum_constants.html)。
- 查询外的原生实时回调：[迅投回报函数](https://dict.thinktrader.net/innerApi/callback_function.html)。
- 计划、上游评估、提交和测试记录：[本次 session](live-ledger/43-big-qmt-bridge-plan.md)。
