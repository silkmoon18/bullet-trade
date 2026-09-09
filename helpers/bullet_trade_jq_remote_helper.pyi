"""Type contract for the standalone JoinQuant strategy runtime helper."""

from enum import Enum
from typing import Any, Callable, Dict, FrozenSet, Literal, Optional, Tuple, TypedDict, Union

from joinquant_typing import Context

STRATEGY_RUNTIME_API_VERSION: int
STRATEGY_RUNTIME_HELPER_MARKER: str
PROFILE_SCHEMA_VERSION: int
HONG_KONG_ETF_KEYWORDS: Tuple[str, ...]
HONG_KONG_ETF_CODE_DENYLIST: FrozenSet[str]

def is_hong_kong_etf(
    security: str,
    display_name: Any = ...,
    traced_index_name: Any = ...,
    keywords: Optional[Tuple[str, ...]] = ...,
    code_denylist: Optional[Any] = ...,
) -> bool: ...

_RuntimeMode = Literal["BACKTEST", "JQ", "QMT_REMOTE", "JQ_QMT_PARALLEL"]


class ExecutionType(str, Enum):
    LIMIT: str
    CONDITIONAL_LIMIT: str
    MARKET: str
    MARKETABLE_LIMIT: str


class RuntimeMode(str, Enum):
    BACKTEST: str
    JQ: str
    QMT_REMOTE: str
    JQ_QMT_PARALLEL: str


class FollowUpPolicy(str, Enum):
    NONE: str
    UNTIL_FILLED_TODAY: str


class RepricingPolicy(str, Enum):
    KEEP_ORIGINAL: str
    RECOMPUTE: str


class ConditionalLimitPriceMode(str, Enum):
    BOUNDARY: str
    COUNTERPARTY: str


class LimitExecution:
    def __new__(cls, price_band_ppm: int = ...) -> LimitExecution: ...
    price_band_ppm: int
    execution_type: ExecutionType


class ConditionalLimitExecution:
    def __new__(
        cls,
        price_band_ppm: int = ...,
        price_mode: ConditionalLimitPriceMode = ...,
    ) -> ConditionalLimitExecution: ...
    price_band_ppm: int
    price_mode: ConditionalLimitPriceMode
    execution_type: ExecutionType


class MarketExecution:
    def __new__(
        cls, protect_price_band_ppm: int = ...
    ) -> MarketExecution: ...
    protect_price_band_ppm: int
    execution_type: ExecutionType


class MarketableLimitExecution:
    def __new__(
        cls, price_band_ppm: int = ...
    ) -> MarketableLimitExecution: ...
    price_band_ppm: int
    execution_type: ExecutionType


_ExecutionStyle = Union[
    LimitExecution,
    ConditionalLimitExecution,
    MarketExecution,
    MarketableLimitExecution,
]


class ExecutionRequest:
    def __new__(
        cls,
        style: _ExecutionStyle = ...,
        follow_up: FollowUpPolicy = ...,
        repricing: RepricingPolicy = ...,
        sell_style: Optional[_ExecutionStyle] = ...,
    ) -> ExecutionRequest: ...
    style: _ExecutionStyle
    follow_up: FollowUpPolicy
    repricing: RepricingPolicy
    sell_style: Optional[_ExecutionStyle]


def default_etf_rebalance_execution() -> ExecutionRequest: ...
def default_etf_stop_loss_execution() -> ExecutionRequest: ...
def default_etf_take_profit_execution() -> ExecutionRequest: ...


class PositionView:
    def __init__(self, payload: Dict[str, Any]) -> None: ...

    security: str
    total_amount: int
    closeable_amount: int
    avg_cost: float
    price: float
    mark_as_of: Optional[str]
    mark_source: Optional[str]
    value: float
    unrealized_pnl: float


class PortfolioView:
    def __init__(self, payload: Dict[str, Any]) -> None: ...

    account_id: str
    as_of: str
    snapshot_version: str
    ledger_version: int
    cash: float
    reserved_cash: float
    available_cash: float
    positions_value: float
    total_value: float
    starting_cash: float
    total_pnl: Optional[float]
    realized_pnl: Optional[float]
    unrealized_pnl: Optional[float]
    fees: Optional[float]
    fees_known: bool
    unknown_fee_fill_count: int
    unknown_price_fill_count: int
    nav: Optional[float]
    returns: Optional[float]
    performance_blockers: Tuple[str, ...]
    performance_ready: bool
    positions: Dict[str, PositionView]


class AccountPortfolioView:
    account: Literal["JQ", "QMT"]
    portfolio: Any


class _StrategyRuntimeRequiredState(TypedDict):
    api_version: int
    profile_schema_version: int
    profile: Optional[str]
    mode: _RuntimeMode
    run_type: str
    strategy_id: str
    jq_account_enabled: bool
    qmt_account_enabled: bool
    enabled: bool
    orders_enabled: bool
    production_ready: bool
    reason: str


class _StrategyRuntimeOptionalState(TypedDict, total=False):
    profile_module: Optional[str]
    blocked_mutations: Tuple[str, ...]
    mirror_jq_orders: bool
    remote_validation_enabled: bool


class _StrategyRuntimeState(_StrategyRuntimeRequiredState, _StrategyRuntimeOptionalState):
    pass


def install_strategy_runtime(
    namespace: Dict[str, Any],
    *,
    context: Context,
    mode: _RuntimeMode,
    strategy_id: str,
    expected_api_version: int = ...,
    profile_module: str = ...,
    validate_remote: bool = ...,
) -> _StrategyRuntimeState: ...


class JoinQuantRuntime:
    def __init__(
        self,
        state: Dict[str, Any],
        namespace: Optional[Dict[str, Any]] = ...,
        qmt_initial_capital: Any = ...,
    ) -> None: ...
    state: Dict[str, Any]
    mode: RuntimeMode
    jq_account_enabled: bool
    qmt_account_enabled: bool

    def configure_platform(self, benchmark: str = ...) -> None: ...
    def schedule_daily(
        self,
        before_market_open: Callable[[Any], Any],
        market_open: Callable[[Any], Any],
        risk_management: Callable[[Any], Any],
        risk_check_times: Tuple[str, ...],
        after_market_check: Callable[[Any], Any],
        reference_security: str = ...,
    ) -> None: ...
    def log_process_initialize(self) -> None: ...
    def log_strategy_event(self, message: str) -> None: ...
    def portfolio(self, context: Context) -> Any: ...
    def account_portfolios(self, context: Context) -> Tuple[AccountPortfolioView, ...]: ...
    def log_account_snapshots(self, context: Context) -> None: ...
    def ensure_ready(self, qmt_initial_capital: Any, context: Context) -> Any: ...
    def submit_targets(
        self,
        context: Context,
        weights: Dict[str, Any],
        marks: Dict[str, Any],
        idempotency_key: str,
        execution: ExecutionRequest,
        security_names: Optional[Dict[str, str]] = ...,
    ) -> Dict[str, Any]: ...
    @staticmethod
    def security_name(security: str, known_name: Any = ...) -> str: ...
    @staticmethod
    def security_label(security: str, known_name: Any = ...) -> str: ...
    def advance_targets(self, context: Context) -> bool: ...
    def cancel_targets(self) -> bool: ...
    def cancel_orders(self) -> int: ...
    def order_target(self, security: str, amount: int) -> Any: ...
    def order_target_value(
        self,
        security: str,
        target_value: float,
        limit_price: Optional[float] = ...,
    ) -> Any: ...
    def notify_target_buy_plan(
        self, items: Any, occurred_at: Any = ...
    ) -> Dict[str, Any]: ...
    @staticmethod
    def target_buy_plan_item(
        security: str,
        target_value: float,
        current_value: float,
        reference_price: float,
        lot_size: int = ...,
        security_name: str = ...,
    ) -> Optional[Dict[str, Any]]: ...
    def send_target_buy_plan(
        self, items: Any, occurred_at: Any = ...
    ) -> Optional[Dict[str, Any]]: ...
    def execute_rebalance(
        self,
        context: Context,
        weights: Dict[str, Any],
        marks: Dict[str, Any],
        idempotency_key: str,
        execution: Optional[ExecutionRequest] = ...,
    ) -> Dict[str, Any]: ...
    def execute_risk_management(
        self,
        context: Context,
        stop_loss_ratio: float,
        take_profit_ratio: float,
        idempotency_key: str,
        stop_loss_execution: Optional[ExecutionRequest] = ...,
        take_profit_execution: Optional[ExecutionRequest] = ...,
    ) -> Dict[str, Any]: ...


def install_joinquant_runtime(
    namespace: Dict[str, Any],
    *,
    context: Context,
    strategy_id: str,
    qmt_initial_capital: Any,
    profile_module: str = ...,
    validate_remote_during_backtest: bool = ...,
    expected_api_version: int = ...,
) -> JoinQuantRuntime: ...


def ensure_account(initial_capital: Any = ...) -> Dict[str, Any]: ...
def get_portfolio(
    marks: Optional[Dict[str, Any]] = ...,
    as_of: Any = ...,
) -> PortfolioView: ...
def submit_targets(
    weights: Dict[str, Any],
    idempotency_key: str,
    marks: Optional[Dict[str, Any]] = ...,
    as_of: Any = ...,
    execution: Optional[ExecutionRequest] = ...,
    security_names: Optional[Dict[str, str]] = ...,
) -> Dict[str, Any]: ...
def notify_target_buy_plan(
    items: Any,
    occurred_at: Any = ...,
) -> Dict[str, Any]: ...
def get_intent(
    intent_id: Optional[str] = ...,
    idempotency_key: Optional[str] = ...,
) -> Dict[str, Any]: ...
def get_reconciliation() -> Dict[str, Any]: ...
def get_configured_execution_mode(
    strategy_id: str,
    profile_module: str = ...,
) -> _RuntimeMode: ...
def get_configured_account_switches(
    strategy_id: str,
    profile_module: str = ...,
) -> Dict[str, bool]: ...
def runtime_portfolio(context: Context) -> Any: ...
def ensure_runtime_ready(initial_capital: Any, context: Context) -> Any: ...
def submit_runtime_targets(
    context: Context,
    weights: Dict[str, Any],
    marks: Dict[str, Any],
    idempotency_key: str,
    execution: ExecutionRequest,
    security_names: Optional[Dict[str, str]] = ...,
) -> Dict[str, Any]: ...
def advance_runtime_targets(context: Context) -> bool: ...
def cancel_runtime_targets() -> bool: ...
def cancel_runtime_orders() -> int: ...
def runtime_order_target(security: str, amount: int) -> Any: ...
def runtime_order_target_value(
    security: str,
    target_value: float,
    limit_price: Optional[float] = ...,
) -> Any: ...
