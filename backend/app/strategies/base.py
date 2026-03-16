from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import ClassVar, Any
import pandas as pd

@dataclass
class ParameterSpec:
    name: str
    type: str  # "int" | "float" | "bool" | "str"
    default: Any
    min_val: Any = None
    max_val: Any = None
    description: str = ""

@dataclass
class SignalRecord:
    symbol: str
    name: str = ""
    signal_date: str = ""  # T (dragon-tiger date)
    net_buy_wan: float = 0.0  # net buy in 万元
    extra: dict = field(default_factory=dict)

@dataclass
class StrategyContext:
    current_date: str  # YYYY-MM-DD
    trading_days: list[str]  # all trading days in range
    parameters: dict[str, Any]
    fetcher: Any  # AkShareFetcher

class AbstractStrategy(ABC):
    STRATEGY_ID: ClassVar[str]
    STRATEGY_NAME: ClassVar[str]
    STRATEGY_DESCRIPTION: ClassVar[str] = ""
    PARAMETERS: ClassVar[list[ParameterSpec]] = []

    @abstractmethod
    def generate_signals(self, ctx: StrategyContext) -> list[SignalRecord]:
        ...
