from dataclasses import dataclass
from datetime import datetime
from enum import Enum, IntEnum

class MarketType(Enum):
    spot = 0
    future = 1
    move = 2
    perpetual = 3
    prediction = 4


@dataclass
class Market:
    exchange: str
    market: str
    instrument: str
    type_: MarketType
    enabled: bool
    maker_fee: float
    taker_fee: float
    tick_size: float
    min_size: float
    price_precision: float
    size_precision: float
    mm_size: float
    expiry: datetime


class Side(IntEnum):
    UNINITIALIZED = -1
    BID = 1
    ASK = 2

    @staticmethod
    def from_signal(signal):
        if signal == 1:
            return Side.BID
        elif signal == -1:
            return Side.ASK
        elif signal == 0:
            return Side.UNINITIALIZED

    @staticmethod
    def from_string(str):
        if str in ("buy", "bid"):
            return Side.BID
        elif str in ("sell", "ask"):
            return Side.ASK
        else:
            raise ValueError(f"Invalid side str: {str}")

    @staticmethod
    def to_string(side):
        if side == Side.BID:
            return "buy"
        elif side == Side.ASK:
            return "sell"
        else:
            raise ValueError(f"Invalid to_string side : {side}")

@dataclass
class Trade:
    symbol: str
    id_: int
    price: float
    size: float
    side: Side
    liquidation: bool
    date: datetime
    timestamp: str


@dataclass
class Quote:
    symbol: str
    price: float
    size: float
    side: Side
    date: datetime
    timestamp: str            