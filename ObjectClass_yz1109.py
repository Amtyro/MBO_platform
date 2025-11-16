"""
Basic data structure used for general trading function in the trading platform.
"""

from dataclasses import dataclass, field
from datetime import datetime as Datetime
from collections import deque, defaultdict
from typing import Deque, Dict, Optional, Literal
import struct
import json
from databento_dbn import Action

# from .constant import Direction, Exchange, Interval, Offset, Status, Product, OptionType, OrderType

INFO: int = 20
EVENT_MBO = 'eMBO'
EVENT_TIMER = 'eTimer'
MBO_STRUCT = struct.Struct("!Q I c B d d Q Q")
SIDE_ENCODE = {
    "":0,
    "B" : 1, # bid
    "A" : 2, # ask
    "S" : 3, # sell 
}
ACTION_TO_BYTE = {
    Action.ADD : b"A",
    Action.CANCEL: b"C",
    Action.FILL:   b"F",
    Action.MODIFY: b"M",
    Action.TRADE:  b"T",
}
# ACTIVE_STATUSES = set([Status.SUBMITTING, Status.NOTTRADED, Status.PARTTRADED])

Side = Literal["B","A"] # Bid or Ask

@dataclass
class BaseData:
    """
    Any data object needs a gateway_name as source
    and should inherit base data.
    """

    gateway_name= 'HFT_MBO'

    extra: dict | None = field(default=None, init=False)

@dataclass
class MBOData(BaseData):
    """
    Event data contains information about:
        coming event news
    """
    # symbol : str
    ts_event : Datetime

    rtype: int 
    publisher_id:int 
    instrument_id : int
    action : str
    side : str
    price : float
    size : float
    channel_id : int 
    order_id : float
    flags : float
    ts_in_delta : float
    sequence : float

@dataclass
class OrderBook:
    """
    price-time FIFO order book
    
    -Each price level holds a deque to preserve FIFO
    - `orders` maps order_id -> (side, price) to locate a level quickly.
      (For production speed you'd keep a node pointer / linked list.)
    """
    def __init__(self, depth:int = 10) -> None:
        self.depth = depth
        self.bids: Dict[float, Deque[MBOData]] = {}   # price -> deque
        self.asks: Dict[float, Deque[MBOData]] = {}
        self.bid_levels = defaultdict(float)
        self.ask_levels = defaultdict(float)
        self.last_ts : int | None = None
        self.id : str | None = None
        self.orders: Dict[int, tuple[Side, float]] = {}  # order_id -> (side, price)

    # ---------- helpers ----------

    def _side_book(self, side: Side) -> Dict[float, Deque[MBOData]]:
        return self.bids if side == "B" else self.asks

    def _insert(self, side:Side, mbo: MBOData) -> None:
        # pick the correct side book
        levels = self._side_book(side)

        # FIFO deque per price level
        q = levels.setdefault(mbo.price, deque())
        q.append(mbo)              
        # store mapping : order_id -> (side, price)
        oid = int(mbo.order_id)
        self.orders[oid]= (side, mbo.price)

        if side == "B":
            self.bid_levels[mbo.price] += mbo.size
        else:
            self.ask_levels[mbo.price] += mbo.size

    def _remove_or_reduce(self, order_id: int, reduce_size: Optional[int] = None) -> None:
        """
        Cancel or partially reduce an order by order_id.
        If reduce_size is None: remove entirely.
        Else: subtract size; delete the order if it reaches 0.
        """
        info = self.orders.get(order_id)
        if not info:
            return  # unknown order 
        side, price = info
        levels = self._side_book(side)
        q = levels.get(price)
        if not q:
            self.orders.pop(order_id, None)
            return

        # Linear scan within the price level (OK for a clean baseline).
        # For high throughput, store node refs.
        for i in range(len(q)):
            if q[i].order_id == order_id:
                if reduce_size is None or reduce_size >= q[i].size:
                    delta = q[i].size
                    # remove order fully
                    q.remove(q[i])
                    self.orders.pop(order_id, None)
                else:
                    delta = reduce_size
                    # partial reduction
                    q[i].size -= reduce_size

                if side == "B":
                    self.bid_levels[price] -= delta
                    if self.bid_levels[price] <= 0:
                        self.bid_levels.pop(price,None)
                else:
                    self.ask_levels[price] -= delta
                    if self.ask_levels[price] <= 0:
                        self.ask_levels.pop(price,None)
                break

        if q and len(q) == 0:
            levels.pop(price, None)

    def _modify(self, order_id: int, new_price: Optional[float], new_size: Optional[int], ts_event: Datetime) -> None:
        """
        Modify an order's price and/or size. If price changes,
        we move the order to the new price level and (typically) to the tail
        to reflect loss of time priority on many venues.
        """
        info = self.orders.get(order_id)
        if not info:
            return
        side, old_price = info
        levels = self._side_book(side)
        q = levels.get(old_price)
        if not q:
            self.orders.pop(order_id, None)
            return

        # Find and pop from old price level
        moved_order: Optional[MBOData] = None
        for i in range(len(q)):
            if q[i].order_id == order_id:
                moved_order = q[i]
                q.remove(q[i])
                break

        if moved_order is None:
            self.orders.pop(order_id, None)
            return

        # remove old volume from aggregated level
        if side == "B":
            self.bid_levels[old_price] -= moved_order.size
            if self.bid_levels[old_price] <= 0:
                self.bid_levels.pop(old_price, None)
        else:
            self.ask_levels[old_price] -= moved_order.size
            if self.ask_levels[old_price] <= 0:
                self.ask_levels.pop(old_price, None)

        # Apply updates
        # apply size/price changes
        if new_size is not None:
            moved_order.size = new_size
        if new_price is not None:
            moved_order.price = new_price

        moved_order.ts_event = ts_event

        # insert into new price level
        new_price_eff = moved_order.price
        new_levels = self._side_book(side)
        new_q = new_levels.setdefault(new_price_eff, deque())
        new_q.append(moved_order)

        # update order map
        self.orders[order_id] = (side, new_price_eff)

        # add to new aggregated level
        if side == "B":
            self.bid_levels[new_price_eff] += moved_order.size
        else:
            self.ask_levels[new_price_eff] += moved_order.size

        # Clean up empty level
        if q and len(q) == 0:
            levels.pop(old_price, None)

    # ---------- public API you’ll call from the handler ----------

    def add_order(self, mbo : MBOData) -> None:
        if mbo.size <= 0:
            return
        side: Side = mbo.side.upper()
        if side not in ("B","A"):
            return
        
        self.last_ts = mbo.ts_event
        self.id = str(mbo.instrument_id)
        self._insert(side, mbo)

    def cancel(self, order_id: int, size: Optional[int] = None) -> None:
        self._remove_or_reduce(order_id, reduce_size=size)

    def trade_fill(self, order_id: int, filled_size: int) -> None:
        if filled_size > 0:
            self._remove_or_reduce(order_id, reduce_size=filled_size)

    def modify(self, order_id: int, new_price: Optional[float], new_size: Optional[int], ts_event: Datetime) -> None:
        self._modify(order_id, new_price, new_size, ts_event)

    def clear(self) -> None:
        self.bids.clear()
        self.asks.clear()
        self.orders.clear()
        self.bid_levels.clear()
        self.ask_levels.clear()

    # Convenience for debugging
    def top_of_book(self) -> dict:
        best_bid = max(self.bids.keys()) if self.bids else None
        best_ask = min(self.asks.keys()) if self.asks else None
        bb_sz = self.bids[best_bid][0].size if best_bid is not None else None
        ba_sz = self.asks[best_ask][0].size if best_ask is not None else None
        return {"best_bid": best_bid, "best_bid_sz": bb_sz,
                "best_ask": best_ask, "best_ask_sz": ba_sz}
    
    def snapshot_dict(self,depth : int | None = None)->dict:
        """
        Build a lightweight top-N snapshot, using the pre-aggregated
        bid_levels/ask_levels. This is O(#price_levels log N) and
        independent of the number of individual orders.
        """
        if depth is None:
            depth = self.depth

        # sort price levels
        bid_items = sorted(self.bid_levels.items(), key=lambda x: x[0], reverse=True)
        ask_items = sorted(self.ask_levels.items(), key=lambda x: x[0])

        bids = [
            {"price": float(p), "size": float(sz)}
            for p, sz in bid_items if sz > 0
        ][:depth]

        asks = [
            {"price": float(p), "size": float(sz)}
            for p, sz in ask_items if sz > 0
        ][:depth]

        return {
            "id": self.id,
            "last_ts": self.last_ts if self.last_ts else None,
            "bids": bids,
            "asks": asks,
        }

    def snapshot_json(self, depth: int | None = None) -> str:
        """
        JSON-encoded snapshot for sending to clients / logging.
        separators=(",",":") keeps it compact and fast.
        """
        return json.dumps(self.snapshot_dict(depth), separators=(",", ":"))

# @dataclass
# class TickData(BaseData):
#     """
#     Tick data contains information about:
#         * last trade in market
#         * orderbook snapshot
#         * intraday market statistics.
#     """

#     symbol: str
#     exchange: Exchange
#     datetime: Datetime

#     name: str = ""
#     volume: float = 0
#     turnover: float = 0
#     open_interest: float = 0
#     last_price: float = 0
#     last_volume: float = 0
#     limit_up: float = 0
#     limit_down: float = 0

#     open_price: float = 0
#     high_price: float = 0
#     low_price: float = 0
#     pre_close: float = 0

#     bid_price_1: float = 0
#     bid_price_2: float = 0
#     bid_price_3: float = 0
#     bid_price_4: float = 0
#     bid_price_5: float = 0

#     ask_price_1: float = 0
#     ask_price_2: float = 0
#     ask_price_3: float = 0
#     ask_price_4: float = 0
#     ask_price_5: float = 0

#     bid_volume_1: float = 0
#     bid_volume_2: float = 0
#     bid_volume_3: float = 0
#     bid_volume_4: float = 0
#     bid_volume_5: float = 0

#     ask_volume_1: float = 0
#     ask_volume_2: float = 0
#     ask_volume_3: float = 0
#     ask_volume_4: float = 0
#     ask_volume_5: float = 0

#     localtime: Datetime | None = None

#     def __post_init__(self) -> None:
#         """"""
#         self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"


# @dataclass
# class BarData(BaseData):
#     """
#     Candlestick bar data of a certain trading period.
#     """

#     symbol: str
#     #exchange: Exchange
#     datetime: Datetime

#     #interval: Interval | None = None
#     volume: float = 0
#     turnover: float = 0
#     open_interest: float = 0
#     open_price: float = 0
#     high_price: float = 0
#     low_price: float = 0
#     close_price: float = 0

#     def __post_init__(self) -> None:
#         """"""
#         self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"


# @dataclass
# class OrderData(BaseData):
#     """
#     Order data contains information for tracking lastest status
#     of a specific order.
#     """

#     symbol: str
#     #exchange: Exchange
#     orderid: str

#     # type: OrderType = OrderType.LIMIT
#     # direction: Direction | None = None
#     # offset: Offset = Offset.NONE
#     # price: float = 0
#     # volume: float = 0
#     # traded: float = 0
#     # status: Status = Status.SUBMITTING
#     datetime: Datetime | None = None
#     reference: str = ""

#     def __post_init__(self) -> None:
#         """"""
#         self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"
#         self.vt_orderid: str = f"{self.gateway_name}.{self.orderid}"

#     def is_active(self) -> bool:
#         """
#         Check if the order is active.
#         """
#         # return self.status in ACTIVE_STATUSES

#     def create_cancel_request(self) -> "CancelRequest":
#         """
#         Create cancel request object from order.
#         """
#         req: CancelRequest = CancelRequest(
#             orderid=self.orderid, symbol=self.symbol, exchange=self.exchange
#         )
#         return req


# @dataclass
# class TradeData(BaseData):
#     """
#     Trade data contains information of a fill of an order. One order
#     can have several trade fills.
#     """

#     symbol: str
#     # exchange: Exchange
#     orderid: str
#     tradeid: str
#     # direction: Direction | None = None

#     # offset: Offset = Offset.NONE
#     price: float = 0
#     volume: float = 0
#     datetime: Datetime | None = None

#     def __post_init__(self) -> None:
#         """"""
#         self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"
#         self.vt_orderid: str = f"{self.gateway_name}.{self.orderid}"
#         self.vt_tradeid: str = f"{self.gateway_name}.{self.tradeid}"


# @dataclass
# class PositionData(BaseData):
#     """
#     Position data is used for tracking each individual position holding.
#     """

#     symbol: str
#     # exchange: Exchange
#     # direction: Direction

#     volume: float = 0
#     frozen: float = 0
#     price: float = 0
#     pnl: float = 0
#     yd_volume: float = 0

#     def __post_init__(self) -> None:
#         """"""
#         self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"
#         self.vt_positionid: str = f"{self.gateway_name}.{self.vt_symbol}.{self.direction.value}"


# @dataclass
# class AccountData(BaseData):
#     """
#     Account data contains information about balance, frozen and
#     available.
#     """

#     accountid: str

#     balance: float = 0
#     frozen: float = 0

#     def __post_init__(self) -> None:
#         """"""
#         self.available: float = self.balance - self.frozen
#         self.vt_accountid: str = f"{self.gateway_name}.{self.accountid}"


# @dataclass
# class LogData(BaseData):
#     """
#     Log data is used for recording log messages on GUI or in log files.
#     """

#     msg: str
#     level: int = INFO

#     def __post_init__(self) -> None:
#         """"""
#         self.time: Datetime = Datetime.now()


# @dataclass
# class ContractData(BaseData):
#     """
#     Contract data contains basic information about each contract traded.
#     """

#     symbol: str
#     # exchange: Exchange
#     # name: str
#     # product: Product
#     size: float
#     pricetick: float

#     min_volume: float = 1                   # minimum order volume
#     max_volume: float | None = None         # maximum order volume
#     stop_supported: bool = False            # whether server supports stop order
#     net_position: bool = False              # whether gateway uses net position volume
#     history_data: bool = False              # whether gateway provides bar history data

#     option_strike: float | None = None
#     option_underlying: str | None = None     # vt_symbol of underlying contract
#     # option_type: OptionType | None = None
#     option_listed: Datetime | None = None
#     option_expiry: Datetime | None = None
#     option_portfolio: str | None = None
#     option_index: str | None = None          # for identifying options with same strike price

#     def __post_init__(self) -> None:
#         """"""
#         self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"


# @dataclass
# class QuoteData(BaseData):
#     """
#     Quote data contains information for tracking lastest status
#     of a specific quote.
#     """

#     symbol: str
#     exchange: Exchange
#     quoteid: str

#     bid_price: float = 0.0
#     bid_volume: int = 0
#     ask_price: float = 0.0
#     ask_volume: int = 0
#     bid_offset: Offset = Offset.NONE
#     ask_offset: Offset = Offset.NONE
#     status: Status = Status.SUBMITTING
#     datetime: Datetime | None = None
#     reference: str = ""

#     def __post_init__(self) -> None:
#         """"""
#         self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"
#         self.vt_quoteid: str = f"{self.gateway_name}.{self.quoteid}"

#     def is_active(self) -> bool:
#         """
#         Check if the quote is active.
#         """
#         return self.status in ACTIVE_STATUSES

#     def create_cancel_request(self) -> "CancelRequest":
#         """
#         Create cancel request object from quote.
#         """
#         req: CancelRequest = CancelRequest(
#             orderid=self.quoteid, symbol=self.symbol, exchange=self.exchange
#         )
#         return req


# @dataclass
# class SubscribeRequest:
#     """
#     Request sending to specific gateway for subscribing tick data update.
#     """

#     symbol: str
#     exchange: Exchange

#     def __post_init__(self) -> None:
#         """"""
#         self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"


# @dataclass
# class OrderRequest:
#     """
#     Request sending to specific gateway for creating a new order.
#     """

#     symbol: str
#     exchange: Exchange
#     direction: Direction
#     type: OrderType
#     volume: float
#     price: float = 0
#     offset: Offset = Offset.NONE
#     reference: str = ""

#     def __post_init__(self) -> None:
#         """"""
#         self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"

#     def create_order_data(self, orderid: str, gateway_name: str) -> OrderData:
#         """
#         Create order data from request.
#         """
#         order: OrderData = OrderData(
#             symbol=self.symbol,
#             exchange=self.exchange,
#             orderid=orderid,
#             type=self.type,
#             direction=self.direction,
#             offset=self.offset,
#             price=self.price,
#             volume=self.volume,
#             reference=self.reference,
#             gateway_name=gateway_name,
#         )
#         return order


# @dataclass
# class CancelRequest:
#     """
#     Request sending to specific gateway for canceling an existing order.
#     """

#     orderid: str
#     symbol: str
#     exchange: Exchange

#     def __post_init__(self) -> None:
#         """"""
#         self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"


# @dataclass
# class HistoryRequest:
#     """
#     Request sending to specific gateway for querying history data.
#     """

#     symbol: str
#     exchange: Exchange
#     start: Datetime
#     end: Datetime | None = None
#     interval: Interval | None = None

#     def __post_init__(self) -> None:
#         """"""
#         self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"


# @dataclass
# class QuoteRequest:
#     """
#     Request sending to specific gateway for creating a new quote.
#     """

#     symbol: str
#     exchange: Exchange
#     bid_price: float
#     bid_volume: int
#     ask_price: float
#     ask_volume: int
#     bid_offset: Offset = Offset.NONE
#     ask_offset: Offset = Offset.NONE
#     reference: str = ""

#     def __post_init__(self) -> None:
#         """"""
#         self.vt_symbol: str = f"{self.symbol}.{self.exchange.value}"

#     def create_quote_data(self, quoteid: str, gateway_name: str) -> QuoteData:
#         """
#         Create quote data from request.
#         """
#         quote: QuoteData = QuoteData(
#             symbol=self.symbol,
#             exchange=self.exchange,
#             quoteid=quoteid,
#             bid_price=self.bid_price,
#             bid_volume=self.bid_volume,
#             ask_price=self.ask_price,
#             ask_volume=self.ask_volume,
#             bid_offset=self.bid_offset,
#             ask_offset=self.ask_offset,
#             reference=self.reference,
#             gateway_name=gateway_name,
#         )
#         return quote
