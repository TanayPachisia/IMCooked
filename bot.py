"""CMI Exchange bot framework.

Lightweight bot base class for connecting to the CMI simulated exchange.
"""

import json
import time
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from enum import StrEnum
from functools import cached_property
from threading import Thread
from traceback import format_exc
from typing import Any, Callable, Literal

import requests
import sseclient

STANDARD_HEADERS = {"Content-Type": "application/json; charset=utf-8"}


class DictLikeFrozenDataclassMapping(Mapping):
    """Mixin class to allow frozen dataclasses behave like a dict."""

    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)

    def __iter__(self):
        return iter(self.__annotations__)

    def __len__(self) -> int:
        return len(self.__annotations__)

    def to_dict(self) -> dict:
        return asdict(self)

    def keys(self):
        return self.__annotations__.keys()

    def values(self):
        return [getattr(self, k) for k in self.keys()]

    def items(self):
        return [(k, getattr(self, k)) for k in self.keys()]


@dataclass(frozen=True)
class Product(DictLikeFrozenDataclassMapping):
    symbol: str
    tickSize: float
    startingPrice: int
    contractSize: int


@dataclass(frozen=True)
class Trade(DictLikeFrozenDataclassMapping):
    timestamp: str
    product: str
    buyer: str
    seller: str
    volume: int
    price: float


@dataclass(frozen=True)
class Order(DictLikeFrozenDataclassMapping):
    price: float
    volume: int
    own_volume: int


@dataclass(frozen=True)
class OrderBook(DictLikeFrozenDataclassMapping):
    product: str
    tick_size: float
    buy_orders: list[Order]
    sell_orders: list[Order]


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True)
class OrderRequest:
    product: str
    price: float
    side: Side
    volume: int


@dataclass(frozen=True)
class OrderResponse:
    id: str
    status: Literal["ACTIVE", "PART_FILLED"]
    product: str
    side: Side
    price: float
    volume: int
    filled: int
    user: str
    timestamp: str
    targetUser: str | None = None
    message: str | None = None


class _SSEThread(Thread):
    """Background thread that consumes the CMI SSE stream and dispatches events."""

    def __init__(
        self,
        bearer: str,
        url: str,
        handle_orderbook: Callable[[OrderBook], Any],
        handle_trade_event: Callable[[Trade], Any],
    ):
        super().__init__(daemon=True)
        self._bearer = bearer
        self._url = url
        self._handle_orderbook = handle_orderbook
        self._handle_trade_event = handle_trade_event
        self._http_stream: requests.Response | None = None
        self._client: sseclient.SSEClient | None = None
        self._closed = False

    def run(self):
        while not self._closed:
            try:
                self._consume()
            except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError):
                pass
            except Exception:
                if not self._closed:
                    print("SSE error, reconnecting...")
                    print(format_exc())

    def close(self):
        self._closed = True
        if self._http_stream:
            self._http_stream.close()
        if self._client:
            self._client.close()

    def _consume(self):
        headers = {
            "Authorization": self._bearer,
            "Accept": "text/event-stream; charset=utf-8",
        }
        self._http_stream = requests.get(self._url, stream=True, headers=headers, timeout=30)
        self._client = sseclient.SSEClient(self._http_stream)

        for event in self._client.events():
            if event.event == "order":
                self._on_order_event(json.loads(event.data))
            elif event.event == "trade":
                data = json.loads(event.data)
                trades = data if isinstance(data, list) else [data]
                trade_fields = {f.name for f in Trade.__dataclass_fields__.values()}
                for t in trades:
                    self._handle_trade_event(Trade(**{k: v for k, v in t.items() if k in trade_fields}))

    def _on_order_event(self, data: dict[str, Any]):
        buy_orders = sorted(
            [
                Order(price=float(price), volume=v["marketVolume"], own_volume=v["userVolume"])
                for price, v in data["buyOrders"].items()
            ],
            key=lambda o: -o.price,
        )
        sell_orders = sorted(
            [
                Order(price=float(price), volume=v["marketVolume"], own_volume=v["userVolume"])
                for price, v in data["sellOrders"].items()
            ],
            key=lambda o: o.price,
        )
        self._handle_orderbook(OrderBook(data["productsymbol"], data["tickSize"], buy_orders, sell_orders))


class BaseBot(ABC):
    """Base bot for CMI Exchange.
    """

    def __init__(self, cmi_url: str, username: str, password: str):
        self._cmi_url = cmi_url.rstrip("/")
        self.username = username
        self._password = password
        self._sse_thread: _SSEThread | None = None

        # Incremental trade state
        self.trades: list[Trade] = []
        self._trade_watermark: str | None = None
        self._last_trade_fetch: float | None = None

    @cached_property
    def auth_token(self) -> str:
        response = requests.post(
            f"{self._cmi_url}/api/user/authenticate",
            headers=STANDARD_HEADERS,
            json={"username": self.username, "password": self._password},
        )
        response.raise_for_status()
        return response.headers["Authorization"]

    # -- lifecycle --

    def start(self) -> None:
        if self._sse_thread:
            raise RuntimeError("Bot already running. Call stop() first.")
        self._sse_thread = _SSEThread(
            bearer=self.auth_token,
            url=f"{self._cmi_url}/api/market/stream",
            handle_orderbook=self.on_orderbook,
            handle_trade_event=self.on_trades,
        )
        self._sse_thread.start()

    def stop(self) -> None:
        if self._sse_thread:
            self._sse_thread.close()
            self._sse_thread.join(timeout=5)
            self._sse_thread = None

    # -- callbacks --

    @abstractmethod
    def on_orderbook(self, orderbook: OrderBook) -> None: ...

    @abstractmethod
    def on_trades(self, trade: Trade) -> None: ...

    # -- market trades (incremental) --

    def get_market_trades(self) -> list[Trade]:
        """Fetch new market trades from the exchange and append to self.trades.

        Uses incremental loading: only requests trades newer than the last
        seen timestamp. Returns the full accumulated list.
        """
        params: dict[str, str] = {}
        if self._trade_watermark:
            params["from"] = self._trade_watermark
        response = requests.get(
            f"{self._cmi_url}/api/trade",
            params=params,
            headers=self._auth_headers(),
        )
        self._last_trade_fetch = time.monotonic()
        if not response.ok:
            print(f"Failed to fetch trades: {response.status_code}")
            return self.trades

        new_trades = []
        for raw in response.json():
            trade = Trade(**raw)
            if self._trade_watermark is None or trade.timestamp > self._trade_watermark:
                new_trades.append(trade)

        if new_trades:
            self.trades.extend(new_trades)
            self._trade_watermark = new_trades[-1].timestamp

        return self.trades

    @property
    def last_trade_fetch_age(self) -> float | None:
        """Seconds since last get_market_trades() call, or None if never called."""
        if self._last_trade_fetch is None:
            return None
        return time.monotonic() - self._last_trade_fetch

    # -- trading helpers --

    def send_order(self, order: OrderRequest) -> OrderResponse | None:
        response = requests.post(
            f"{self._cmi_url}/api/order",
            json=asdict(order),
            headers=self._auth_headers(),
        )
        if response.ok:
            return OrderResponse(**response.json())
        print(f"Order failed: {response.text}")
        return None

    def send_orders(self, orders: list[OrderRequest]) -> list[OrderResponse]:
        results: list[OrderResponse] = []

        def _send(o: OrderRequest):
            r = self.send_order(o)
            if r:
                results.append(r)

        threads = [Thread(target=_send, args=(o,)) for o in orders]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return results

    def cancel_order(self, order_id: str) -> None:
        requests.delete(f"{self._cmi_url}/api/order/{order_id}", headers=self._auth_headers())

    def cancel_all_orders(self) -> None:
        orders = self.get_orders()
        threads = [Thread(target=self.cancel_order, args=(o["id"],)) for o in orders]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    def get_orders(self, product: str | None = None) -> list[dict]:
        params = {"productsymbol": product} if product else {}
        response = requests.get(
            f"{self._cmi_url}/api/order/current-user",
            params=params,
            headers=self._auth_headers(),
        )
        return response.json() if response.ok else []

    def get_products(self) -> list[Product]:
        response = requests.get(f"{self._cmi_url}/api/product", headers=self._auth_headers())
        response.raise_for_status()
        return [Product(**p) for p in response.json()]

    def get_positions(self) -> dict[str, int]:
        response = requests.get(
            f"{self._cmi_url}/api/position/current-user",
            headers=self._auth_headers(),
        )
        if response.ok:
            return {p["product"]: p["netPosition"] for p in response.json()}
        return {}

    def get_orderbook(self, product: str) -> OrderBook:
        response = requests.get(
            f"{self._cmi_url}/api/product/{product}/order-book/current-user",
            headers=self._auth_headers(),
        )
        response.raise_for_status()
        data = response.json()
        buy_orders = sorted(
            [Order(price=e["price"], volume=e["volume"], own_volume=e["userOrderVolume"]) for e in data.get("buy", [])],
            key=lambda o: -o.price,
        )
        sell_orders = sorted(
            [Order(price=e["price"], volume=e["volume"], own_volume=e["userOrderVolume"]) for e in data.get("sell", [])],
            key=lambda o: o.price,
        )
        return OrderBook(data["product"], data["tickSize"], buy_orders, sell_orders)

    def get_pnl(self) -> dict:
        response = requests.get(
            f"{self._cmi_url}/api/profit/current-user",
            headers=self._auth_headers(),
        )
        return response.json() if response.ok else {}

    # -- internals --

    def _auth_headers(self) -> dict[str, str]:
        return {**STANDARD_HEADERS, "Authorization": self.auth_token}


class ETFArbitrageBot(BaseBot):
    """ETF Arbitrage bot that exploits price differences between ETF and its components.

    ETF (Market 7) = Market 1 (Water) + Market 3 (Weather) + Market 5 (Airport arrivals)
    """

    def __init__(self, cmi_url: str, username: str, password: str):
        super().__init__(cmi_url, username, password)

        # Track orderbooks for all relevant markets
        self.orderbooks: dict[str, OrderBook] = {}

        # Configuration
        self.MARKET_1 = "M1"  # Water level
        self.MARKET_3 = "M3"  # Temperature * Humidity
        self.MARKET_5 = "M5"  # Airport arrivals
        self.MARKET_7 = "M7"  # ETF

        self.MIN_SPREAD = 5  # Minimum spread to trigger arbitrage
        self.MAX_POSITION = 100  # Maximum position per market
        self.ORDER_SIZE = 5  # Size per arbitrage trade

    def on_orderbook(self, orderbook: OrderBook) -> None:
        """Store orderbook updates and check for arbitrage opportunities."""
        self.orderbooks[orderbook.product] = orderbook

        # Check for arbitrage when we have all orderbooks
        if all(m in self.orderbooks for m in [self.MARKET_1, self.MARKET_3, self.MARKET_5, self.MARKET_7]):
            self.check_arbitrage()

    def on_trades(self, trade: Trade) -> None:
        """Handle trade events."""
        pass

    def get_best_bid(self, product: str) -> float | None:
        """Get best bid price for a product."""
        if product not in self.orderbooks:
            return None
        book = self.orderbooks[product]
        return book.buy_orders[0].price if book.buy_orders else None

    def get_best_ask(self, product: str) -> float | None:
        """Get best ask price for a product."""
        if product not in self.orderbooks:
            return None
        book = self.orderbooks[product]
        return book.sell_orders[0].price if book.sell_orders else None

    def get_mid_price(self, product: str) -> float | None:
        """Get mid price for a product."""
        bid = self.get_best_bid(product)
        ask = self.get_best_ask(product)
        if bid is None or ask is None:
            return None
        return (bid + ask) / 2

    def get_synthetic_etf_bid(self) -> float | None:
        """Calculate the synthetic ETF bid (what we can sell components for)."""
        # To create ETF, we buy components, so we use best bid prices (what we can sell for)
        bid_1 = self.get_best_bid(self.MARKET_1)
        bid_3 = self.get_best_bid(self.MARKET_3)
        bid_5 = self.get_best_bid(self.MARKET_5)

        if None in [bid_1, bid_3, bid_5]:
            return None
        return bid_1 + bid_3 + bid_5

    def get_synthetic_etf_ask(self) -> float | None:
        """Calculate the synthetic ETF ask (what we must pay for components)."""
        # To create ETF, we buy components, so we use best ask prices (what we must pay)
        ask_1 = self.get_best_ask(self.MARKET_1)
        ask_3 = self.get_best_ask(self.MARKET_3)
        ask_5 = self.get_best_ask(self.MARKET_5)

        if None in [ask_1, ask_3, ask_5]:
            return None
        return ask_1 + ask_3 + ask_5

    def check_arbitrage(self) -> None:
        """Check for arbitrage opportunities between ETF and its components."""
        try:
            positions = self.get_positions()

            # Get ETF prices
            etf_bid = self.get_best_bid(self.MARKET_7)
            etf_ask = self.get_best_ask(self.MARKET_7)

            # Get synthetic ETF prices
            synthetic_bid = self.get_synthetic_etf_bid()
            synthetic_ask = self.get_synthetic_etf_ask()

            if None in [etf_bid, etf_ask, synthetic_bid, synthetic_ask]:
                return

            # Opportunity 1: ETF is overpriced
            # Buy components (at synthetic_ask), sell ETF (at etf_bid)
            spread_1 = etf_bid - synthetic_ask

            # Opportunity 2: ETF is underpriced
            # Buy ETF (at etf_ask), sell components (at synthetic_bid)
            spread_2 = synthetic_bid - etf_ask

            # Check position limits
            pos_1 = positions.get(self.MARKET_1, 0)
            pos_3 = positions.get(self.MARKET_3, 0)
            pos_5 = positions.get(self.MARKET_5, 0)
            pos_7 = positions.get(self.MARKET_7, 0)

            # Execute arbitrage if spread is sufficient
            if spread_1 > self.MIN_SPREAD:
                # ETF overpriced: buy components, sell ETF
                if (abs(pos_1 + self.ORDER_SIZE) <= self.MAX_POSITION and
                    abs(pos_3 + self.ORDER_SIZE) <= self.MAX_POSITION and
                    abs(pos_5 + self.ORDER_SIZE) <= self.MAX_POSITION and
                    abs(pos_7 - self.ORDER_SIZE) <= self.MAX_POSITION):

                    print(f"Arbitrage: ETF overpriced by {spread_1:.2f}. Buying components, selling ETF.")
                    self.execute_arbitrage_etf_overpriced()

            elif spread_2 > self.MIN_SPREAD:
                # ETF underpriced: buy ETF, sell components
                if (abs(pos_1 - self.ORDER_SIZE) <= self.MAX_POSITION and
                    abs(pos_3 - self.ORDER_SIZE) <= self.MAX_POSITION and
                    abs(pos_5 - self.ORDER_SIZE) <= self.MAX_POSITION and
                    abs(pos_7 + self.ORDER_SIZE) <= self.MAX_POSITION):

                    print(f"Arbitrage: ETF underpriced by {spread_2:.2f}. Buying ETF, selling components.")
                    self.execute_arbitrage_etf_underpriced()

        except Exception as e:
            print(f"Error in check_arbitrage: {e}")

    def execute_arbitrage_etf_overpriced(self) -> None:
        """Execute arbitrage when ETF is overpriced: buy components, sell ETF."""
        orders = []

        # Buy components at market (cross the spread)
        ask_1 = self.get_best_ask(self.MARKET_1)
        ask_3 = self.get_best_ask(self.MARKET_3)
        ask_5 = self.get_best_ask(self.MARKET_5)

        # Sell ETF at market
        bid_7 = self.get_best_bid(self.MARKET_7)

        if None in [ask_1, ask_3, ask_5, bid_7]:
            return

        orders.append(OrderRequest(self.MARKET_1, ask_1, Side.BUY, self.ORDER_SIZE))
        orders.append(OrderRequest(self.MARKET_3, ask_3, Side.BUY, self.ORDER_SIZE))
        orders.append(OrderRequest(self.MARKET_5, ask_5, Side.BUY, self.ORDER_SIZE))
        orders.append(OrderRequest(self.MARKET_7, bid_7, Side.SELL, self.ORDER_SIZE))

        self.send_orders(orders)

    def execute_arbitrage_etf_underpriced(self) -> None:
        """Execute arbitrage when ETF is underpriced: buy ETF, sell components."""
        orders = []

        # Buy ETF at market
        ask_7 = self.get_best_ask(self.MARKET_7)

        # Sell components at market
        bid_1 = self.get_best_bid(self.MARKET_1)
        bid_3 = self.get_best_bid(self.MARKET_3)
        bid_5 = self.get_best_bid(self.MARKET_5)

        if None in [ask_7, bid_1, bid_3, bid_5]:
            return

        orders.append(OrderRequest(self.MARKET_7, ask_7, Side.BUY, self.ORDER_SIZE))
        orders.append(OrderRequest(self.MARKET_1, bid_1, Side.SELL, self.ORDER_SIZE))
        orders.append(OrderRequest(self.MARKET_3, bid_3, Side.SELL, self.ORDER_SIZE))
        orders.append(OrderRequest(self.MARKET_5, bid_5, Side.SELL, self.ORDER_SIZE))

        self.send_orders(orders)
