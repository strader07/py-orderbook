import os
import json
import aiohttp
import logging
from abc import ABC, abstractmethod
from urllib.parse import urlencode, quote_plus

from app.config import Config
from app.types import Trade, Quote, Side
from app.time import date_from_timestamp, date_from_str
from app.types import Market, MarketType

import requests
import pendulum


class WSChannel:
    name = None
    market = None

    def __init__(self, channel_name, market):
        self.name = channel_name
        self.market = market

    @staticmethod
    def to_ftx_filters(ws_channels):
        channels_filters = {}
        for channel in ws_channels:
            if channel.name in channels_filters:
                channels_filters[channel.name].append(channel.market)
            else:
                channels_filters[channel.name] = [channel.market]

        filters = []
        for channel, symbols in channels_filters.items():
            filters.append({"channel": channel, "symbols": symbols})

        return filters


class TardisFeed():
    def __init__(self, exchange, market, from_date, to_date, orderbook=False, trades=False):
        self.got_snapshot = False
        self.trades_markets = [market.market] if trades else []
        self.book_markets = [market.market] if orderbook else []

        self.exchange = exchange
        self.symbol = market.market
        self.from_date = from_date
        self.to_date = to_date
        self.raw_messages = []

        baseurl = "muz-tardis:1juBfOmEjzgTABUxAJUqr8TQ@tardis.muwazana.com"

        replay_options = {
            "exchange": exchange,
            "from": from_date,
            "to": to_date,
        }

        options = urlencode(replay_options)
        self._ENDPOINT = "https://{}/replay?options={}".format(baseurl, "{options}")
        self._http_replay_options = {
            "exchange": exchange,
            "from": self.from_date,
            "to": self.to_date,
            "filters": [],
        }


    def get_http_endpoint(self, replay_options):
        return self._ENDPOINT.format(options=quote_plus(json.dumps(replay_options)))


    @property
    def messages(self):
        return self.raw_messages


    @staticmethod
    async def get_markets(expiry):

        config = Config()        
        exchanges = config.exchanges
        markets_filter = config.markets_filter

        markets = []

        for exchange in exchanges:
            for market_filter in markets_filter:
                our_market = Market(
                    exchange=exchange,
                    market=market_filter,
                    instrument=market_filter.replace("_", ""),
                    type_=MarketType["spot"],
                    enabled=True,
                    maker_fee=0,
                    taker_fee=0,
                    tick_size=0,
                    min_size=0,
                    price_precision=0,
                    size_precision=0,
                    mm_size=0,
                    expiry=None,
                )

                markets.append(our_market)

        return markets


    @staticmethod
    def normalize_price(price):
        try:
            idx = price.index(".")
        except ValueError as e:
            return str(price)
        concat = "%s.%s" % (price[0:idx], price[idx + 1 :].ljust(8, "0")[0:8])
        return concat


    def process_snapshot(self, data):
        bids = []
        asks = []
        time = data["time"]
        utcdt = pendulum.parse(time)
        timestamp = utcdt.timestamp()
        dt = utcdt.replace(tzinfo=None)

        sides = data["params"][1]

        for bid in sides["bids"]:
            price = self.normalize_price(bid[0])
            bids.append(Quote(self.symbol, price, bid[1], Side.BID, dt, timestamp))

        for ask in sides["asks"]:
            price = self.normalize_price(ask[0])
            asks.append(Quote(self.symbol, price, ask[1], Side.ASK, dt, timestamp))

        self.raw_messages.append(("S", (bids, asks)))


    def process_quote(self, data):
        time = data["time"]
        utcdt = pendulum.parse(time)
        timestamp = utcdt.timestamp()
        dt = utcdt.replace(tzinfo=None)

        sides = data["params"][1]

        if "bids" in sides:
            for bid in sides["bids"]:
                price = self.normalize_price(bid[0])
                quote = Quote(self.symbol, price, bid[1], Side.BID, dt, timestamp)
                self.raw_messages.append(("Q", quote))

        if "asks" in sides:
            for ask in sides["asks"]:
                price = self.normalize_price(ask[0])
                quote = Quote(self.symbol, price, ask[1], Side.ASK, dt, timestamp)
                self.raw_messages.append(("Q", quote))


    def process_trade(self, data):
        trades = data["params"][1]
        from_dt = date_from_str(self.from_date).replace(tzinfo=None)

        for raw_trade in trades:
            dt = date_from_timestamp(raw_trade["time"])
            timestamp = dt.timestamp()
            dt = dt.replace(tzinfo=None)

            if dt < from_dt:
                continue

            price = self.normalize_price(raw_trade["price"])

            trade = Trade(
                self.symbol,
                raw_trade["id"],
                price,
                raw_trade["amount"],
                Side.from_string(raw_trade["type"]),
                False,
                dt,
                timestamp,
            )
            self.raw_messages.append(("T", trade))


    def on_message(self, message):
        method = message["method"]

        if method == "depth.update":
            snapshot = message["params"][0]
            if snapshot:
                self.process_snapshot(message)
            else:
                self.process_quote(message)

        elif method == "trades.update":
            self.process_trade(message)


    async def replay(self):
        book_channels = (
            [WSChannel("depth", market) for market in self.book_markets]
            if self.book_markets
            else []
        )

        trades_channels = (
            [WSChannel("trades", market) for market in self.book_markets]
            if self.book_markets
            else []
        )

        await self.subscribe_http(book_channels + trades_channels)


    async def subscribe_http(self, ws_channels):
        self._http_replay_options["filters"] = WSChannel.to_ftx_filters(ws_channels)
        timeout = aiohttp.ClientTimeout(total=0)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            url = self.get_http_endpoint(self._http_replay_options)

            async with session.get(url) as response:
                response.content._high_water = 100_000_000

                async for line in response.content:

                    raw = json.loads(line.decode())
                    data = raw["message"]
                    data["time"] = raw["localTimestamp"]
                    if isinstance(data, str):
                        err = f"Error: {data}"
                        logging.error(err)
                        raise RuntimeError(err)

                    self.on_message(data)
