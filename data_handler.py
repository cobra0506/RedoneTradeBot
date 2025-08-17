import asyncio
import websocket
import json
import time
import concurrent.futures
from collections import deque
from pybit.unified_trading import HTTP
from requests import Session as RawSession, get as raw_get
from utils import logger, validate_candle, add_candle_uniquely, convert_timestamp_to_readable
import global_data

def get_client(demo=global_data.demo):
    key = global_data.config['api']['demo_key'] if demo else global_data.config['api']['real_key']
    secret = global_data.config['api']['demo_secret'] if demo else global_data.config['api']['real_secret']
    try:
        return HTTP(api_key=key, api_secret=secret, demo=demo)
    except Exception as e:
        logger.error(f"Pybit client failed: {e}. Falling back to raw API.")
        session = RawSession()
        session.headers.update({'X-BAPI-API-KEY': key, 'X-BAPI-SIGN-TYPE': '2', 'X-BAPI-RECV-WINDOW': '5000'})
        return session

def raw_get_instruments(params):
    url = 'https://api.bybit.com/v5/market/instruments-info'
    response = raw_get(url, params=params)
    return response.json()

def get_symbols():
    client = get_client()
    excluded_symbols = ['USDC', 'USDE', 'USTC']
    all_symbols = []
    cursor = None
    while True:
        params = {"category": "linear", "limit": 1000, "cursor": cursor}
        try:
            if hasattr(client, 'get_instruments_info'):
                response = client.get_instruments_info(**params)
            else:
                response = raw_get_instruments(params)
            if response.get("retCode") != 0:
                logger.error(f"Failed to fetch symbols: {response.get('retMsg')}")
                break
            items = response['result']['list']
            symbols = [
                item['symbol']
                for item in items
                if not any(exclusion in item['symbol'] for exclusion in excluded_symbols)
                and "-" not in item['symbol']
                and not item['symbol'].endswith("PERP")
            ]
            all_symbols.extend(symbols)
            cursor = response['result'].get('nextPageCursor')
            if not cursor:
                break
        except Exception as e:
            logger.error(f"Exception fetching symbols: {e}")
            time.sleep(1)
    global_data.symbols = all_symbols
    logger.info(f"Fetched {len(all_symbols)} perpetual symbols")
    return all_symbols

def get_historical_data(symbol, interval, limit=global_data.candle_limit, start_time=None, end_time=None):
    client = get_client()
    data = []
    while True:
        params = {"symbol": symbol, "interval": interval, "limit": 1000}
        if start_time:
            params["start"] = start_time
        if end_time:
            params["end"] = end_time
        try:
            if hasattr(client, 'get_kline'):
                response = client.get_kline(**params)
            else:
                url = 'https://api.bybit.com/v5/market/kline'
                response = raw_get(url, params=params).json()
            if response.get('retCode') != 0:
                logger.error(f"Failed to fetch historical data for {symbol}/{interval}: {response.get('retMsg')}")
                return []
            candles = response['result']['list'][:-1]  # Exclude open candle
            validated = []
            for item in candles:
                candle = {
                    'timestamp': int(item[0]),
                    'open': float(item[1]),
                    'high': float(item[2]),
                    'low': float(item[3]),
                    'close': float(item[4]),
                    'volume': float(item[5])
                }
                if validate_candle(candle):
                    validated.append(candle)
                else:
                    logger.warning(f"Skipping invalid historical candle for {symbol}/{interval}: {candle}")
            data.extend(validated)
            if len(candles) < 1000:
                break
            start_time = int(candles[-1][0]) + 1  # Paging to avoid gaps
        except Exception as e:
            logger.error(f"Exception fetching historical data for {symbol}/{interval}: {e}")
            time.sleep(1)
    # Remove duplicates and sort
    unique_data = {c['timestamp']: c for c in data}
    sorted_data = sorted(unique_data.values(), key=lambda x: x['timestamp'])
    return sorted_data[-limit:]  # Latest limit

def fetch_historical_data():
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_map = {}
        for symbol in global_data.symbols:
            for interval in global_data.time_frames:
                future = executor.submit(get_historical_data, symbol, interval)
                future_map[future] = (symbol, interval)
        for future in concurrent.futures.as_completed(future_map):
            symbol, interval = future_map[future]
            try:
                data = future.result()
                if data:
                    with global_data.symbol_locks[symbol]:
                        global_data.candle_data[symbol][interval] = deque(data, maxlen=global_data.candle_limit)
                    logger.info(f"{symbol}/{interval}: {len(data)} historical candles fetched")
                else:
                    global_data.symbol_health[symbol] = global_data.symbol_health.get(symbol, 0) + 1
            except Exception as e:
                logger.error(f"{symbol}/{interval}: Error fetching historical data: {str(e)}")
                global_data.symbol_health[symbol] = global_data.symbol_health.get(symbol, 0) + 1
    elapsed = time.time() - start_time
    logger.info(f"Historical data fetched in {elapsed:.2f} seconds")

    # Post-fetch gap check
    for symbol in global_data.symbols:
        for interval in global_data.time_frames:
            candles = global_data.candle_data[symbol][interval]
            timestamps = [c['timestamp'] for c in candles]
            interval_ms = int(interval) * 60 * 1000
            for i in range(1, len(timestamps)):
                if timestamps[i] - timestamps[i-1] > interval_ms:
                    logger.warning(f"Gap detected in {symbol}/{interval}. Re-fetching.")
                    new_data = get_historical_data(symbol, interval)
                    global_data.candle_data[symbol][interval] = deque(new_data, maxlen=global_data.candle_limit)
                    break

class BybitWebSocketManager:
    def __init__(self):
        self.data_queue = asyncio.Queue()
        self.stop_event = asyncio.Event()
        self.reconnect_delay = 5
        self.last_message_time = time.time()
        self.heartbeat_interval = 20
        self.reconnect_attempts = 0

    async def hard_recovery(self):
        logger.warning("Initiating hard recovery...")
        global_data.run_strategy = False
        global_data.candle_data = {symbol: {interval: deque(maxlen=global_data.candle_limit) for interval in global_data.time_frames} for symbol in global_data.symbols}
        await self.stop()
        await self.start(global_data.symbols, global_data.time_frames)
        fetch_historical_data()
        logger.info("Hard recovery complete!")

    def _subscribe(self, ws, symbols, intervals):
        args = [f"kline.{interval}.{symbol}" for symbol in symbols for interval in intervals]
        for i in range(0, len(args), 500):
            ws.send(json.dumps({"op": "subscribe", "args": args[i:i+500]}))

    async def _send_ping(self, ws):
        while not self.stop_event.is_set():
            ws.send(json.dumps({"op": "ping"}))
            await asyncio.sleep(self.heartbeat_interval)

    def _process_kline(self, data):
        try:
            topic = data["topic"]
            interval = topic.split(".")[1]
            symbol = topic.split(".")[2]
            candles = data["data"]
            confirmed_candles = [c for c in candles if c.get('confirm', False)]
            if not confirmed_candles:
                return
            with global_data.symbol_locks[symbol]:
                if symbol in global_data.candle_data and interval in global_data.candle_data[symbol]:
                    for candle in confirmed_candles:
                        cleaned = {
                            'timestamp': int(candle['start']),
                            'open': float(candle['open']),
                            'high': float(candle['high']),
                            'low': float(candle['low']),
                            'close': float(candle['close']),
                            'volume': float(candle.get('volume', 0))
                        }
                        if validate_candle(cleaned):
                            candle_deque = global_data.candle_data[symbol][interval]
                            interval_ms = int(interval) * 60 * 1000
                            if candle_deque and cleaned['timestamp'] < candle_deque[-1]['timestamp'] - (3 * interval_ms):
                                logger.warning(f"Skipping stale WebSocket candle for {symbol}/{interval}: {convert_timestamp_to_readable(cleaned['timestamp'])}")
                                continue
                            add_candle_uniquely(candle_deque, cleaned, int(interval))
                        else:
                            logger.warning(f"Invalid live candle for {symbol}/{interval}: {cleaned}")
                else:
                    logger.warning(f"Unknown symbol/interval: {symbol}/{interval}")
        except Exception as e:
            logger.error(f"Error processing kline: {e}")

    def _on_message(self, ws, message):
        try:
            self.last_message_time = time.time()
            data = json.loads(message)
            if "topic" in data and "kline" in data["topic"]:
                self._process_kline(data)
            elif data.get('op') == 'pong':
                logger.debug("Pong received")
        except Exception as e:
            logger.error(f"Message processing error: {e}")

    async def _run_websocket(self, url, symbols, intervals):
        def on_open(ws):
            logger.info("WebSocket connected")
            global_data.ws_connected = True
            self.last_message_time = time.time()
            self.reconnect_delay = 5
            self._subscribe(ws, symbols, intervals)
            asyncio.create_task(self._send_ping(ws))

        while not self.stop_event.is_set():
            try:
                ws = websocket.WebSocketApp(
                    url,
                    on_open=on_open,
                    on_message=self._on_message,
                    on_error=lambda ws, e: logger.error(f"WebSocket error: {e}"),
                    on_close=lambda ws, s, m: logger.info(f"WebSocket closed: {s}, {m}")
                )
                ws.run_forever(ping_interval=0, ping_timeout=10)
                global_data.ws_connected = False
                logger.info("WebSocket disconnected, attempting reconnect")
                if not self.stop_event.is_set():
                    await asyncio.sleep(self.reconnect_delay)
                    self.reconnect_delay = min(self.reconnect_delay * 2, 60)
            except Exception as e:
                logger.error(f"WebSocket crashed: {e}")
                global_data.ws_connected = False
                await asyncio.sleep(self.reconnect_delay)
                self.reconnect_delay = min(self.reconnect_delay * 2, 60)

    async def start(self, symbols, intervals):
        self.stop_event.clear()
        self.symbols = symbols
        self.intervals = intervals
        await self._run_websocket("wss://stream.bybit.com/v5/public/linear", symbols, intervals)

    async def stop(self):
        self.stop_event.set()
        global_data.ws_connected = False
        global_data.run_strategy = False
        logger.info("WebSocket manager stopped")