import json
import time
import threading
from collections import deque
from typing import List, Dict, Any, Optional

import websocket
from pybit.unified_trading import HTTP
from requests import Session as RawSession, get as raw_get

from utils import logger, validate_candle, add_candle_uniquely, convert_timestamp_to_readable
import global_data


# ---------- Public REST / pybit helpers ----------

def get_client(demo=global_data.demo):
    key = global_data.config['api']['demo_key'] if demo else global_data.config['api']['real_key']
    secret = global_data.config['api']['demo_secret'] if demo else global_data.config['api']['real_secret']
    try:
        return HTTP(api_key=key, api_secret=secret, demo=demo)
    except Exception as e:
        logger.error(f"Pybit client failed: {e}. Falling back to raw API.")
        session = RawSession()
        session.headers.update({
            'X-BAPI-API-KEY': key,
            'X-BAPI-SIGN-TYPE': '2',
            'X-BAPI-RECV-WINDOW': '5000'
        })
        return session


def raw_get_instruments(params):
    url = 'https://api.bybit.com/v5/market/instruments-info'
    response = raw_get(url, params=params)
    return response.json()


def get_symbols() -> List[str]:
    client = get_client()
    excluded_symbols = ['USDC', 'USDE', 'USTC']
    all_symbols: List[str] = []
    cursor: Optional[str] = None

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
                item['symbol'] for item in items
                if not any(excl in item['symbol'] for excl in excluded_symbols)
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


def get_historical_data(symbol: str, interval: str,
                        limit: int = global_data.candle_limit,
                        start_time: Optional[int] = None,
                        end_time: Optional[int] = None) -> List[Dict[str, Any]]:
    client = get_client()
    data: List[Dict[str, Any]] = []

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

            validated: List[Dict[str, Any]] = []
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

            # page forward safely to avoid duplicates
            start_time = int(candles[-1][0]) + 1
        except Exception as e:
            logger.error(f"Exception fetching historical data for {symbol}/{interval}: {e}")
            time.sleep(1)

    # De-dup and sort; keep latest 'limit'
    unique_data = {c['timestamp']: c for c in data}
    sorted_data = sorted(unique_data.values(), key=lambda x: x['timestamp'])
    return sorted_data[-limit:]


def fetch_historical_data():
    start_time = time.time()
    # ThreadPool for speed; maintain per-symbol locks
    from concurrent.futures import ThreadPoolExecutor, as_completed

    with ThreadPoolExecutor(max_workers=20) as executor:
        future_map = {}
        for symbol in global_data.symbols:
            for interval in global_data.time_frames:
                future = executor.submit(get_historical_data, symbol, interval)
                future_map[future] = (symbol, interval)

        for future in as_completed(future_map):
            symbol, interval = future_map[future]
            try:
                data = future.result()
                if data:
                    with global_data.symbol_locks[symbol]:
                        global_data.candle_data[symbol][interval] = deque(
                            data, maxlen=global_data.candle_limit
                        )
                    logger.info(f"{symbol}/{interval}: {len(data)} historical candles fetched")
                else:
                    global_data.symbol_health[symbol] = global_data.symbol_health.get(symbol, 0) + 1
            except Exception as e:
                logger.error(f"{symbol}/{interval}: Error fetching historical data: {str(e)}")
                global_data.symbol_health[symbol] = global_data.symbol_health.get(symbol, 0) + 1

    elapsed = time.time() - start_time
    logger.info(f"Historical data fetched in {elapsed:.2f} seconds")

    # Post-fetch coarse gap check; if gap found, one more full refetch for that tf
    for symbol in global_data.symbols:
        for interval in global_data.time_frames:
            candles = global_data.candle_data[symbol][interval]
            if not candles:
                continue
            timestamps = [c['timestamp'] for c in candles]
            interval_ms = int(interval) * 60 * 1000
            for i in range(1, len(timestamps)):
                if timestamps[i] - timestamps[i - 1] > interval_ms:
                    logger.warning(f"Gap detected in {symbol}/{interval}. Re-fetching.")
                    new_data = get_historical_data(symbol, interval)
                    with global_data.symbol_locks[symbol]:
                        global_data.candle_data[symbol][interval] = deque(new_data, maxlen=global_data.candle_limit)
                    break


# ---------- WebSocket manager (threaded; no asyncio) ----------

class BybitWebSocketManager:
    """
    Thread-based WS manager. Safe to start/stop from any thread.
    Keeps last_message_time for external liveness checks.
    """
    def __init__(self):
        self.data_queue = []  # not used externally, kept for compatibility
        self.stop_event = threading.Event()
        self.last_message_time = time.time()
        self.heartbeat_interval = 20
        self.reconnect_delay = 5
        self.ws = None
        self._ws_thread: Optional[threading.Thread] = None
        self._ping_thread: Optional[threading.Thread] = None
        self.symbols: List[str] = []
        self.intervals: List[str] = []

    def _subscribe(self, ws, symbols, intervals):
        args = [f"kline.{interval}.{symbol}" for symbol in symbols for interval in intervals]
        for i in range(0, len(args), 500):
            payload = json.dumps({"op": "subscribe", "args": args[i:i + 500]})
            try:
                ws.send(payload)
                logger.debug(f"Sent subscription: {payload}")
            except Exception as e:
                logger.error(f"Subscription send failed: {e}")

    def _send_ping_loop(self):
        while not self.stop_event.is_set():
            try:
                if self.ws:
                    self.ws.send(json.dumps({"op": "ping"}))
                    logger.debug("Ping sent")
            except Exception as e:
                logger.warning(f"Ping error: {e}")
            time.sleep(self.heartbeat_interval)

    def _process_kline(self, data: Dict[str, Any]):
        try:
            topic = data.get("topic", "")
            if "kline" not in topic:
                return
            _, interval, symbol = topic.split(".")
            candles = data.get("data", [])
            confirmed_candles = [c for c in candles if c.get('confirm', False)]
            if not confirmed_candles:
                return

            # Ensure dicts exist
            if symbol not in global_data.candle_data:
                global_data.candle_data[symbol] = {tf: deque(maxlen=global_data.candle_limit) for tf in global_data.time_frames}
            if symbol not in global_data.symbol_locks:
                global_data.symbol_locks[symbol] = threading.Lock()
            if interval not in global_data.candle_data[symbol]:
                global_data.candle_data[symbol][interval] = deque(maxlen=global_data.candle_limit)

            with global_data.symbol_locks[symbol]:
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
                            logger.warning(
                                f"Skipping stale WS candle {symbol}/{interval}: {convert_timestamp_to_readable(cleaned['timestamp'])}"
                            )
                            continue
                        add_candle_uniquely(candle_deque, cleaned, int(interval))
                    else:
                        logger.warning(f"Invalid live candle {symbol}/{interval}: {cleaned}")
        except Exception as e:
            logger.error(f"Error processing kline: {e}")

    def _on_message(self, ws, message: str):
        try:
            logger.debug(f"Received message: {message}")
            self.last_message_time = time.time()
            data = json.loads(message)
            if "topic" in data and "kline" in data["topic"]:
                self._process_kline(data)
            elif data.get('op') == 'pong':
                logger.debug("Pong received")
        except Exception as e:
            logger.error(f"Message processing error: {e}")

    def _on_open(self, ws):
        logger.info("WebSocket connected successfully")
        global_data.ws_connected = True
        self.last_message_time = time.time()
        self.reconnect_delay = 5
        self._subscribe(ws, self.symbols, self.intervals)
        # Start ping thread
        self._ping_thread = threading.Thread(target=self._send_ping_loop, daemon=True)
        self._ping_thread.start()

    def _on_error(self, ws, e):
        logger.error(f"WebSocket error: {e}")

    def _on_close(self, ws, status_code, msg):
        logger.info(f"WebSocket closed: {status_code}, {msg}")
        global_data.ws_connected = False

    def _ws_forever(self):
        url = "wss://stream.bybit.com/v5/public/linear"
        while not self.stop_event.is_set():
            try:
                self.ws = websocket.WebSocketApp(
                    url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close
                )
                # built-in ping disabled; we run our own
                self.ws.run_forever(ping_interval=0, ping_timeout=10)
            except Exception as e:
                logger.error(f"WebSocket crashed: {e}")
            finally:
                global_data.ws_connected = False
                if self.stop_event.is_set():
                    break
                # backoff
                logger.info("WebSocket disconnected, attempting reconnect")
                time.sleep(self.reconnect_delay)
                self.reconnect_delay = min(self.reconnect_delay * 2, 60)

    # Public API (synchronous; spawns threads and returns immediately)
    def start(self, symbols: List[str], intervals: List[str]):
        self.stop_event.clear()
        self.symbols = symbols
        self.intervals = intervals
        if self._ws_thread and self._ws_thread.is_alive():
            return
        self._ws_thread = threading.Thread(target=self._ws_forever, daemon=True)
        self._ws_thread.start()

    def stop(self):
        self.stop_event.set()
        try:
            if self.ws:
                self.ws.close()
        except Exception:
            pass
        global_data.ws_connected = False
        global_data.run_strategy = False

    def hard_recovery(self):
        logger.warning("Initiating hard recovery...")
        global_data.run_strategy = False
        global_data.candle_data = {
            symbol: {interval: deque(maxlen=global_data.candle_limit) for interval in global_data.time_frames}
            for symbol in global_data.symbols
        }
        self.stop()
        # slight pause to ensure thread exit
        time.sleep(1.0)
        self.start(global_data.symbols, global_data.time_frames)
        fetch_historical_data()
        logger.info("Hard recovery complete!")
