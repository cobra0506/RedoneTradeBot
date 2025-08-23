from typing import Optional, Dict, Any

from pybit.unified_trading import HTTP
from requests import post, get as requests_get

from utils import logger, log_opened_position
import global_data
from global_data import config, demo, current_balance, balance_offset, mode

# Global session + flag
_session: Optional[HTTP] = None
_is_raw: bool = False


def _raw_request(method: str, endpoint: str, params: Dict[str, Any] = None, payload: Dict[str, Any] = None):
    url = f'https://api.bybit.com{endpoint}'
    headers = {
        'X-BAPI-API-KEY': config['api']['real_key'] if not global_data.demo else config['api']['demo_key'],
        'X-BAPI-RECV-WINDOW': '5000'
    }
    if method == 'POST':
        response = post(url, json=payload, headers=headers, params=params)
    else:
        response = requests_get(url, headers=headers, params=params)
    return response.json()


def initialize_connection():
    global _session, _is_raw
    api_key = config['api']['demo_key'] if global_data.demo else config['api']['real_key']
    api_secret = config['api']['demo_secret'] if global_data.demo else config['api']['real_secret']
    try:
        _session = HTTP(testnet=False, demo=global_data.demo, api_key=api_key, api_secret=api_secret, recv_window=5000)
        _session.get_server_time()  # Test connection
        _is_raw = False
        logger.info("Bybit connection established with pybit")
    except Exception as e:
        logger.error(f"Pybit init failed: {e}. Switching to raw HTTP fallback.")
        _session = None
        _is_raw = True
    return _session


def get_account_balance():
    global current_balance, balance_offset
    if mode == 'backtest':
        return current_balance

    if _session is None and not _is_raw:
        initialize_connection()
    try:
        if not _is_raw:
            balance_data = _session.get_wallet_balance(accountType="UNIFIED")
        else:
            balance_data = _raw_request('GET', '/v5/account/wallet-balance', params={"accountType": "UNIFIED"})

        if balance_data.get("retCode") == 0:
            live_balance = float(balance_data["result"]["list"][0]["totalWalletBalance"])
            if global_data.demo or mode == 'paper':
                if balance_offset == 0:
                    balance_offset = live_balance - config['defaults']['start_balance']
                adjusted = live_balance - balance_offset
                if adjusted < 0:
                    balance_offset = live_balance - config['defaults']['start_balance']
                    adjusted = live_balance - balance_offset
                current_balance = adjusted
                logger.info(f"[Paper] Adjusted balance: {adjusted:.2f}")
                return adjusted
            else:
                current_balance = live_balance
                logger.info(f"Live balance: {live_balance:.2f}")
                return live_balance
        else:
            logger.error(f"Balance fetch failed: {balance_data.get('retMsg')}")
            return 0.0
    except Exception as e:
        logger.error(f"Balance error: {e}")
        return 0.0


def adjust_leverage(symbol, new_leverage):
    if mode == 'backtest':
        global_data.Leverage_amounts = getattr(global_data, "Leverage_amounts", {})
        global_data.Leverage_amounts[symbol] = new_leverage
        return True

    if _session is None and not _is_raw:
        initialize_connection()
    try:
        payload = {"category": "linear", "symbol": symbol,
                   "buyLeverage": str(new_leverage), "sellLeverage": str(new_leverage)}

        if not _is_raw:
            response = _session.set_leverage(**payload)
        else:
            response = _raw_request('POST', '/v5/position/set-leverage', payload=payload)

        if response.get('retCode') == 0:
            logger.info(f"Leverage for {symbol} adjusted to {new_leverage}x")
            return True
        logger.error(f"Failed to adjust leverage: {response.get('retMsg')}")
        return False
    except Exception as e:
        logger.error(f"Error adjusting leverage: {str(e)}")
        return False


def get_symbol_leverage(symbol):
    if mode == 'backtest':
        return getattr(global_data, "Leverage_amounts", {}).get(symbol, 10)

    if _session is None and not _is_raw:
        initialize_connection()
    try:
        params = {"category": "linear", "symbol": symbol}
        if not _is_raw:
            position = _session.get_positions(**params)
        else:
            position = _raw_request('GET', '/v5/position/list', params=params)

        if position.get('retCode') == 0 and position['result']['list']:
            leverage = float(position['result']['list'][0]['leverage'])
            logger.info(f"Current leverage for {symbol}: {leverage}x")
            return leverage
        logger.info(f"No position found for {symbol}")
        return None
    except Exception as e:
        logger.error(f"Failed to get leverage: {str(e)}")
        return None


def get_market_price(symbol, category="linear"):
    if mode == 'backtest':
        return global_data.candle_data.get(symbol, {}).get('1', [{}])[-1].get('close', 0) or 0

    if _session is None and not _is_raw:
        initialize_connection()
    try:
        params = {"category": category, "symbol": symbol}
        if not _is_raw:
            ticker = _session.get_tickers(**params)
        else:
            ticker = _raw_request('GET', '/v5/market/tickers', params=params)
        if ticker.get('retCode') == 0 and ticker['result']['list']:
            price = float(ticker['result']['list'][0]['lastPrice'])
            logger.info(f"Current {symbol} price: {price}")
            return price
        logger.info(f"No price data found for {symbol}")
        return None
    except Exception as e:
        logger.error(f"Failed to get price: {str(e)}")
        return None


def get_position_info(symbol):
    if mode == 'backtest':
        return global_data.positions.get(symbol, {
            'status': "closed", 'side': None, 'size': 0.0, 'entry_price': 0.0, 'leverage': 0.0, 'pnl': 0.0
        })

    if _session is None and not _is_raw:
        initialize_connection()
    try:
        params = {"category": "linear", "symbol": symbol}
        if not _is_raw:
            position = _session.get_positions(**params)
        else:
            position = _raw_request('GET', '/v5/position/list', params=params)
        if position.get('retCode') == 0 and position['result']['list']:
            pos_data = position['result']['list'][0]
            size = float(pos_data['size'])
            if size > 0:
                return {
                    'status': "open",
                    'side': "long" if pos_data['side'] == 'Buy' else "short",
                    'size': size,
                    'entry_price': float(pos_data['avgPrice']),
                    'leverage': float(pos_data['leverage']),
                    'pnl': float(pos_data['unrealisedPnl'])
                }
        return {'status': "closed", 'side': None, 'size': 0.0, 'entry_price': 0.0, 'leverage': 0.0, 'pnl': 0.0}
    except Exception as e:
        logger.error(f"Error getting position info: {str(e)}")
        return None


def place_smart_order(symbol, side, amount_usd, leverage=None, tp=None, sl=None):
    if mode == 'backtest':
        logger.info(f"Simulating order for {symbol} in backtest mode")
        return True  # simulated in backtester

    if _session is None and not _is_raw:
        initialize_connection()
    try:
        if leverage is not None:
            adjust_leverage(symbol, leverage)

        position = get_position_info(symbol)
        desired_side = "long" if side == 'Buy' else "short"
        if position and position.get('status') == 'open':
            if position['side'] == desired_side:
                logger.info(f"Position already exists: {position['side']} {position['size']} {symbol}. No action.")
                return True
            else:
                logger.info(f"Closing opposite position: {position['side']} {position['size']} {symbol}")
                close_position(symbol, position)

        price = get_market_price(symbol)
        if price is None:
            logger.error(f"Could not retrieve market price for {symbol}")
            return False

        # symbol details for qty rounding
        params = {"category": "linear", "symbol": symbol}
        if not _is_raw:
            symbol_info = _session.get_instruments_info(**params)
        else:
            symbol_info = _raw_request('GET', '/v5/market/instruments-info', params=params)

        qty_step = float(symbol_info['result']['list'][0]['lotSizeFilter']['qtyStep'])
        min_order_value = 5
        quantity = max(amount_usd / price, min_order_value / price)
        rounded_qty = round(quantity / qty_step) * qty_step
        rounded_qty = float(f"{rounded_qty:.8f}".rstrip('0').rstrip('.'))

        logger.info(f"Market order: {side} {rounded_qty} {symbol} @ ~{price}")

        # SL/TP fallbacks (2.5%/5%)
        if sl is None:
            sl = price * (1 - 0.025) if side == "Buy" else price * (1 + 0.025)
        if tp is None:
            tp = price * (1 + 0.05) if side == "Buy" else price * (1 - 0.05)

        # Validate basic SL/TP
        if side == "Buy" and (sl >= price or tp <= price):
            logger.error(f"Invalid SL/TP for buy: sl={sl}, tp={tp}, price={price}")
            return False
        if side == "Sell" and (sl <= price or tp >= price):
            logger.error(f"Invalid SL/TP for sell: sl={sl}, tp={tp}, price={price}")
            return False

        payload = {
            "category": "linear",
            "symbol": symbol,
            "side": side,
            "orderType": "Market",
            "qty": str(rounded_qty),
            "timeInForce": "GTC",
            "reduceOnly": False,
            "stopLoss": str(sl),
            "takeProfit": str(tp)
        }
        if not _is_raw:
            order = _session.place_order(**payload)
        else:
            order = _raw_request('POST', '/v5/order/create', payload=payload)

        if order.get('retCode') == 0:
            logger.info(f"Order executed: {order['result']['orderId']}")
            action = "open_long" if side == "Buy" else "open_short"
            log_opened_position(symbol, action, price, amount_usd)
            return True
        logger.error(f"Order failed: {order.get('retMsg')}")
        return False
    except Exception as e:
        logger.error(f"Order error: {str(e)}")
        return False


def close_position(symbol, position=None):
    if mode == 'backtest':
        logger.info(f"Simulating close for {symbol} in backtest mode")
        return True

    if _session is None and not _is_raw:
        initialize_connection()

    if position is None:
        position = get_position_info(symbol)
    if not position or position.get('status') != 'open':
        return True

    close_side = "Sell" if position['side'] == 'long' else "Buy"
    payload = {
        "category": "linear",
        "symbol": symbol,
        "side": close_side,
        "orderType": "Market",
        "qty": str(position['size']),
        "timeInForce": "GTC",
        "reduceOnly": True
    }
    try:
        if not _is_raw:
            response = _session.place_order(**payload)
        else:
            response = _raw_request('POST', '/v5/order/create', payload=payload)
        if response.get('retCode') == 0:
            logger.info(f"Closed {position['size']} {symbol} {position['side']} position")
            action = f"closed_{position['side']}"
            price = get_market_price(symbol) or position['entry_price']
            log_opened_position(symbol, action, price, position['size'] * price)
            return True
        logger.error(f"Failed to close position: {response.get('retMsg')}")
        return False
    except Exception as e:
        logger.error(f"Error closing position: {str(e)}")
        return False


# convenience wrappers
def handle_buy_signal(symbol, amount_usd=100, leverage=None, tp=None, sl=None):
    logger.info(f"Opening long position for {symbol}")
    return place_smart_order(symbol, "Buy", amount_usd, leverage, tp, sl)


def handle_sell_signal(symbol, amount_usd=100, leverage=None, tp=None, sl=None):
    logger.info(f"Opening short position for {symbol}")
    return place_smart_order(symbol, "Sell", amount_usd, leverage, tp, sl)
