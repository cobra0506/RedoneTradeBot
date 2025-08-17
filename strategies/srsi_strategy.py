from indicators import calc_stoch_rsi, calc_sma, calc_adx
from risk import get_position_size, set_sl_tp
from global_data import config, current_balance, time_frames, positions
from utils import logger

class SRSIStrategy:
    def analyze(self, market_data, mode):
        symbol = market_data['symbol']
        candles_by_tf = market_data['candles_by_tf']
        buy_thresh = config['strategies']['srsi']['buy_threshold']
        sell_thresh = config['strategies']['srsi']['sell_threshold']
        use_cross = config['strategies']['srsi']['use_cross']
        use_trend = config['strategies']['srsi']['use_trend_filter']
        
        stoch_k = {}
        for tf in time_frames:
            candles = candles_by_tf.get(tf, [])
            if len(candles) < 20:
                logger.warning(f"Not enough {tf}m candles for {symbol}")
                return 'HOLD'
            k_series = calc_stoch_rsi(candles, return_series=True)
            if k_series is None or len(k_series) < 2:
                return 'HOLD'
            prev_k = k_series[-2]
            curr_k = k_series[-1]
            stoch_k[tf] = {'prev': prev_k, 'curr': curr_k}
        
        current_position = positions.get(symbol, {}).get('side')
        current_price = candles_by_tf['1'][-1]['close'] if '1' in candles_by_tf else None
        if not current_price:
            return 'HOLD'
        
        # Trend filter if enabled
        trend_bullish = False
        trend_bearish = False
        if use_trend:
            candles_15 = candles_by_tf.get('15', [])
            if len(candles_15) < 21:
                return 'HOLD'
            fast_sma = calc_sma(candles_15, 9)
            slow_sma = calc_sma(candles_15, 21)
            adx = calc_adx(candles_15)
            if fast_sma and slow_sma and adx:
                trend_bullish = fast_sma > slow_sma and adx > 0
                trend_bearish = fast_sma < slow_sma and adx > 0
        
        # Open Long
        if all(stoch_k[tf]['curr'] < buy_thresh and (not use_cross or stoch_k[tf]['curr'] > stoch_k[tf]['prev']) for tf in time_frames):
            if current_position == 'long':
                return 'HOLD'
            if use_trend and not trend_bullish:
                return 'HOLD'
            if len(positions) >= config['defaults']['max_positions']:
                return 'HOLD'
            amount = get_position_size(method='percent', value=1, symbol_price=current_price)
            sl, tp = set_sl_tp(current_price, 'long', method='atr', candles=candles_by_tf.get('15', []))
            return {'signal': 'OPEN_LONG', 'amount': amount, 'sl': sl, 'tp': tp}
        
        # Close Long
        if current_position == 'long' and (stoch_k['15']['curr'] > sell_thresh or (use_trend and trend_bearish)):
            return 'CLOSE_LONG'
        
        # Open Short (inverse)
        if all(stoch_k[tf]['curr'] > sell_thresh and (not use_cross or stoch_k[tf]['curr'] < stoch_k[tf]['prev']) for tf in time_frames):
            if current_position == 'short':
                return 'HOLD'
            if use_trend and not trend_bearish:
                return 'HOLD'
            if len(positions) >= config['defaults']['max_positions']:
                return 'HOLD'
            amount = get_position_size(method='percent', value=1, symbol_price=current_price)
            sl, tp = set_sl_tp(current_price, 'short', method='atr', candles=candles_by_tf.get('15', []))
            return {'signal': 'OPEN_SHORT', 'amount': amount, 'sl': sl, 'tp': tp}
        
        # Close Short
        if current_position == 'short' and (stoch_k['15']['curr'] < buy_thresh or (use_trend and trend_bullish)):
            return 'CLOSE_SHORT'
        
        return 'HOLD'