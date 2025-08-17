from indicators import calc_sma, calc_adx, calc_atr
from risk import get_position_size, set_sl_tp
from global_data import config, current_balance, positions
from utils import logger

class GridStrategy:
    def analyze(self, market_data, mode):
        symbol = market_data['symbol']
        candles = market_data['candles_by_tf'].get('15', [])  # Use 15m for trend/volatility (configurable if needed)
        if len(candles) < 21:
            logger.warning(f"Not enough 15m candles for {symbol} in GridStrategy")
            return 'HOLD'
        
        # Params from config (testable/editable)
        num_levels = config['strategies']['grid']['num_levels']
        spacing_pct = config['strategies']['grid']['spacing_pct']
        breakout_offset_pct = config['strategies']['grid']['breakout_offset_pct']
        
        current_price = candles[-1]['close']
        sma = calc_sma(candles, 21)  # Slow SMA for trend
        adx = calc_adx(candles)  # Trend strength (volatility factor)
        atr = calc_atr(candles)  # Range for grid sizing
        if not sma or not adx or not atr:
            logger.warning(f"Indicator calculation failed for {symbol} in GridStrategy")
            return 'HOLD'
        
        # Trend determination
        trend = 'up' if current_price > sma else 'down' if current_price < sma else 'flat'
        volatility_score = atr * adx  # Combined for selection in runner (not here)
        
        # Pause on flat: Close if open, else hold (reselect happens in runner)
        if trend == 'flat':
            if symbol in positions:
                return 'CLOSE_' + positions[symbol]['side'].upper()
            return 'HOLD'
        
        # Enforce one position max: Ignore new signals if already open
        current_position = positions.get(symbol, {}).get('side')
        if current_position:
            logger.info(f"Ignoring signal for {symbol} in GridStrategy - position already open ({current_position})")
            return 'HOLD'
        
        # Generate dynamic grid levels based on ATR and spacing
        grid_size = atr * (spacing_pct / 100)  # Dynamic spacing
        if trend == 'up':
            low_grid = current_price - (num_levels / 2) * grid_size
            high_grid = current_price + (num_levels / 2) * grid_size
            # Buy on low grid hit
            if current_price <= low_grid + grid_size:  # Near low end
                amount = get_position_size(method='percent', value=1, symbol_price=current_price)
                sl, tp = set_sl_tp(current_price, 'long', method='atr', mult=2, candles=candles)
                logger.info(f"Grid buy long signal for {symbol} at {current_price}")
                return {'signal': 'OPEN_LONG', 'amount': amount, 'sl': sl, 'tp': tp}
            # Sell on high grid hit (if open)
            if current_position == 'long' and current_price >= high_grid - grid_size:
                return 'CLOSE_LONG'
            # Breakout buy above high grid
            if current_price > high_grid + (breakout_offset_pct / 100 * current_price):
                amount = get_position_size(method='percent', value=1, symbol_price=current_price)
                sl, tp = set_sl_tp(current_price, 'long', method='atr', mult=2, candles=candles)
                logger.info(f"Breakout buy long signal for {symbol} at {current_price}")
                return {'signal': 'OPEN_LONG', 'amount': amount, 'sl': sl, 'tp': tp}
        elif trend == 'down':
            high_grid = current_price + (num_levels / 2) * grid_size
            low_grid = current_price - (num_levels / 2) * grid_size
            # Sell on high grid hit
            if current_price >= high_grid - grid_size:  # Near high end
                amount = get_position_size(method='percent', value=1, symbol_price=current_price)
                sl, tp = set_sl_tp(current_price, 'short', method='atr', mult=2, candles=candles)
                logger.info(f"Grid sell short signal for {symbol} at {current_price}")
                return {'signal': 'OPEN_SHORT', 'amount': amount, 'sl': sl, 'tp': tp}
            # Buy on low grid hit (if open)
            if current_position == 'short' and current_price <= low_grid + grid_size:
                return 'CLOSE_SHORT'
            # Breakout sell below low grid (inverse breakout)
            if current_price < low_grid - (breakout_offset_pct / 100 * current_price):
                amount = get_position_size(method='percent', value=1, symbol_price=current_price)
                sl, tp = set_sl_tp(current_price, 'short', method='atr', mult=2, candles=candles)
                logger.info(f"Breakout sell short signal for {symbol} at {current_price}")
                return {'signal': 'OPEN_SHORT', 'amount': amount, 'sl': sl, 'tp': tp}
        
        return 'HOLD'