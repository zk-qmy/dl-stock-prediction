from ta.trend import SMAIndicator, EMAIndicator, MACD
from ta.momentum import RSIIndicator
import plotly.graph_objects as go


def get_indicator_signals(data):
    plot_dt = data.copy()
    """
    This function calculates various technical indicators and
    generates trading signals
    for a given stock price plot_dtFrame.

    Parameters:
    plot_dt (pd.plot_dtFrame): plot_dtFrame containing 'close' price plot_dt.

    Returns:
    pd.plot_dtFrame: plot_dtFrame with added columns for SMA, EMA, MACD, RSI,
    and corresponding signals.
    """
    # Calculate 20-day and 50-day Simple Moving Averages (SMA) on close price
    plot_dt['SMA_20'] = SMAIndicator(
        close=plot_dt['close'], window=20).sma_indicator()
    plot_dt['SMA_50'] = SMAIndicator(
        close=plot_dt['close'], window=50).sma_indicator()

    # 12-day and 26-day Exponential Moving Averages (EMA) on close price
    plot_dt['EMA_12'] = EMAIndicator(
        close=plot_dt['close'], window=12).ema_indicator()
    plot_dt['EMA_26'] = EMAIndicator(
        close=plot_dt['close'], window=26).ema_indicator()

    # 3. Generate Trading Signals Based on SMA and EMA Crossovers
    plot_dt['SMA_Signal'] = 0
    plot_dt['EMA_Signal'] = 0

    # Generate SMA signals: 1 for Buy, -1 for Sell
    plot_dt.loc[(plot_dt['SMA_20'] > plot_dt['SMA_50']),
                'SMA_Signal'] = 1  # Buy Signal (Golden Cross)
    plot_dt.loc[(plot_dt['SMA_20'] < plot_dt['SMA_50']), 'SMA_Signal'] = - \
        1  # Sell Signal (Death Cross)

    # Generate EMA signals: 1 for Buy, -1 for Sell
    plot_dt.loc[(plot_dt['EMA_12'] > plot_dt['EMA_26']),
                'EMA_Signal'] = 1  # Buy Signal
    plot_dt.loc[(plot_dt['EMA_12'] < plot_dt['EMA_26']),
                'EMA_Signal'] = -1  # Sell Signal

    # 4. Calculate MACD and Signal Line Using ta Library
    macd = MACD(close=plot_dt['close'], window_slow=26,
                window_fast=12, window_sign=9)
    plot_dt['MACD'] = macd.macd()
    plot_dt['Signal_Line'] = macd.macd_signal()
    plot_dt['MACD_Histogram'] = macd.macd_diff()

    # Generate MACD signals based on crossovers
    plot_dt['MACD_Signal'] = 0
    plot_dt.loc[plot_dt['MACD'] > plot_dt['Signal_Line'],
                'MACD_Signal'] = 1  # Buy Signal
    plot_dt.loc[plot_dt['MACD'] < plot_dt['Signal_Line'],
                'MACD_Signal'] = -1  # Sell Signal

    # 5. Calculate Relative Strength Index (RSI)
    rsi = RSIIndicator(close=plot_dt['close'], window=14)
    plot_dt['RSI'] = rsi.rsi()

    # Generate Buy and Sell signals based on RSI levels
    plot_dt['RSI_Signal'] = 0
    plot_dt.loc[plot_dt['RSI'] > 70, 'RSI_Signal'] = - \
        1  # Sell Signal (Overbought)
    # Buy Signal (Oversold)
    plot_dt.loc[plot_dt['RSI'] < 30, 'RSI_Signal'] = 1
    plot_dt.dropna(inplace=True)
    return plot_dt


def make_decision(plot_dt):
    plot_dt['decision'] = 0  # Default to hold
    
    # Calculate the sum of signals
    signal_sum = plot_dt[['SMA_Signal', 'MACD_Signal', 'RSI_Signal']].sum(axis=1)

    # Assign buy, sell, and hold decisions
    plot_dt.loc[signal_sum >= 2, 'decision'] = 1  # Buy
    plot_dt.loc[signal_sum <= -2, 'decision'] = 2  # Sell

    return plot_dt


def plot_candlestick_with_decisions(go_df):
    """
    Generates a candlestick chart with buy and sell trading decisions.

    Parameters:
    go_df (pd.plot_dtFrame): plot_dtFrame containing 'time', 'open',
        'high', 'low', 'close', and 'decision' columns.
        The 'decision' column should contain values for trading decisions:
              1 for Buy, 2 for Sell, and 0 for Hold.

    Returns:
    the candlestick chart with buy and sell markers.
    """
    time = go_df.index
    # Create a candlestick chart
    fig = go.Figure(data=[go.Candlestick(x=time,
                                         open=go_df['open'],
                                         high=go_df['high'],
                                         low=go_df['low'],
                                         close=go_df['close'],
                                         name='Candlestick'),
                          go.Scatter(x=time[go_df['decision'] == 1],
                                     y=go_df['close'][go_df['decision'] == 1],
                                     mode='markers',
                                     marker=dict(color='green', size=10),
                                     name='Buy decision'),
                          go.Scatter(x=time[go_df['decision'] == 2],
                                     y=go_df['close'][go_df['decision'] == 2],
                                     mode='markers',
                                     marker=dict(color='red', size=10),
                                     name='Sell decision')])

    # Update layout for better visualization
    fig.update_layout(title='Candlestick Chart with Trading decisions',
                      xaxis_title='Time',
                      yaxis_title='Price',
                      xaxis_rangeslider_visible=False)

    return fig


def plot_trading_signal(plot_dt):
    plot_dt = get_indicator_signals(plot_dt)
    plot_dt = make_decision(plot_dt)
    return plot_candlestick_with_decisions(plot_dt)
