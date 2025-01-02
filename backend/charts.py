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
    plot_dt (pd.plot_dtFrame): plot_dtFrame containing 'Close' price plot_dt.

    Returns:
    pd.plot_dtFrame: plot_dtFrame with added columns for SMA, EMA, MACD, RSI,
    and corresponding signals.
    """
    # Calculate 20-day and 50-day Simple Moving Averages (SMA) on Close price
    plot_dt['SMA_20'] = SMAIndicator(
        close=plot_dt['Close'], window=20).sma_indicator()
    plot_dt['SMA_50'] = SMAIndicator(
        close=plot_dt['Close'], window=50).sma_indicator()

    # 12-day and 26-day Exponential Moving Averages (EMA) on Close price
    plot_dt['EMA_12'] = EMAIndicator(
        close=plot_dt['Close'], window=12).ema_indicator()
    plot_dt['EMA_26'] = EMAIndicator(
        close=plot_dt['Close'], window=26).ema_indicator()

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
    macd = MACD(close=plot_dt['Close'], window_slow=26,
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
    rsi = RSIIndicator(close=plot_dt['Close'], window=14)
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
    plot_dt['Decision'] = 0  # hold
    for i in plot_dt.index[1:]:
        if (plot_dt.loc[i, 'SMA_Signal']
            + plot_dt.loc[i, 'MACD_Signal']
                + plot_dt.loc[i, 'RSI_Signal']) >= 2:
            plot_dt.loc[i, 'Decision'] = 1  # buy
        elif (plot_dt.loc[i, 'SMA_Signal']
              + plot_dt.loc[i, 'MACD_Signal']
              + plot_dt.loc[i, 'RSI_Signal']) <= -2:
            plot_dt.loc[i, 'Decision'] = 2  # sell
        else:
            plot_dt.loc[i, 'Decision'] = 0
    return plot_dt


def plot_candlestick_with_decisions(go_df):
    """
    Generates a candlestick chart with buy and sell trading decisions.

    Parameters:
    go_df (pd.plot_dtFrame): plot_dtFrame containing 'TradingDate', 'Open',
        'High', 'Low', 'Close', and 'Decision' columns.
        The 'Decision' column should contain values for trading decisions:
              1 for Buy, 2 for Sell, and 0 for Hold.

    Returns:
    the candlestick chart with buy and sell markers.
    """
    index_as_x = go_df.index
    # Create a candlestick chart
    fig = go.Figure(data=[go.Candlestick(x=index_as_x,
                                         open=go_df['Open'],
                                         high=go_df['High'],
                                         low=go_df['Low'],
                                         close=go_df['Close'],
                                         name='Candlestick'),
                          go.Scatter(x=index_as_x[go_df['Decision'] == 1],
                                     y=go_df['Close'][go_df['Decision'] == 1],
                                     mode='markers',
                                     marker=dict(color='green', size=10),
                                     name='Buy Decision'),
                          go.Scatter(x=index_as_x[go_df['Decision'] == 2],
                                     y=go_df['Close'][go_df['Decision'] == 2],
                                     mode='markers',
                                     marker=dict(color='red', size=10),
                                     name='Sell Decision')])

    # Update layout for better visualization
    fig.update_layout(title='Candlestick Chart with Trading Decisions',
                      xaxis_title='TradingDate',
                      yaxis_title='Price',
                      xaxis_rangeslider_visible=False)

    return fig


def plot_trading_signal(plot_dt):
    plot_dt = get_indicator_signals(plot_dt)
    plot_dt = make_decision(plot_dt)
    return plot_candlestick_with_decisions(plot_dt)
