# Based on : https://www.youtube.com/watch?v=qLkY_IeYfeU&t=16s
# This code tries to load 1 day interval data for Reliance stock and try to define a buy and sell strategy.
# Note: Based on the plot, it looks like this strategy can not be applied to all stocks. But can be used as valueable indicator.

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import talib

RSI_OB = 60
RSI_OS = 40
ADX_LEVEL = 20

# Define the length (as per the Pine Script code)
DI_L = 14  # DI Length
ADX_L = 14  # ADX Length

# Set the start and end dates
start_date = "2023-01-01"
end_date = "2025-01-01"

# Load your data
data_path = "./NIFTY_50_2015_01_01_2025_01_01/RELIANCE_data.csv"
df = pd.read_csv(data_path, parse_dates=["Date"])

# Compute DI+ and DI- using TA-Lib
df["plus_di"] = talib.PLUS_DI(df["High"], df["Low"], df["Close"], timeperiod=DI_L)
df["minus_di"] = talib.MINUS_DI(df["High"], df["Low"], df["Close"], timeperiod=DI_L)

# Compute ADX using TA-Lib
df["adx"] = talib.ADX(df["High"], df["Low"], df["Close"], timeperiod=ADX_L)

# Filter data between start and end dates
df = df[(df["Date"] >= start_date) & (df["Date"] <= end_date)]

# Ensure datetime index and column format
df["Date"] = pd.to_datetime(df["Date"])

# Create numeric index for x-axis (no gaps for weekends)
df["Index"] = range(len(df))

# ✅ Compute RSI using TA-Lib (default period is 14)
df["RSI"] = talib.RSI(df["Close"], timeperiod=14)

# Shifted RSI to compare with previous value
df["RSI_prev"] = df["RSI"].shift(1)

# Detect cross above 50
df["RSI_cross_up"] = (df["RSI_prev"] < 50) & (df["RSI"] >= 50)

# Detect cross below 50
df["RSI_cross_down"] = (df["RSI_prev"] > 50) & (df["RSI"] <= 50)

# Add EMA 50 days
df["EMA_50"] = talib.EMA(df["Close"], timeperiod=50)

# Buy signals
df["buy"] = (
    (df["RSI"] < RSI_OS) & (df["Close"] < df["EMA_50"]) & (df["adx"] > ADX_LEVEL)
)

# Sell signals
df["sell"] = df["RSI_cross_up"]

# Create the figure and axes
fig, (ax_candle, ax_vol, ax_rsi, ax_di_adx) = plt.subplots(
    4, 1, figsize=(20, 10), sharex=True, gridspec_kw={"height_ratios": [3, 1, 1, 1]}
)
width = 0.6

# Plot candles
for idx, row in df.iterrows():
    color = "green" if row["Close"] >= row["Open"] else "red"
    # Body
    ax_candle.bar(
        row["Index"],
        row["Close"] - row["Open"],
        bottom=row["Open"],
        color=color,
        width=width,
        edgecolor="black",
        linewidth=0.1,
    )
    # Wick
    ax_candle.vlines(
        row["Index"], row["Low"], row["High"], color="black", linewidth=0.1
    )

# Volume plot
volume_colors = [
    "green" if row["Close"] >= row["Open"] else "red" for _, row in df.iterrows()
]
ax_vol.bar(df["Index"], df["Volume"], color=volume_colors, width=width)


# ✅ Plot RSI
ax_rsi.plot(df["Index"], df["RSI"], label="RSI", color="blue", linewidth=1)
ax_rsi.axhline(RSI_OB, color="red", linestyle="--", linewidth=0.8)
ax_rsi.axhline(RSI_OS, color="green", linestyle="--", linewidth=0.8)

# Buy signals
buy_df = df[df["buy"]]
ax_candle.plot(
    buy_df["Index"],
    buy_df["Low"],
    marker="^",
    color="green",
    linestyle=None,
    label="Buy Signal",
    markersize=1,
)  # green dot

# Sell signals
sell_df = df[df["sell"]]
ax_candle.plot(
    sell_df["Index"],
    sell_df["Low"],
    marker="^",
    color="red",
    linestyle=None,
    label="Sell Signal",
    markersize=1,
)  # red dot

ax_rsi.set_ylabel("RSI")
ax_rsi.set_ylim(0, 100)
ax_rsi.grid(True, linestyle="--", alpha=0.5)
ax_rsi.legend(loc="upper left")

# Plot DI and ADX
ax_di_adx.plot(
    df["Index"][DI_L - 1 :],
    df["plus_di"][DI_L - 1 :],
    label="+DI",
    color="green",
    linewidth=1,
)
ax_di_adx.plot(
    df["Index"][DI_L - 1 :],
    df["minus_di"][DI_L - 1 :],
    label="-DI",
    color="red",
    linewidth=1,
)
ax_di_adx.plot(
    df["Index"][ADX_L - 1 :],
    df["adx"][ADX_L - 1 :],
    label="ADX",
    color="black",
    linewidth=1,
)

# Set the y-axis limits for DI/ADX plot
ax_di_adx.set_ylim(0, 100)
ax_di_adx.set_ylabel("DI / ADX")
ax_di_adx.grid(True, linestyle="--", alpha=0.5)

# Add legend to DI/ADX plot
ax_di_adx.legend()

# Format the x-axis
ax_candle.set_ylabel("Price")
ax_vol.set_ylabel("Volume")
ax_candle.set_title("Candlestick, Volume and RSI Chart")

# Set custom x-axis ticks with dates
step = max(1, len(df) // 10)  # show ~10 ticks
ax_vol.set_xticks(df["Index"][::step])
ax_vol.set_xticklabels(df["Date"].dt.strftime("%Y-%m-%d")[::step], rotation=45)

# Add gridlines
ax_candle.grid(True, which="major", linestyle="--", alpha=0.5)
ax_vol.grid(True, which="major", linestyle="--", alpha=0.5)

# Save the figure
plt.tight_layout()
plt.savefig("candlestick_volume_chart.png", dpi=600)
plt.show()
