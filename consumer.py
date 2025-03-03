import json
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
from kafka import KafkaConsumer
from threading import Thread
from matplotlib.animation import FuncAnimation

KAFKA_TOPIC = "stock_prices"
KAFKA_SERVER = "localhost:9092"

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

# Data storage
df = pd.DataFrame(columns=["timestamp", "symbol", "open", "high", "low", "close", "volume"])


def consume_messages():
    global df
    for message in consumer:
        data = message.value
        new_data = pd.DataFrame([data])
        new_data["timestamp"] = pd.to_datetime(new_data["timestamp"])  # Ensure correct datetime format

        # Append to dataframe and keep only the latest N data points for performance
        df = pd.concat([df, new_data], ignore_index=True).tail(300)  # Keep recent data


# Start Kafka consumer in a separate thread
thread = Thread(target=consume_messages, daemon=True)
thread.start()


# Create figure and axes
fig, ax = plt.subplots(1, 2, figsize=(12, 15))  # Single axis for the plot
ax =  ax.flatten()

line_sma_5 = []
line_sma_10 = []
line_bollinger_upper = []
line_bollinger_lower = []
# Initialize plots
for i in range(0, 1):
    line_sma_5.append(ax[2*i].plot([], [], label="SMA 5", color="blue", linewidth=1.5))
    line_sma_10.append(ax[2*i].plot([], [], label="SMA 10", color="orange", linewidth=1.5))
    line_bollinger_upper.append(ax[2*i].plot([], [], label="Bollinger Upper", color="green", linestyle="dashed", linewidth=1.0))
    line_bollinger_lower.append(ax[2*i].plot([], [], label="Bollinger Lower", color="red", linestyle="dashed", linewidth=1.0))


def animate(i):
    if df.empty:
        return

    cryptos = df["symbol"].unique()[:1]  # Limit to first 3 cryptos

    for idx, crypto in enumerate(cryptos):
        df_crypto = df[df["symbol"] == crypto].copy().sort_values("timestamp")

        if len(df_crypto) < 10:
            continue  # Skip if not enough data for moving averages

        df_crypto["SMA_5"] = df_crypto["close"].rolling(window=5).mean()
        df_crypto["SMA_10"] = df_crypto["close"].rolling(window=10).mean()
        df_crypto["Bollinger_Upper"] = df_crypto["close"].rolling(window=20).mean() + 2 * df_crypto["close"].rolling(window=20).std()
        df_crypto["Bollinger_Lower"] = df_crypto["close"].rolling(window=20).mean() - 2 * df_crypto["close"].rolling(window=20).std()
        df_crypto.set_index("timestamp", inplace=True)

        # Update the lines with new data
        line_sma_5[idx][0].set_data(df_crypto.index, df_crypto["SMA_5"])
        line_sma_10[idx][0].set_data(df_crypto.index, df_crypto["SMA_10"])
        line_bollinger_upper[idx][0].set_data(df_crypto.index, df_crypto["Bollinger_Upper"])
        line_bollinger_lower[idx][0].set_data(df_crypto.index, df_crypto["Bollinger_Lower"])

        # Update the candlestick plot
        mpf.plot(df_crypto, ax=ax[2 * idx], type="candle", volume=ax[2 * idx + 1], style="starsandstripes")

    for i in range(1):
        ax[i].relim()
        ax[i].autoscale_view()
        ax[i].legend()


# Set up animation
ani = FuncAnimation(fig, animate, interval=1000)  # Update every second
plt.show()