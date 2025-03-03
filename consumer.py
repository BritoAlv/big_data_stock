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

# Create figure
fig = plt.figure(figsize=(12, 10))

def animate(i):
    if df.empty:
        return

    fig.clf()  # Clear the figure to avoid overlapping plots

    cryptos = df["symbol"].unique()[:3]  # Limit to first 3 cryptos

    for idx, crypto in enumerate(cryptos):
        df_crypto = df[df["symbol"] == crypto].copy().sort_values("timestamp")

        if len(df_crypto) < 10:
            continue  # Skip if not enough data for moving averages

        df_crypto["SMA_5"] = df_crypto["close"].rolling(window=5).mean()
        df_crypto["SMA_10"] = df_crypto["close"].rolling(window=10).mean()
        df_crypto.set_index("timestamp", inplace=True)

        # Create subplot for each crypto
        ax_main = fig.add_subplot(len(cryptos), 1, idx + 1)
        ax_main.set_title(crypto)

        # Plot candlestick chart
        mpf.plot(
            df_crypto,
            type="candle",
            ax=ax_main,
            show_nontrading=True
        )

        # Plot moving averages manually with labels for legend
        ax_main.plot(df_crypto.index, df_crypto["SMA_5"], label="SMA 5", color="blue", linewidth=1.5)
        ax_main.plot(df_crypto.index, df_crypto["SMA_10"], label="SMA 10", color="orange", linewidth=1.5)
        ax_main.scatter(df_crypto.index, df_crypto["high"], label="High Prices", color="green", marker="^", alpha=0.6)
        ax_main.scatter(df_crypto.index, df_crypto["low"], label="Low Prices", color="red", marker="v", alpha=0.6)


# Add legend to each subplot
        ax_main.legend(loc="upper left")

    fig.tight_layout()

# Set up animation
ani = FuncAnimation(fig, animate, interval=1000)  # Update every second
plt.show()
