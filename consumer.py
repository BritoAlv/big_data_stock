import json
import pandas as pd
import streamlit as st
from kafka import KafkaConsumer
import altair as alt

KAFKA_TOPIC = "stock_prices"
KAFKA_SERVER = "localhost:9092"

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
)

st.title("Real-time Cryptocurrency Prices")

# Create placeholders for category-based charts
open_placeholder = st.empty()
high_placeholder = st.empty()
low_placeholder = st.empty()
close_placeholder = st.empty()
volume_placeholder = st.empty()

# Store received data
data = []

# Function to create a chart with multiple cryptocurrencies
def create_chart(df, column, title):
    chart = (
        alt.Chart(df)
        .mark_line()
        .encode(
            x="timestamp:T",
            y=alt.Y(column, title=title),
            color="symbol:N",  # Differentiate cryptos by color
            tooltip=["timestamp:T", "symbol:N", column],
        )
        .properties(width=700, height=300)
    ).interactive()
    return chart

# Stream data
for message in consumer:
    stock = message.value  # Deserialize message
    print(stock)  # Debugging

    # Append new stock data
    data.append(stock)

    # Convert list of dicts to DataFrame
    df = pd.DataFrame(data)

    # Ensure correct datetime format and sorting
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")

    # Create charts where each plot contains all cryptocurrencies
    open_chart = create_chart(df, "open", "Open Prices")
    high_chart = create_chart(df, "high", "High Prices")
    low_chart = create_chart(df, "low", "Low Prices")
    close_chart = create_chart(df, "close", "Close Prices")
    volume_chart = create_chart(df, "volume", "Trading Volume")

    # Update placeholders with new charts
    open_placeholder.altair_chart(open_chart, use_container_width=True)
    high_placeholder.altair_chart(high_chart, use_container_width=True)
    low_placeholder.altair_chart(low_chart, use_container_width=True)
    close_placeholder.altair_chart(close_chart, use_container_width=True)
    volume_placeholder.altair_chart(volume_chart, use_container_width=True)