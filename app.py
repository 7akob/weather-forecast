import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

st.set_page_config(page_title='Weather Dashboard', layout='wide')
st.title('Real-Time Weather Dashboard')
st.caption('Helsinki | Live data via Kafka + Cassandra | Forecast via Random Forest')

# Load the processed data
@st.cache_data(ttl=600)
def load_data():
    df = pd.read_csv('processed_weather.csv')
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    return df

df = load_data()
latest = df.iloc[-1]

# Metric cards
col1, col2, col3, col4 = st.columns(4)
col1.metric('Current Temperature', f"{latest['temperature']:.1f} °C")
col2.metric('Wind Speed', f"{latest['windspeed']:.1f} km/h")
col3.metric('Weather Code', int(latest['weathercode']))
col4.metric('Total Records', len(df))

# Temperature over time
st.subheader('Temperature Over Time')
fig, ax = plt.subplots(figsize=(12, 4))
ax.plot(df['timestamp'], df['temperature'], color='steelblue', linewidth=1.5, label='Actual')
ax.plot(df['timestamp'], df['temp_rolling_avg'], color='orange', linewidth=1, linestyle='--', label='Rolling Average')
ax.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m %H:%M'))
plt.xticks(rotation=45)
ax.set_ylabel('Temperature (°C)')
ax.legend()
ax.grid(True, alpha=0.3)
st.pyplot(fig)

# Feature columns
st.subheader('Engineered Features')
col1, col2 = st.columns(2)

with col1:
    fig2, ax2 = plt.subplots(figsize=(6, 3))
    ax2.plot(df['timestamp'], df['temp_lag_1h'], color='green', linewidth=1)
    ax2.set_title('Lag Feature (temp 1 reading ago)')
    ax2.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m'))
    plt.xticks(rotation=45)
    ax2.grid(True, alpha=0.3)
    st.pyplot(fig2)

with col2:
    fig3, ax3 = plt.subplots(figsize=(6, 3))
    ax3.plot(df['timestamp'], df['windspeed'], color='purple', linewidth=1)
    ax3.set_title('Wind Speed Over Time')
    ax3.xaxis.set_major_formatter(mdates.DateFormatter('%d/%m'))
    plt.xticks(rotation=45)
    ax3.grid(True, alpha=0.3)
    st.pyplot(fig3)

# Raw data table
st.subheader('Recent Records')
st.dataframe(df.tail(20).sort_values('timestamp', ascending=False))