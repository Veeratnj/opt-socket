import pandas as pd
import json
from datetime import datetime, timedelta

def load_ticks(file_path):
    ticks = []
    with open(file_path, 'r') as f:
        for line in f:
            tick = json.loads(line.strip().replace("'", '"'))
            ticks.append({
                'LTT': tick['LTT'],
                'LTP': float(tick['LTP'])
            })
    return ticks

def create_manual_30sec_candles(ticks, date_str):
    candles = []

    current_candle = None
    start_time = None
    end_time = None

    for tick in ticks:
        tick_time = datetime.strptime(date_str + ' ' + tick['LTT'], '%Y-%m-%d %H:%M:%S')
        price = tick['LTP']

        if current_candle is None:
            # Start new candle
            start_time = tick_time.replace(second=(tick_time.second // 30) * 30, microsecond=0)
            end_time = start_time + timedelta(seconds=30)

            current_candle = {
                'timestamp': start_time,
                'open': price,
                'high': price,
                'low': price,
                'close': price
            }
        else:
            if tick_time < end_time:
                # Update current candle
                current_candle['high'] = max(current_candle['high'], price)
                current_candle['low'] = min(current_candle['low'], price)
                current_candle['close'] = price
            else:
                # Save current candle and start new one
                candles.append(current_candle)

                # Start next 30-sec bucket
                start_time = tick_time.replace(second=(tick_time.second // 30) * 30, microsecond=0)
                end_time = start_time + timedelta(seconds=30)

                current_candle = {
                    'timestamp': start_time,
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price
                }

    # Save the last candle if exists
    if current_candle:
        candles.append(current_candle)

    # Convert to DataFrame
    df = pd.DataFrame(candles)
    return df

def save_to_csv(df, output_file):
    df.to_csv(output_file, index=False)

# Usage
file_path = 'condition_log.txt'
output_csv = '30sec_candles_manual.csv'
fixed_date = '2025-07-11'  # Set correct date

ticks = load_ticks(file_path)
ohlc_df = create_manual_30sec_candles(ticks, fixed_date)
save_to_csv(ohlc_df, output_csv)

print("Manual 30-sec candles saved to", output_csv)
