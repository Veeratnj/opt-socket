from dhanhq import DhanContext, MarketFeed
import json
from datetime import date, datetime, timedelta

import pytz
ist = pytz.timezone("Asia/Kolkata")

from services import insert_ohlc_data

client_id='1100449732'
access_token='eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzU0NDUzNjA1LCJ0b2tlbkNvbnN1bWVyVHlwZSI6IlNFTEYiLCJ3ZWJob29rVXJsIjoiIiwiZGhhbkNsaWVudElkIjoiMTEwMDQ0OTczMiJ9.K_zJLkSzMNpmUPBqDzhhki2CD-s0WiByit-8YZa-WZutplFG5SRuio1lzBbEDb4hiTyVmD56rjY2O-57EXKW1Q'

dhan_context = DhanContext(client_id, access_token)

security_id = '25'
instruments = [
    (MarketFeed.IDX, security_id, MarketFeed.Ticker),
]

version = "v2"

def round_down_time_30s(dt):
    second_block = 0 if dt.second < 30 else 30
    return dt.replace(second=second_block, microsecond=0)

current_candle = {}
current_interval_start = {}
candles = {}
print(instruments)
try:
    data = MarketFeed(dhan_context, instruments, version)
    print("Starting Market Feed...")
    while True:
        now = datetime.now()
        start_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
        end_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
        print(now)
        if not(start_time <= now <= end_time):
            print("📈 Market hours (9:15 AM - 3:30 PM). Waiting...")
            continue
        data.run_forever()
        # print("Market Feed running...")
        # print(dir(data.get_data))
        response = data.get_data()
        print(f"Response: {response}")
        stock_name = 'BANK_NIFTY'

        if 'LTP' in response and 'LTT' in response:
            ltp = response['LTP']
            ts_epoch = response['LTT']
            with open("condition_log.txt", "a") as f:
                f.write(f"{str(response)}\n")

            # Ensure dicts are initialized
            if security_id not in current_interval_start:
                current_interval_start[security_id] = None
            if security_id not in current_candle:
                current_candle[security_id] = None
            if security_id not in candles:
                candles[security_id] = []

            # Convert LTT ("12:03:50") to IST datetime
            time_obj = datetime.strptime(ts_epoch, "%H:%M:%S").time()
            ts = ist.localize(datetime.combine(date.today(), time_obj))

            interval_start = round_down_time_30s(ts)
            print(f"Timestamp: {ts}, Interval start: {interval_start}")

            # Create new candle if new interval
            if current_interval_start[security_id] is None or interval_start > current_interval_start[security_id]:
                if current_candle[security_id] is not None:
                    candles[security_id].append(current_candle[security_id])
                    completed_candle = current_candle[security_id]
                    try:
                        insert_ohlc_data(
                            token=security_id,
                            start_time=completed_candle['start_time'],
                            open_=completed_candle['open'],
                            high=completed_candle['high'],
                            low=completed_candle['low'],
                            close=completed_candle['close'],
                            interval='30s'  # Changed to 30s for clarity
                        )
                        print(f"Inserted candle: {completed_candle}")
                    except Exception as e:
                        print(f"Insert DB error: {e}")

                current_interval_start[security_id] = interval_start
                current_candle[security_id] = {
                    'start_time': interval_start.strftime("%Y-%m-%d %H:%M:%S"),
                    'open': ltp,
                    'high': ltp,
                    'low': ltp,
                    'close': ltp
                }
            else:
                # Update current candle
                candle = current_candle[security_id]
                candle['high'] = max(candle['high'], ltp)
                candle['low'] = min(candle['low'], ltp)
                candle['close'] = ltp
        else:
            print(f"Missing data for token {security_id}, response: {response}")
            continue

except Exception as e:
    print(f"Main loop error: {e}")
