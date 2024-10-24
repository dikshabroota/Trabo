from kiteconnect import KiteConnect
import pandas as pd
from datetime import datetime, timedelta
import time
from config import api_key, access_token, instrument_tokens
import concurrent.futures
from kiteconnect.exceptions import NetworkException
import requests

from websocket import start_websocket, get_live_data, stop_websocket, selected_stocks

kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)


def create_hypothetical_box(data):
    # Filter data for the last 45 minutes of trading
    # last_45_min_data = data.tail(23)  # Assuming 2-minute candles, so 45 mins = 22 candles
    
    # Calculate the high and low of the last 45 minutes
    high_45_min = data['high'].max()
    low_45_min = data['low'].min()
    
    # Return the hypothetical box values
    return {
        'high_45_min': float(high_45_min),
        'low_45_min': float(low_45_min)
    }


def fetch_2min_historical_data(stock_symbol, from_date, to_date):
    delay = 5
    try:

        # Fetch 2-minute candle data
        data = kite.historical_data(instrument_tokens[stock_symbol], from_date, to_date, "2minute")
        # Convert the data to a DataFrame
        df = pd.DataFrame(data)
        #print(df)
        return df
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError, NetworkException) as e:
        # delay*=2
        print(f"{e} occured in FETCH_2MIN_HISTORICAL_DATA. Retrying after {delay} seconds...\n")
        time.sleep(delay)
        return fetch_2min_historical_data(stock_symbol, from_date, to_date)
    except KeyError:
        print(f"Error: Could not find instrument token for {stock_symbol}.")
        return None
    

def calculate_position_size(entry_price, stop_loss_price, max_loss_per_stock=500):
    stop_loss_distance = abs(entry_price - stop_loss_price)
    position_size = max_loss_per_stock / stop_loss_distance
    return int(position_size)


def check_candle_range(first_candle, prev_close_price):
    candle_range = first_candle['high'] - first_candle['low']
    return candle_range <= 0.0075 * prev_close_price

def candle_colour(candle):
    if candle['close'] > candle['open']:
        return "green"
    elif candle['close'] < candle['open']:
        return "red"
    else:
        return None  # Neutral candle, treat as no signal


def process_new_candle(symbol, first_candle, current_price, hypothetical_box, prev_close_price, max_loss_per_stock=500):
    
    if not check_candle_range(first_candle, prev_close_price):
        return None  # If the size of the first candle is too big, we do not proceed with the trade

    # Conditions for the body of the first candle being outside the hypothetical box - above or below
    # body_above_box = first_candle['open'] > hypothetical_box['high_45_min'] and first_candle['close'] > hypothetical_box['high_45_min']
    # body_below_box = first_candle['open'] < hypothetical_box['low_45_min'] and first_candle['close'] < hypothetical_box['low_45_min']

    # max_volume_prev_day = previous_day_df['volume'].max()

    

    # Check for Buy Conditions
    if candle_colour(first_candle)=='green' and current_price > first_candle['high']:
        stop_loss = first_candle['low']
        target = first_candle['high'] + 2 * (first_candle['high'] - stop_loss)
        position_size = calculate_position_size(first_candle['high'], stop_loss, max_loss_per_stock)
        print(f"Trying to BUY for stock {symbol} time : {datetime.now().time()} and at price : {current_price}")

        # Place a Bracket Buy Order
        try:
            order_id = kite.place_order(
                tradingsymbol=symbol,
                exchange="NSE",
                transaction_type="BUY",
                quantity=position_size,
                order_type="MARKET",
                product="MIS",
                variety="regular"
            )
            
            # Print time of order and price at which it was bought
            print(f"Buy order placed successfully! Order ID: {order_id}")

            # Fetch trade details to get the exact price and time
            order_details = kite.order_trades(order_id)
            if order_details:
                execution_time = order_details[0]['exchange_timestamp']
                executed_price = order_details[0]['average_price']
                print(f"Stock {symbol} Time of Buy:{execution_time} at Price:{executed_price} SL:{stop_loss} Target:{target}" )

        except Exception as e:
            print(f"Error placing buy order: {e}")

        return "buy", first_candle['high'], stop_loss, target, position_size


    # Check for Sell Conditions
    elif candle_colour(first_candle) == 'red' and current_price < first_candle['low']:
        stop_loss = first_candle['high']
        target = first_candle['low'] - 2 * (stop_loss - first_candle['low'])
        position_size = calculate_position_size(first_candle['low'], stop_loss, max_loss_per_stock)
        print(f"Trying to SELL for stock {symbol} time : {datetime.now().time} and at price : {current_price}")

         # Place a Bracket Sell Order with Market Order Type
        try:
            order_id = kite.place_order(
                tradingsymbol=symbol,
                exchange="NSE",
                transaction_type="SELL",
                quantity=position_size,
                order_type="MARKET",
                product="MIS",
                variety="regular"
            )

            # Print time of order and price at which it was sold
            print(f"Sell order placed successfully! Order ID: {order_id}")

            # Fetch trade details to get the exact price and time
            order_details = kite.order_trades(order_id)
            if order_details:
                execution_time = order_details[0]['exchange_timestamp']
                executed_price = order_details[0]['average_price']
                print(f"Stock {symbol} Time of Sell:{execution_time} at Price:{executed_price} SL:{stop_loss} Target:{target}")


        except Exception as e:
            print(f"Error placing sell order: {e}")

        return "sell", first_candle['low'], stop_loss, target, position_size
        
    return None


def square_off_time_reached(square_off_time):
    #Checks if it's time to square off all trades
    return datetime.now().time() >= square_off_time



def get_completed_candle(symbol):
    # Assuming you use Kite API's historical data to fetch the completed candle
    start_time = datetime(2024, 9, 27, 9, 15)
    end_time = (start_time + timedelta(minutes=2))
    
    # instrument_token = kite.ltp(f"NSE:{symbol}")['NSE:' + symbol]['instrument_token']

    # instrument_token = get_instrument_token(symbol)
    delay = 5

    try:
        data = kite.historical_data(
            instrument_tokens[symbol],
            from_date=start_time.strftime("%Y-%m-%d %H:%M:%S"),
            to_date=end_time.strftime("%Y-%m-%d %H:%M:%S"),
            interval="2minute"
        )

        if data:
            #print(f"start time:{start_time} and end time:{end_time}")
            return data[-1]  # Return the last candle which should be the one from the specified interval
        else:
            return None  # If no data is returned
        
    except NetworkException or ConnectionError as e:
        # delay*=2
        print(f"{e} encountered in get_completed_candle(). Retrying in {delay} seconds...\n")
        time.sleep(delay)
        


def monitor_trade(symbol, stop_loss, target, square_off_time, trade_type, position_size):
    #Monitors the trade and exits based on stop-loss, target, or square-off time
    while True:

        # live_data = kite.ltp(f"NSE:{symbol}")  

        live_data = get_live_data(symbol)

        # Wait until live data is available
        #start_time = time.time()
        while live_data is None: #and time.time() - start_time < 10:  # Wait for up to 10 seconds
            print(f"Waiting for live data of {symbol}...")
            time.sleep(2)
            live_data = get_live_data(symbol)


        current_price = live_data #[f'NSE:{symbol}']['last_price']


        if trade_type == "buy":
            if current_price <= stop_loss:
                print(f"Stop-loss hit for {symbol} (Buy). Exiting trade.")
                kite.place_order(
                    tradingsymbol=symbol,
                    exchange="NSE",
                    transaction_type="SELL",
                    quantity=position_size,
                    order_type="MARKET",
                    product="MIS",
                    variety="regular"
                )
                break
            elif current_price >= target:
                print(f"Target hit for {symbol} (Buy). Exiting trade.")
                kite.place_order(
                    tradingsymbol=symbol,
                    exchange="NSE",
                    transaction_type="SELL",
                    quantity=position_size,
                    order_type="MARKET",
                    product="MIS",
                    variety="regular"
                )
                break

        elif trade_type == "sell":
            if current_price >= stop_loss:
                print(f"Stop-loss hit for {symbol} (Sell). Exiting trade.")
                kite.place_order(
                    tradingsymbol=symbol,
                    exchange="NSE",
                    transaction_type="BUY",
                    quantity=position_size,
                    order_type="MARKET",
                    product="MIS",
                    variety="regular"
                )
                break
            elif current_price <= target:
                print(f"Target hit for {symbol} (Sell). Exiting trade.")
                kite.place_order(
                    tradingsymbol=symbol,
                    exchange="NSE",
                    transaction_type="BUY",
                    quantity=position_size,
                    order_type="MARKET",
                    product="MIS",
                    variety="regular"
                )
                break


        if square_off_time_reached(square_off_time):
            try:
                kite.place_order(
                    tradingsymbol=symbol,
                    exchange="NSE",
                    transaction_type="SELL" if trade_type == "buy" else "BUY",
                    quantity=position_size,
                    order_type="MARKET",
                    product="MIS",
                    variety="regular"
                )
                print(f"Square-off order placed for {symbol} at {datetime.now()}")
            except Exception as e:
                print(f"Error placing square-off order for {symbol}: {e}")
            break



def live_trading_simulation(symbol, hypothetical_box, prev_close_price, max_trades=6, max_loss_per_stock=500):
    trades = []
    first_candle = None
    square_off_time = datetime.strptime("10:00", "%H:%M").time()
    first_candle_time = datetime.strptime("09:17", "%H:%M").time()
    #candle_count = 0

    while True:
        current_time = datetime.now().time()


        # Wait until the first candle is fully formed at 9:17 AM
        if current_time < first_candle_time:
            print("Waiting for first candle to form. Will check again in 5 seconds")
            time.sleep(5)
            continue


        # Square off all trades at 10:00 AM
        if square_off_time_reached(square_off_time):
            print(f"Square off time reached")
            # stop_websocket()
            # print("WebSocket closed after square-off time.")
            break

        

        # Fetch the first candle after it's formed
        if first_candle is None and current_time >= first_candle_time and current_time <square_off_time:
            first_candle = get_completed_candle(symbol)  # Placeholder for fetching the completed first candle

                   
            # if not (
            #     (first_candle['close'] > first_candle['open'] and first_candle['open'] > hypothetical_box['high_45_min']) or
            #     (first_candle['close'] < first_candle['open'] and first_candle['open'] < hypothetical_box['low_45_min']) 
            # ):


        if first_candle:

            body_above_box = first_candle['open'] > hypothetical_box['high_45_min'] and first_candle['close'] > hypothetical_box['high_45_min']
            body_below_box = first_candle['open'] < hypothetical_box['low_45_min'] and first_candle['close'] < hypothetical_box['low_45_min']

            if not (candle_colour(first_candle) == 'green' and body_above_box) or (candle_colour(first_candle) == 'red' and body_below_box):
                print(f"First candle does not meet the conditions. Exiting simulation for {symbol}.")
                break

            #candle_count+=1
            # live_data = kite.ltp(f"NSE:{symbol}") 
            live_data = get_live_data(symbol)

            # Wait until live data is available
            #start_time = time.time()
            while live_data is None: #and time.time() - start_time < 10:  # Wait for up to 10 seconds
                print(f"Waiting for live data of {symbol}...")
                time.sleep(2)
                live_data = get_live_data(symbol)


            current_price = live_data #[f'NSE:{symbol}']['last_price']

            # Pass the first candle and current price to process_new_candle
            trade = process_new_candle(symbol, first_candle, current_price, hypothetical_box, prev_close_price, max_loss_per_stock)
            if trade:
                trade_type, entry_price, stop_loss, target, position_size = trade
                trades.append(trade)
                monitor_trade(symbol, stop_loss, target, square_off_time, trade_type, position_size)
                break

            first_candle_datetime = datetime.combine(datetime.today(), first_candle_time)
            current_datetime = datetime.combine(datetime.today(), current_time)

            # Check if 10 minutes or more have passed [5 candles basically]
            if current_datetime >= first_candle_datetime + timedelta(minutes=10):
            #if candle_count >= 5:
                print(f"No trade executed for {symbol} within 5 candles. Stopping checks.")
                break

    return trades



def apply_trading_logic():
    start_websocket()

    all_trades = []

    def trade_for_stock(stock):
        # Fetch the data you will use to create the hypothetical box (from the previous day)
        end_time_for_box = datetime(2024, 9, 26, 15, 30)
        start_time_for_box = end_time_for_box - timedelta(minutes=45)
        df = fetch_2min_historical_data(stock, start_time_for_box, end_time_for_box)  # Placeholder function; you need to implement this
        prev_close_price = df.iloc[-1]['close']
        hypothetical_box = create_hypothetical_box(df)
        trades = live_trading_simulation(stock, hypothetical_box, prev_close_price)
        return trades 
    
    
        # Use ThreadPoolExecutor to run the trading logic for each stock concurrently
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(trade_for_stock, selected_stocks)

    # Collect all trades from the threads
    for trades in results:
        all_trades.extend(trades)

    print("All trades executed today:", all_trades)

    stop_websocket()  # Close WebSocket after all trades are done
    print("Web socket closed")
    return all_trades


apply_trading_logic()