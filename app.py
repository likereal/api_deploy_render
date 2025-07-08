import upstox_client
from upstox_client.rest import ApiException
import time
import threading
import logging
from flask import Flask, jsonify, request
import csv
import sqlite3

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

latest_ltps = {}  # Store LTPs for multiple symbols
subscribed_symbols = set()  # Track currently subscribed symbols
ltp_lock = threading.Lock()
streamer = None  # Will hold the MarketDataStreamerV3 instance


def upstox_main():
    global streamer
    configuration = upstox_client.Configuration()
    configuration.access_token = 'eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI0S0FaUlEiLCJqdGkiOiI2ODZkNzJiNjhlYzdhMTUzOWY1NWEzOWYiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc1MjAwMzI1NCwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzUyMDEyMDAwfQ.sR5IVzjyFCHAnxL_RnhNhpLzgsJsdnU-YD7EsuOhbr8'  # Replace with your actual access token

    try:
        api_client = upstox_client.ApiClient(configuration)
        streamer = upstox_client.MarketDataStreamerV3(api_client)
    except Exception as e:
        logger.error(f"Failed to initialize API client: {e}")
        return

    def on_open():
        logger.info("Connected to WebSocket")
        # No initial subscriptions; subscribe on demand

    def on_message(message):
        global latest_ltps
        try:
            with ltp_lock:
                if 'feeds' in message:
                    for symbol in message['feeds']:
                        feed = message['feeds'][symbol]
                        logger.info(f"Feed data for {symbol}: {feed}")
                        if 'fullFeed' in feed and 'marketFF' in feed['fullFeed']:
                            ltpc = feed['fullFeed']['marketFF'].get('ltpc', {})
                            ltp = ltpc.get('ltp', None)
                            if ltp is not None:
                                latest_ltps[symbol] = ltp
                                logger.debug(f"Received LTP for {symbol}: {ltp}")
                        else:
                            logger.error(f"'fullFeed' or 'marketFF' not in feed: {feed}")
                else:
                    logger.error(f"'feeds' not in message: {message}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def on_error(error):
        logger.error(f"WebSocket error: {error}")

    def on_close():
        logger.info("WebSocket connection closed")

    streamer.on("open", on_open)
    streamer.on("message", on_message)
    streamer.on("error", on_error)
    streamer.on("close", on_close)

    try:
        logger.info("Connecting to WebSocket...")
        streamer.connect()
        while True:
            time.sleep(1)
    except ApiException as e:
        logger.error(f"API exception occurred: {e}")
    except KeyboardInterrupt:
        logger.info("Stopping the script...")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        try:
            streamer.disconnect()
            logger.info("Disconnected from WebSocket")
        except Exception as e:
            logger.error(f"Error during disconnect: {e}")

# Flask app
app = Flask(__name__)

def subscribe_symbol(symbol):
    global streamer
    with ltp_lock:
        if symbol not in subscribed_symbols and streamer is not None:
            try:
                streamer.subscribe([symbol], "full")
                subscribed_symbols.add(symbol)
                logger.info(f"Subscribed to symbol: {symbol}")
            except Exception as e:
                logger.error(f"Failed to subscribe to {symbol}: {e}")

def csv_to_sqlite(csv_path, db_path):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS bse_securities (scrip_code TEXT, name TEXT, security_id TEXT)')
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            c.execute('INSERT INTO bse_securities (scrip_code, name, security_id) VALUES (?, ?, ?)', (row['SCRIP_CODE'], row['NAME'], row['Security_Id']))
    conn.commit()
    conn.close()

# Run this once to create the DB
# csv_to_sqlite('Equity.csv', 'bse_securities.db')

def nl2sql_stock_name(nl_query, db_path='bse_securities.db'):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()
    # Normalize input for better matching
    search = nl_query.replace(' ', '').lower()
    # Try exact match on name
    c.execute("SELECT scrip_code, name, security_id FROM bse_securities WHERE REPLACE(LOWER(name), ' ', '') = ?", (search,))
    result = c.fetchone()
    if not result:
        # Try exact match on security_id
        c.execute("SELECT scrip_code, name, security_id FROM bse_securities WHERE LOWER(security_id) = ?", (nl_query.lower(),))
        result = c.fetchone()
    if not result:
        # Try partial match on name
        c.execute("SELECT scrip_code, name, security_id FROM bse_securities WHERE LOWER(name) LIKE ?", (f"%{nl_query.lower()}%",))
        result = c.fetchone()
    if not result:
        # Try partial match on security_id
        c.execute("SELECT scrip_code, name, security_id FROM bse_securities WHERE LOWER(security_id) LIKE ?", (f"%{nl_query.lower()}%",))
        result = c.fetchone()
    conn.close()
    return result  # (scrip_code, name, security_id) or None

@app.route('/ltp')
def get_ltp():
    nl = request.args.get('nl')
    symbol = request.args.get('symbol')
    if nl:
        result = nl2sql_stock_name(nl)
        if not result:
            return jsonify({'error': f'No BSE stock found for query: {nl}'}), 404
        scrip_code, name, security_id = result
        symbol = f'BSE_EQ|{scrip_code}'
    if not symbol:
        return jsonify({'error': 'Missing symbol or nl parameter'}), 400
    subscribe_symbol(symbol)
    with ltp_lock:
        ltp = latest_ltps.get(symbol)
    if ltp is None:
        return jsonify({'ltp': None, 'message': 'LTP not available yet, please retry in a moment.'}), 202
    return jsonify({'ltp': ltp})

if __name__ == "__main__":
    threading.Thread(target=upstox_main, daemon=True).start()
    app.run(host='0.0.0.0', port=5001)
