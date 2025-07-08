import upstox_client
from upstox_client.rest import ApiException
import time
import threading
import logging
from flask import Flask, jsonify

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

latest_ltp = None
ltp_lock = threading.Lock()

def upstox_main():
    global latest_ltp

    configuration = upstox_client.Configuration()
    configuration.access_token = 'UPSTOX_TOKEN'  # Replace with your actual access token

    try:
        api_client = upstox_client.ApiClient(configuration)
        streamer = upstox_client.MarketDataStreamerV3(api_client)
    except Exception as e:
        logger.error(f"Failed to initialize API client: {e}")
        return

    def on_open():
        logger.info("Connected to WebSocket")
        try:
            streamer.subscribe(["NSE_EQ|INE155A01022"], "full")
            logger.info("Subscribed to Tata Motors market data")
        except Exception as e:
            logger.error(f"Subscription error: {e}")

    def on_message(message):
        global latest_ltp
        try:
            with ltp_lock:
                if 'feeds' in message and 'NSE_EQ|INE155A01022' in message['feeds']:
                    feed = message['feeds']['NSE_EQ|INE155A01022']
                    logger.info(f"Feed data: {feed}")
                    # Updated extraction for new structure
                    if 'fullFeed' in feed and 'marketFF' in feed['fullFeed']:
                        ltpc = feed['fullFeed']['marketFF'].get('ltpc', {})
                        latest_ltp = ltpc.get('ltp', None)
                        if latest_ltp:
                            logger.debug(f"Received LTP: {latest_ltp}")
                    else:
                        logger.error(f"'fullFeed' or 'marketFF' not in feed: {feed}")
                else:
                    logger.error(f"'feeds' or symbol not in message: {message}")
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

@app.route('/ltp')
def get_ltp():
    global latest_ltp
    with ltp_lock:
        return jsonify({'ltp': latest_ltp})

if __name__ == "__main__":
    threading.Thread(target=upstox_main, daemon=True).start()
    app.run(host='0.0.0.0', port=5001)
