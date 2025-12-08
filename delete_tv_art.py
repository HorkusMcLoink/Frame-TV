I believe this is the script I last tried:
'
import os
import asyncio
import logging
from samsungtvws import SamsungTVWS

# --- CONFIGURATION ---
TV_HOST = '192.168.5.81' 
# IMPORTANT: Use the exact path your other script uses to save the token.
# Assuming it's in the same directory as this script, named 'tv-token.txt'
TOKEN_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'tv-token.txt') 
# --- END CONFIGURATION ---

# Set up basic logging for script feedback
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

async def delete_all_art():
    """
    Connects to the Samsung TV and deletes all uploaded art mode photos,
    reusing an existing token from the specified file.
    """
    if not os.path.exists(TOKEN_FILE):
        log.error(f"Token file not found at: {TOKEN_FILE}. Ensure your token is generated first.")
        return

    log.info(f"Connecting to TV at {TV_HOST} using token from {TOKEN_FILE}...")

    # The token_file argument automatically tells SamsungTVWS to load the existing token.
    tv = SamsungTVWS(host=TV_HOST, 
                     port=8002, 
                     token_file=TOKEN_FILE) 

    try:
        # 1. Get the list of all available art
        available_art = tv.art().available()
        
        if not available_art:
            log.info("No art found on the TV. Nothing to delete.")
            return

        # 2. Extract all content IDs
        content_ids_to_delete = [art['content_id'] for art in available_art]
        
        log.info(f"Found {len(content_ids_to_delete)} items to delete.")
        
        # 3. Use delete_list to remove all of them
        tv.art().delete_list(content_ids_to_delete)
        
        log.info("Art Mode deletion complete. TV art list should now be empty.")
        
    except ConnectionRefusedError:
        log.error("Connection refused. Ensure the TV is on, the IP is correct, and the token is still valid.")
    except TimeoutError:
        log.error("Connection timed out. Ensure the TV is awake.")
    except Exception as e:
        log.error(f"An unexpected error occurred: {e}")
    finally:
        # Ensure the connection is closed
        await tv.close()

if __name__ == '__main__':
    try:
        asyncio.run(delete_all_art())
    except KeyboardInterrupt:
        log.info("Script manually stopped.")
    except Exception as e:
        log.error(f"Script failed during execution: {e}")


'
