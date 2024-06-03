from src.logger import logger
import requests
import sys
import time

class Nexus:
    @staticmethod
    def upsert_entity(base_url, entity, event):
        local_url = f"{base_url}/upsert/{entity}"
        response = Nexus.push(local_url, event)
        if response.status_code < 300: return response.json()
        else: return False

    @staticmethod
    def push(local_url, event):
        max_retries = 10
        retries = 0

        while True:
            try:
                response = requests.post(local_url, json=event)
                if response.status_code >= 300:
                    logger.error(response.json())
                    time.sleep(10)
                    
                return response

            except Exception as e:
                if retries == max_retries:
                    logger.error('Max retries reached. Exiting...')
                    sys.exit(1)
                retries+=1
                logger.error(f"Retries: {retries}, Max allowed: {max_retries}")
                time.sleep(1)