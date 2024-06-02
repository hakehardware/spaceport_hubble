import argparse

from src.logger import logger
from src.hubble import Hubble

def main():
    parser = argparse.ArgumentParser(description='SpacePort Node')

    parser.add_argument('--host_ip', type=str, required=True, help='Host IP')
    parser.add_argument('--nexus_url', type=str, required=True, help='Nexus URL')

    # Parse the arguments
    args = parser.parse_args()

    # Access the arguments
    config = {
        'host_ip': args.host_ip,
        'nexus_url': args.nexus_url
    }

    logger.info(f"Got Config: {config}")

    Hubble(config).run()

if __name__ == "__main__":
    main()