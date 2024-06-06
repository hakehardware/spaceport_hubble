import sys
import re
import time

from datetime import datetime, timezone

from src.logger import logger
from src.utils import Utils
from src.nexus import Nexus


class StreamParser:

    @staticmethod
    def upsert_initial(nexus_url, container_id, container_alias, container_type):

        logger.info(f"Upserting initial container data for {container_alias}")
        Nexus.upsert_entity(nexus_url, container_type, {
            'container_id': container_id,
            'alias': container_alias
        })

    @staticmethod
    def upsert_cache_attributes(container, nexus_url, container_id, container_type):
        command = container.attrs['Config']['Cmd']
        for c in command:
            if 'path' in c:
                # Define regex patterns for path and size
                path_pattern = r"path=(.*?)(?=,)"
                size_pattern = r"size=(.*)"

                # Extract path and size using re.findall
                path = re.findall(path_pattern, c)[0]
                size = re.findall(size_pattern, c)[0]

                logger.info(f"Found cache Path of {path} and size of {size}")

                if path or size:
                    logger.info('Upserting Cache Path and Size')
                    Nexus.upsert_entity(nexus_url, container_type,  {
                        'container_id': container_id,
                        'cache_path': path,
                        'cache_size': size
                    })

    @staticmethod
    def upsert_controller_attributes(container, nexus_url, container_id, container_type):
        command = container.attrs['Config']['Cmd']
        base_path_index = command.index('--base-path')

        base_path = command[base_path_index + 1]

        logger.info('Upserting Base Path for Controller')
        Nexus.upsert_entity(nexus_url, container_type,  {
            'container_id': container_id,
            'base_path': base_path
        })

    @staticmethod
    def upsert_cluster_farmer_attributes(container, nexus_url, container_id, container_type):
        command = container.attrs['Config']['Cmd']
        reward_address_index = command.index('--reward-address')

        reward_address = command[reward_address_index + 1]

        logger.info('Upserting reward address for Farmer')
        Nexus.upsert_entity(nexus_url, container_type,  {
            'container_id': container_id,
            'reward_address': reward_address
        })

    @staticmethod
    def event_age(event):
        # Parse the event_datetime
        event_datetime_str = event.get('event_datetime')
        event_datetime = datetime.strptime(event_datetime_str, '%Y-%m-%d %H:%M:%S')

        # Ensure the event_datetime is in UTC
        event_datetime = event_datetime.replace(tzinfo=timezone.utc)

        # Get the current UTC time
        current_utc_time = datetime.now(timezone.utc)

        # Calculate the difference in hours
        time_difference = current_utc_time - event_datetime
        hours_difference = time_difference.total_seconds() / 3600

        # Return the rounded number of hours
        return round(hours_difference)

    @staticmethod
    def extract_cpu_sets(text):
        cpu_sets = re.findall(r'CpuSet\((.*?)\)', text)
        split_values = []
        for cpu_set in cpu_sets:
            split_values.extend(cpu_set.split(','))
        return split_values    
    
    @staticmethod
    def parse_event(log, container_id, container_alias, container_type):
        try:

            event = {
                'event_name': None,
                'event_type': None,
                'event_container_type': container_type,
                'event_container_alias': container_alias,
                'event_level': log["event_level"],
                'event_source': container_id,
                'event_datetime': log["event_datetime"],
                'event_data': {},
            }

            if 'Connecting to node RPC url' in log['event_data']:
                # Set Event name & type
                event['event_name'] = 'Register RPC URL'
                event['event_type'] = 'farmer'

                pattern = r'ws://([^ ]+)'
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None

                rpc_url = match.group(1)
                event['event_data'] = {
                    'status': 'RPC URL Registered',
                    'rpc_url': rpc_url
                }
                    
            if 'l3_cache_groups' in log['event_data']:
                event['event_name'] = 'Detecting L3 Cache Groups'
                event['event_type'] = 'farmer'

                pattern = r'l3_cache_groups=(\d+)'
                match = re.search(pattern, log['event_data'])
                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                l3_cache_groups_value = match.group(1)
                event['event_data'] = {
                    "status": "Detecting L3 Cache Groups",
                    'l3_cache_groups': l3_cache_groups_value
                }

            if 'plotting_thread_pool_core_indices' in log['event_data']:
                event['event_name'] = 'Preparing Plotting Thread Pools'
                event['event_type'] = 'farmer'

                plotting_pattern = r'plotting_thread_pool_core_indices=\[(.*?)\]'
                replotting_pattern = r'replotting_thread_pool_core_indices=\[(.*?)\]'


                # Search and extract the values for plotting_thread_pool_core_indices
                plotting_match = re.search(plotting_pattern, log['event_data'])
                plotting_values = []
                if plotting_match:
                    plotting_values = StreamParser.extract_cpu_sets(plotting_match.group(1))

                # Search and extract the values for replotting_thread_pool_core_indices
                replotting_match = re.search(replotting_pattern, log['event_data'])
                replotting_values = []
                if replotting_match:
                    replotting_values = StreamParser.extract_cpu_sets(replotting_match.group(1))

                event['event_data'] = {
                    'status': "Preparing Plotting Thread Pools",
                    'plotting_cpu_sets': ', '.join(plotting_values),
                    'replotting_cpu_sets': ', '.join(replotting_values)
                }

            if 'Checking plot cache contents' in log['event_data']:
                event['event_name'] = 'Checking Plot Cache Contents'
                event['event_type'] = 'farm'

                farm_index_pattern = r'farm_index=(\d+)'
                farm_index_match = re.search(farm_index_pattern, log['event_data'])

                if not farm_index_match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None

                event['event_data'] = {
                    'status': 'Checking Plot Cache Contents',
                    'farm_index': farm_index_match.group(1)
                }

            if 'Finished checking plot cache contents' in log['event_data']:
                event['event_name'] = 'Finished Checking Plot Cache Contents'
                event['event_type'] = 'farm'
                farm_index_pattern = r'farm_index=(\d+)'
                farm_index_match = re.search(farm_index_pattern, log['event_data'])

                if not farm_index_match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None

                event['event_data'] = {
                    'status': 'Finished Checking Plot Cache Contents',
                    'farm_index': farm_index_match.group(1)
                }

            if 'Downloading all segment headers from node' in log['event_data']:
                event['event_name'] = 'Downloading Segment Headers'
                event['event_type'] = 'farmer'
                event['event_data'] = {
                    'status': 'Downloading Segment Headers'
                }

            if 'Downloaded all segment headers from node successfully' in log['event_data']:
                event['event_name'] = 'Downloaded All Segment Headers'
                event['event_type'] = 'farmer'
                event['event_data'] = {
                    'status': 'Downloaded All Segment Headers'
                }

            if 'async_nats: event: connected' in log['event_data']:
                event['event_name'] = 'NATs Connected'
                event['event_type'] = 'farmer'
                event['event_data'] = {
                    'nats_connected': 1
                }

            if 'async_nats: event: disconnected' in log['event_data']:
                event['event_name'] = 'NATs Disconnected'
                event['event_type'] = 'farmer'
                event['event_data'] = {
                    'nats_connected': 0
                }

            if 'Benchmarking faster proving method' in log['event_data']:
                event['event_name'] = 'Benchmarking Proving Method'
                event['event_type'] = 'farm'

                farm_index_pattern = r'farm_index=(\d+)'
                farm_index_match = re.search(farm_index_pattern, log['event_data'])

                if not farm_index_match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None

                event['event_data'] = {
                    'status': 'Benchmarking Proving Method',
                    'farm_index': farm_index_match.group(1)
                }
                
            if 'Subscribing to slot info notifications' in log['event_data']:
                event['event_name'] = 'Subscribing to Slot Info Notifications'
                event['event_type'] = 'farm'

                farm_index_pattern = r'farm_index=(\d+)'
                farm_index_match = re.search(farm_index_pattern, log['event_data'])

                if not farm_index_match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None

                event['event_data'] = {
                    'status': 'Subscribing to Slot Info Notifications',
                    'farm_index': farm_index_match.group(1)
                }

            if 'Subscribing to reward signing notifications' in log['event_data']:
                event['event_name'] = 'Subscribing to Reward Signing Notifications'
                event['event_type'] = 'farm'

                farm_index_pattern = r'farm_index=(\d+)'
                farm_index_match = re.search(farm_index_pattern, log['event_data'])

                if not farm_index_match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None

                event['event_data'] = {
                    'status': 'Subscribing to Reward Signing Notifications',
                    'farm_index': farm_index_match.group(1)
                }

            if 'Subscribing to archived segments' in log['event_data']:
                event['event_name'] = 'Subscribing to Archived Segments'
                event['event_type'] = 'farm'

                farm_index_pattern = r'farm_index=(\d+)'
                farm_index_match = re.search(farm_index_pattern, log['event_data'])

                if not farm_index_match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None

                event['event_data'] = {
                    'status': 'Subscribing to Archived Segments',
                    'farm_index': farm_index_match.group(1)
                }

            if 'fastest_mode' in log['event_data']:
                event['event_name'] = 'Found Fastest Mode'
                event['event_type'] = 'farm'
                pattern = r"\{farm_index=(\d+)\}.*fastest_mode=(\w+)"
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                event['event_data'] = {
                    'farm_index': match.group(1),
                    'fastest_mode': match.group(2),
                    'status': 'Found Fastest Mode'
                }

            if 'ID:' in log['event_data']:
                event['event_name'] = 'Register Farm ID'
                event['event_type'] = 'farm'
                # Define the regex patterns
                farm_index_pattern = r'farm_index=(\d+)'
                id_pattern = r'ID:\s+(\S+)'

                # Search for the farm_index value
                farm_index_match = re.search(farm_index_pattern, log['event_data'])
                

                # Search for the ID value
                id_match = re.search(id_pattern, log['event_data'])

                if not farm_index_match or not id_match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)

                farm_index_value = farm_index_match.group(1)
                farm_id_value = id_match.group(1)

                event['event_data'] = {
                    'farm_index': farm_index_value,
                    'farm_id': farm_id_value,
                    'status': 'Register Farm ID'
                }

            if 'Genesis hash:' in log['event_data']:
                event['event_name'] = 'Register Genesis Hash'
                event['event_type'] = 'farm'

                # Define the regex patterns
                farm_index_pattern = r'farm_index=(\d+)'
                genesis_hash_pattern = r'Genesis hash:\s+(0x\S+)'

                farm_index_match = re.search(farm_index_pattern, log['event_data'])
                genesis_hash_match = re.search(genesis_hash_pattern, log['event_data'])

                if not farm_index_match or not genesis_hash_match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)

                farm_index_value = farm_index_match.group(1)
                genesis_hash_value = genesis_hash_match.group(1)

                event['event_data'] = {
                    'farm_index': farm_index_value,
                    'farm_genesis_hash': genesis_hash_value,
                    'status': 'Register Genesis Hash'
                }

            if 'Public key:' in log['event_data']:
                event['event_name'] = 'Register Public Key'
                event['event_type'] = 'farm'

                # Define the regex patterns
                farm_index_pattern = r'farm_index=(\d+)'
                public_key_pattern = r'Public key:\s+(0x[0-9a-fA-F]+)'

                # Search for the farm_index value
                farm_index_match = re.search(farm_index_pattern, log['event_data'])
                public_key_match = re.search(public_key_pattern, log['event_data'])

                if not farm_index_match or not public_key_match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)

                farm_index_value = farm_index_match.group(1)
                public_key_value = public_key_match.group(1)

                event['event_data'] = {
                    'farm_index': farm_index_value,
                    'farm_public_key': public_key_value,
                    'status': 'Initializing'
                }

            if 'Allocated space:' in log['event_data']:
                event['event_name'] = 'Register Allocated Space'
                event['event_type'] = 'farm'
                pattern = r'farm_index=(\d+).*Allocated space:\s+([\d.]+)\s+(GiB|TiB|GB|TB)\s+\(([\d.]+)\s+(GiB|TiB|GB|TB)\)'
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                if match.group(3) == 'TiB':
                    allocated_gib = float(match.group(2)) * 1024
                else:
                    allocated_gib = float(match.group(2))

                event['event_data'] = {
                    'farm_index': int(match.group(1)),
                    'farm_size': allocated_gib,
                    'status': 'Register Allocated Space'                
                }

            if 'Directory:' in log['event_data']:
                event['event_name'] = 'Register Directory'
                event['event_type'] = 'farm'

                pattern = r'farm_index=(\d+).*Directory:\s+(.+)'
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                event['event_data'] = {
                    'farm_index': int(match.group(1)),
                    'farm_directory': match.group(2),
                    'status': 'Register Directory'
                }

            if 'Collecting already plotted pieces' in log['event_data']:
                event['event_name'] = 'Collecting Plotted Pieces'
                event['event_type'] = 'farmer'
                event['event_data'] = {
                    'status': 'Collecting Plotted Pieces'
                }

            if 'Finished collecting already plotted pieces successfully' in log['event_data']:
                event['event_name'] = 'Finished Collecting Plotted Pieces'
                event['event_type'] = 'farmer'
                event['event_data'] = {
                    'status': 'Finished Collecting Plotted Pieces'
                }

            if 'Initializing piece cache' in log['event_data']:
                event['event_name'] = 'Initializing Piece Cache'
                event['event_type'] = 'farmer'
                event['event_data'] = {
                    'status': 'Initializing Piece Cache'
                }

            if 'Synchronizing piece cache' in log['event_data']:
                event['event_name'] = 'Syncronizing Piece Cache'
                event['event_type'] = 'farmer'
                event['event_data'] = {
                    'status': 'Syncronizing Piece Cache'
                }

            if 'Piece cache sync' in log['event_data']:
                event['event_name'] = 'Piece Cache Sync'
                event['event_type'] = 'farmer'

                pattern = r'Piece cache sync (\d+\.\d+)% complete'
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                event['event_data'] = {
                    'status': 'Syncronizing Piece Cache',
                    'piece_cache_pct': float(match.group(1))
                }

            if 'Finished piece cache synchronization' in log['event_data']:
                event['event_name'] = 'Finished Piece Cache Syncronization'
                event['event_type'] = 'farmer'
                event['event_data'] = {
                    'status': 'Piece Cache Syncronized',
                    'piece_cache_pct': 100.00
                }

            if 'Plotting sector' in log['event_data']:
                event['event_name'] = 'Plotting Sector'
                event['event_type'] = 'farm'

                pattern = r'farm_index=(\d+).*?(\d+\.\d+)% complete.*?sector_index=(\d+)'      
                match = re.search(pattern, log['event_data'])
                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                event['event_data'] = {
                    'farm_index': int(match.group(1)),
                    'plot_percentage': float(match.group(2)),
                    'plot_current_sector': int(match.group(3)),
                    'plot_type': 'Plot',
                    'status': 'Plotting Sector'
                }

            if 'Successfully signed reward hash' in log['event_data']:
                event['event_name'] = 'Signed Reward Hash'
                event['event_type'] = 'farm'
                pattern = r'farm_index=(\d+).*hash\s(0x[0-9a-fA-F]+)'
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None

                event['event_data'] = {
                    'farm_index': match.group(1),
                    'reward_hash': match.group(2),
                    'reward_type': 'Reward',
                }

            if 'Initial plotting complete' in log['event_data']:
                event['event_name'] = 'Initial Plotting Complete'
                event['event_type'] = 'farm'
                pattern = r"farm_index=(\d+)"
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                event['event_data'] = {
                    'farm_index': int(match.group(1)),
                    'plot_percentage': 100,
                    'plot_current_sector': None,
                    'plot_type': 'Plot',
                    'status': 'Farming'
                }

            if 'Replotting sector' in log['event_data'] and 'complete' in log['event_data']:
                event['event_name'] = 'Replotting Sector'
                event['event_type'] = 'farm'
                pattern = r'farm_index=(\d+).*?(\d+\.\d+)% complete.*?sector_index=(\d+)'      
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                event['event_data'] = {
                    'farm_index': int(match.group(1)),
                    'plot_percentage': float(match.group(2)),
                    'plot_current_sector': int(match.group(3)),
                    'plot_type': 'Replot',
                    'status': 'Replotting Sector'
                } 

            if 'Replotting complete' in log['event_data']:
                event['event_name'] = 'Replotting Complete'
                event['event_type'] = 'farm'
                pattern = r"farm_index=(\d+)"
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                event['event_data'] = {
                    'farm_index': int(match.group(1)),
                    'plot_percentage': 100,
                    'plot_current_sector': None,
                    'plot_type': 'Replot',
                    'status': 'Farming'
                }

            if 'Failed to send solution' in log['event_data']:
                event['event_name'] = 'Failed to Send Solution'
                event['event_type'] = 'farmer'

                pattern = r"farm_index=(\d+)"
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                event['event_data'] = {
                    'farm_index': int(match.group(1)),
                    'reward_hash': None,
                    'reward_type': 'Failed',
                }

            if 'Plot sector request' in log['event_data'] or 'Finished plotting sector successfully' in log['event_data']:
                if 'Plot sector request' in log['event_data']: event['event_name'] = 'Plot Sector Request'
                else: event['event_name'] = 'Finished Plotting Sector'

                event['event_type'] = 'farmer'

                # Define regex patterns
                public_key_pattern = r'public_key=([a-f0-9]+)'
                sector_index_pattern = r'sector_index=(\d+)'

                # Search and extract the public_key
                public_key_match = re.search(public_key_pattern, log['event_data'])
                public_key = public_key_match.group(1) if public_key_match else None

                # Search and extract the sector_index
                sector_index_match = re.search(sector_index_pattern, log['event_data'])
                sector_index = sector_index_match.group(1) if sector_index_match else None

                event['event_data'] = {
                    'status': event['event_name'],
                    'public_key': public_key,
                    'sector_index': sector_index
                }

            if 'New cache discovered' in log['event_data']:
                event['event_name'] = 'New Cache Discovered'
                event['event_type'] = 'farmer'

                # Define regex pattern for cache_id
                cache_id_pattern = r'cache_id=([A-Z0-9]+)'

                # Search and extract the cache_id
                match = re.search(cache_id_pattern, log['event_data'])
                cache_id = match.group(1)

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                event['event_data'] = {
                    'cache_id': cache_id
                }

            if 'Discovered new farm' in log['event_data'] or 'Farm initialized successfully' in log['event_data']:
                if 'Discovered new farm' in log['event_data']: event['event_name'] = 'Discovered New Farm'
                else: event['event_name'] = 'Farm Initialized Successfully'

                event['event_type'] = 'farmer'

                # Define regex patterns
                farm_index_pattern = r'farm_index=(\d+)'
                farm_id_pattern = r'farm_id=([A-Z0-9]+)'

                farm_index_match = re.search(farm_index_pattern, log['event_data'])
                farm_id_match = re.search(farm_id_pattern, log['event_data'])

                if not farm_index_match or not farm_id_match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)

                farm_id = farm_id_match.group(1)
                farm_index = farm_index_match.group(1)
                
                event['event_data'] = {
                    'farm_id': farm_id,
                    'farm_index': farm_index
                }

            if 'Idle' in log['event_data'] and 'best:' in log['event_data']:
                event['event_name'] = 'Idle'
                event['event_type'] = 'node'
                pattern = re.compile(
                    r'Idle \((?P<peers>\d+) peers\), best: #(?P<best>\d+).*finalized #(?P<finalized>\d+).*⬇ (?P<down_speed>\d+(?:\.\d+)?)(?P<down_unit>\s?[kKMmGg]?[iI]?[bB]/s) ⬆ (?P<up_speed>\d+(?:\.\d+)?)(?P<up_unit>\s?[kKMmGg]?[iI]?[bB]/s)'
                )
                match = pattern.search(log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                peers = match.group("peers")
                best = match.group("best")
                finalized = match.group("finalized")
                down_speed = match.group("down_speed")
                down_unit = match.group("down_unit")
                up_speed = match.group("up_speed")
                up_unit = match.group("up_unit")

                event['event_data'] = {
                    'status': 'Idle',
                    'peers': int(peers),
                    'best': int(best),
                    'target': None,
                    'finalized': int(finalized),
                    'bps': None,
                    'down_speed': float(down_speed),
                    'up_speed': float(up_speed),
                    'down_unit': down_unit,
                    'up_unit': up_unit
                }
            
            if 'Preparing' in log['event_data'] and 'target=' in log['event_data']:
                event['event_name'] = 'Preparing'
                event['event_type'] = 'node'

                pattern = re.compile(
                    r'(?:(?P<bps>\d+\.\d+)\s+bps,\s*)?'  # Optional bps
                    r'target=#(?P<target>\d+)\s+\((?P<peers>\d+)\s+peers\),\s+'  # target and peers
                    r'best:\s+#(?P<best>\d+)\s+\([^)]*\),\s+'  # best
                    r'finalized\s+#(?P<finalized>\d+)\s+\([^)]*\),\s+'  # finalized
                    r'⬇\s+(?P<down>\d+(?:\.\d+)?)(?P<down_unit>kiB|MiB)/s\s+'  # download speed and unit
                    r'⬆\s+(?P<up>\d+(?:\.\d+)?)(?P<up_unit>kiB|MiB)/s'  # upload speed and unit
                )
                match = pattern.search(log['event_data'])


                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                log_dict = match.groupdict()
                bps = log_dict["bps"] if log_dict["bps"] else None
                target = log_dict["target"]
                peers = log_dict["peers"]
                best = log_dict["best"]
                finalized = log_dict["finalized"]
                down = log_dict["down"]
                down_unit = log_dict["down_unit"]
                up = log_dict["up"]
                up_unit = log_dict["up_unit"]

                event['event_data'] = {
                    'status': 'Preparing',
                    'peers': int(peers),
                    'best': int(best),
                    'target': int(target),
                    'finalized': int(finalized),
                    'bps': float(bps) if bps else None,
                    'down_speed': float(down),
                    'up_speed': float(up),
                    'down_unit': down_unit,
                    'up_unit': up_unit
                }

            if 'Syncing' in log['event_data'] and 'target=' in log['event_data'] :
                event['event_name'] = 'Syncing'
                event['event_type'] = 'node'

                pattern = re.compile(
                    r'Consensus: substrate: ⚙️  Syncing(?:\s+(?P<bps>\d+\.\d+) bps)?, target=#(?P<target>\d+) \((?P<peers>\d+) peers\), best: #(?P<best>\d+) \([^\)]+\), finalized #(?P<finalized>\d+) \([^\)]+\), ⬇ (?P<down_speed>\d+\.\d+)(?P<down_unit>[kKmMgG][iI]?[bB]/s) ⬆ (?P<up_speed>\d+\.\d+)(?P<up_unit>[kKmMgG][iI]?[bB]/s)'
                )

                # Match the pattern
                match = pattern.search(log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
            
                bps = match.group("bps")
                target = match.group("target")
                peers = match.group("peers")
                best = match.group("best")
                finalized = match.group("finalized")
                down_speed = match.group("down_speed")
                down_unit = match.group("down_unit")
                up_speed = match.group("up_speed")
                up_unit = match.group("up_unit")

                event['event_data'] = {
                    'status': 'Syncing',
                    'peers': int(peers),
                    'best': int(best),
                    'target': int(target),
                    'finalized': int(finalized),
                    'bps': float(bps) if bps else None,
                    'down_speed': float(down_speed),
                    'up_speed': float(up_speed),
                    'down_unit': down_unit,
                    'up_unit': up_unit
                }

            if 'Pending' in log['event_data']:
                event['event_name'] = 'Pending'
                event['event_type'] = 'node'

                pattern = re.compile(
                    r'Consensus: substrate: ⏳ Pending \((?P<peers>\d+) peers\), best: #(?P<best>\d+) \([^\)]+\), finalized #(?P<finalized>\d+) \([^\)]+\), ⬇ (?P<down_speed>\d+\.\d+)(?P<down_unit>[kKmMgG][iI]?[bB])/s ⬆ (?P<up_speed>\d+\.\d+)(?P<up_unit>[kKmMgG][iI]?[bB])/s'
                )

                # Match the pattern
                match = pattern.search(log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                peers = match.group("peers")
                best = match.group("best")
                finalized = match.group("finalized")
                down_speed = match.group("down_speed")
                down_unit = match.group("down_unit")
                up_speed = match.group("up_speed")
                up_unit = match.group("up_unit")

                event['event_data'] = {
                    'status': 'Pending',
                    'peers': int(peers),
                    'best': int(best),
                    'target': None,
                    'finalized': int(finalized),
                    'bps': None,
                    'down_speed': float(down_speed),
                    'up_speed': float(up_speed),
                    'down_unit': down_unit,
                    'up_unit': up_unit
                }

            if 'Claimed' in log['event_data']:
                event['event_name'] = 'Claim'
                event['event_type'] = 'node'

                pattern = r'slot=(\d+)'
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                slot = int(match.group(1))

                event['event_data'] = {
                    'slot': slot,
                    'claim_type': "Vote" if "vote" in log['event_data'] else "Block"
                }

            if 'Imported #' in log['event_data']:
                event['event_name'] = 'Imported'
                event['event_type'] = 'node'

            if 'Reorg on #' in log['event_data']:
                event['event_name'] = 'Reorg'
                event['event_type'] = 'node'

            if 'Plotting sector retry sector_index' in log['event_data']:
                event['event_name'] = 'Retrying Plot Sector'
                event['event_type'] = 'farmer'

                farm_index_pattern = r"farm_index=(\d+)"
                sector_index_pattern = r"sector_index=(\d+)"

                farm_index_match = re.search(farm_index_pattern, log['event_data'])
                sector_index_match = re.search(sector_index_pattern, log['event_data'])

                if not farm_index_match or not sector_index_match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                farm_index = int(farm_index_match.group(1))
                sector_index = int(sector_index_match.group(1))

                event['event_data'] = {
                    'farm_index': farm_index,
                    'sector_index': sector_index
                }

            if 'Timed out without ping from plotter' in log['event_data']:
                event['event_name'] = 'Timed out Without Ping from Plotter'
                event['event_type'] = 'farmer'

                # Regular expression patterns to capture farm_index and sector_index
                farm_index_pattern = r'farm_index=(\d+)'
                sector_index_pattern = r'sector_index=(\d+)'

                # Extracting farm_index and sector_index using regex
                farm_index_match = re.search(farm_index_pattern, log['event_data'])
                sector_index_match = re.search(sector_index_pattern, log['event_data'])

                if not farm_index_match or not sector_index_match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None

                farm_index = int(farm_index_match.group(1))
                sector_index = int(sector_index_match.group(1))

                event['event_data'] = {
                    'farm_index': farm_index,
                    'sector_index': sector_index
                }

            if 'Farm expired and removed' in log['event_data']:
                event['event_name'] = 'Farm expired and removed'
                event['event_type'] = 'farmer'

                # Regular expression patterns to capture farm_index and farm_id
                farm_index_pattern = r'farm_index=(\d+)'
                farm_id_pattern = r'farm_id=([\w\d]+)'

                # Extracting farm_index and farm_id using regex
                farm_index_match = re.search(farm_index_pattern, log['event_data'])
                farm_id_match = re.search(farm_id_pattern, log['event_data'])

                if not farm_index_match or farm_id_match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)

                farm_index = farm_index_match.group(1)
                farm_id = farm_id_match.group(1)

                event['event_data'] = {
                    'farm_index': farm_index,
                    'farm_id': farm_id
                }

            if 'Farm exited successfully farm_index' in log['event_data']:
                event['event_name'] = 'Farm Farm Exited Successfully'
                event['event_type'] = 'farmer'

                farm_index_pattern = r'farm_index=(\d+)'

                farm_index_match = re.search(farm_index_pattern, log['event_data'])

                if not farm_index_match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None

                farm_index = farm_index_match.group(1)
                event['event_data'] = {
                    'farm_index': farm_index
                }

            if 'Solution was ignored' in log['event_data']:
                event['event_name'] = 'Solution was Ignored'
                event['event_type'] = 'farmer'

                # Regular expression patterns to capture slot, public_key, and sector_index
                slot_pattern = r'slot=(\d+)'
                public_key_pattern = r'public_key=([0-9a-f]+)'
                sector_index_pattern = r'sector_index=(\d+)'

                # Extracting values using regex
                slot_match = re.search(slot_pattern, log['event_data'])
                public_key_match = re.search(public_key_pattern, log['event_data'])
                sector_index_match = re.search(sector_index_pattern, log['event_data'])

                if not slot_match or not public_key_match or not sector_index_match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None

                slot = slot_match.group(1)
                public_key = public_key_match.group(1)
                sector_index = sector_index_match.group(1)

                event['event_data'] = {
                    'slot': slot,
                    'public_key': public_key,
                    'sector_index': sector_index
                }

            if not event['event_name'] and log["event_level"] != 'INFO': 
                event['event_name'] = log["event_level"]
                event['event_type'] = 'farmer'
                event['event_data'] = {
                    'message': log['event_data']
                }

            # if event['event_name'] and event['event_source'] == 'Delta Farmer': logger.info(event)
            if not event['event_name']: 
                # logger.info(log['event_data'])
                return None

            return event

        except Exception as e:
            logger.error(f"Error in parse_event {log}:", exc_info=e)

    @staticmethod
    def parse_log(log_str):
        try:
            log_pattern = re.compile(
                r'(?P<datetime>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z)\s+(?P<level>\w+)\s+(?P<data>.+)'
            )
            match = log_pattern.match(log_str)
            
            if match:
                return {
                    'event_datetime': Utils.normalize_date(match.group("datetime")),
                    'event_level': match.group("level"),
                    'event_data': match.group("data")
                }
            
            else:
                return None
            
        except Exception as e:
            logger.error(f"Error in parse_log:", exc_info=e)

    @staticmethod
    def handle_event(event, nexus_url, container_id, container_alias, container_type):
        try:
            create_event = True
            action_event = False

            # event_age = StreamParser.event_age(event)

            if event['event_name'] in ['Idle', 'Syncing', 'Pending', 'Claimed', 'Imported', 'Reorg']:
                create_event = False
                action_event = True

            if create_event: 
                action_event = Nexus.create_event(nexus_url, event)

            if not action_event: return None

            response = None

            event['event_data']['container_id'] = container_id
            data = event['event_data']

            if event['event_name'] == 'NATs Connected':
                # Update the entity that is connected
                response = Nexus.upsert_entity(nexus_url, container_type, data)

            if event['event_name'] == 'NATs Disconnected':
                # Update the entity that is disconnected
                response = Nexus.upsert_entity(nexus_url, container_type, data)

            if event['event_name'] == 'Register RPC URL':
                response = Nexus.upsert_entity(nexus_url, container_type, data)
                
            if event['event_name'] == 'Detecting L3 Cache Groups':
                response = Nexus.upsert_entity(nexus_url, container_type, data)
 
            if event['event_name'] == 'Preparing Plotting Thread Pools':
                response = Nexus.upsert_entity(nexus_url, container_type, data)

            if event['event_name'] == 'Checking Plot Cache Contents':
                response = Nexus.upsert_entity(nexus_url, event['event_type'], event)

            if event['event_name'] == 'Finished Checking Plot Cache Contents':
                response = Nexus.upsert_entity(nexus_url, event['event_type'], event)

            if event['event_name'] == 'Downloading Segment Headers':
                response = Nexus.upsert_entity(nexus_url, container_type, data)

            if event['event_name'] == 'Downloaded All Segment Headers':
                response = Nexus.upsert_entity(nexus_url, container_type, data)

            if event['event_name'] == 'Benchmarking Proving Method':
                response = Nexus.upsert_entity(nexus_url, event['event_type'], event)

            if event['event_name'] == 'Subscribing to Slot Info Notifications':
                response = Nexus.upsert_entity(nexus_url, event['event_type'], event)

            if event['event_name'] == 'Subscribing to reward signing notifications':
                response = Nexus.upsert_entity(nexus_url, event['event_type'], event)

            if event['event_name'] == 'Subscribing to archived segments':
                response = Nexus.upsert_entity(nexus_url, event['event_type'], event)

            if event['event_name'] == 'Found Fastest Mode':
                response = Nexus.upsert_entity(nexus_url, event['event_type'], event)

            if event['event_name'] == 'Register Farm ID':
                response = Nexus.upsert_entity(nexus_url, event['event_type'], event)

            if event['event_name'] == 'Register Genesis Hash':
                response = Nexus.upsert_entity(nexus_url, event['event_type'], event)

            if event['event_name'] == 'Register Public Key':
                response = Nexus.upsert_entity(nexus_url, event['event_type'], event)

            if event['event_name'] == 'Register Allocated Space':
                response = Nexus.upsert_entity(nexus_url, event['event_type'], event)

            if event['event_name'] == 'Register Directory':
                response = Nexus.upsert_entity(nexus_url, event['event_type'], event)

            if event['event_name'] == 'Collecting Plotted Pieces':
                response = Nexus.upsert_entity(nexus_url, container_type, data)

            if event['event_name'] == 'Finished Collecting Plotted Pieces':
                response = Nexus.upsert_entity(nexus_url, container_type, data)

            if event['event_name'] == 'Initializing Piece Cache':
                response = Nexus.upsert_entity(nexus_url, container_type, data)

            if event['event_name'] == 'Syncronizing Piece Cache':
                response = Nexus.upsert_entity(nexus_url, container_type, data)

            if event['event_name'] == 'Piece Cache Sync':
                response = Nexus.upsert_entity(nexus_url, container_type, data)

            if event['event_name'] == 'Finished piece cache synchronization':
                response = Nexus.upsert_entity(nexus_url, container_type, data)

            if event['event_name'] == 'Plotting Sector':
                response = Nexus.upsert_entity(nexus_url, event['event_type'], event)
                Nexus.insert_entity(nexus_url, 'plot', event)

            if event['event_name'] == 'Signed Reward Hash':
                response = Nexus.insert_entity(nexus_url, 'reward', event)

            if event['event_name'] == 'Initial Plotting Complete':
                response = Nexus.upsert_entity(nexus_url, event['event_type'], event)
                Nexus.insert_entity(nexus_url, 'plot', event)

            if event['event_name'] == 'Replotting Sector':
                response = Nexus.upsert_entity(nexus_url, event['event_type'], event)
                Nexus.insert_entity(nexus_url, 'plot', event)

            if event['event_name'] == 'Replotting Complete':
                response = Nexus.upsert_entity(nexus_url, event['event_type'], event)
                Nexus.insert_entity(nexus_url, 'plot', event)

            if event['event_name'] == 'Failed to Send Solution':
                response = Nexus.insert_entity(nexus_url, 'reward', event)

            if event['event_name'] in ['Idle', 'Preparing', 'Syncing', 'Pending']:
                age = StreamParser.event_age(event)
                if age < 1:
                    response = Nexus.upsert_entity(nexus_url, container_type, data)

            if event['event_name'] == 'Claim':
                response = Nexus.insert_entity(nexus_url, 'claim', event)

            if event['event_name'] in ['Plot Sector Request', 'Finished Plotting Sector']:
                response = Nexus.upsert_entity(nexus_url, container_type, data)
            
            if response: 
                logger.info(f"{event['event_name']} on {container_type}: {response}")

            # else:
            #     if event['event_level'] != 'WARN':
            #         logger.info(event)
        except Exception as e:
            logger.error(f"Error in handle_event {event}:", exc_info=e)
            time.sleep(5)

    @staticmethod
    def start_parse(container_data, docker_client, stop_event, nexus_url):
        container_id = container_data['container_id']
        container_alias = container_data['container_alias']
        container_type = container_data['container_type']

        # Upsert the initial data w/ container_id to be sure it exists
        if container_type != 'node': StreamParser.upsert_initial(nexus_url, container_id, container_alias, container_type)

        logger.info(f"Starting Stream Parser for {container_alias} of type {container_type}.")

        # Get the container so we can stream the logs and get other information
        container = docker_client.containers.get(container_id)

        if not container:
            logger.error(f"Unable to get container for container id: {container_id}")
            sys.exit(1)

        # Get specific cluster information from the docker-compose command
        if container_type == 'cluster_cache': StreamParser.upsert_cache_attributes(container, nexus_url, container_id, container_type)
        if container_type == 'cluster_controller': StreamParser.upsert_controller_attributes(container, nexus_url, container_id, container_type)
        if container_type == 'cluster_farmer': StreamParser.upsert_cluster_farmer_attributes(container, nexus_url, container_id, container_type)

        while not stop_event.is_set():
            try:
                container.reload()

                if container.status != 'running':
                    logger.warn(f"Container must be running, current status: {container.status}")
                    stop_event.wait(30)
                    continue
                
                response = Nexus.get_latest_events(nexus_url, container_id)

                if len(response.get('data')) > 0:
                    logger.info(f"Getting Logs Since: {response.get('data')[0].get('event_datetime')} for {container_alias}")
                    start = datetime.strptime(response.get('data')[0].get('event_datetime'), "%Y-%m-%d %H:%M:%S")
                else: 
                    start = datetime.min.replace(tzinfo=timezone.utc)

                generator = container.logs(since=start, stdout=True, stderr=True, stream=True)
                for log in generator:
                    try:
                        if stop_event.is_set():
                            break

                        parsed_log = StreamParser.parse_log(log.decode('utf-8').strip())
                        if not parsed_log:
                            continue

                        event = StreamParser.parse_event(parsed_log, container_id, container_alias, container_type)
                        if not event:
                            continue

                        StreamParser.handle_event(event, nexus_url, container_id, container_alias, container_type)

                    except Exception as e:
                        logger.error(f"Error in generator for container {container_alias}:", exc_info=e)

            except Exception as e:
                logger.error(f"Error in monitor_stream for container {container_alias}", exc_info=e)