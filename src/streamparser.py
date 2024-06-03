import sys
import re
import time

from datetime import datetime, timezone

from src.logger import logger
from src.utils import Utils


class StreamParser:
    @staticmethod
    def extract_cpu_sets(text):
        cpu_sets = re.findall(r'CpuSet\((.*?)\)', text)
        split_values = []
        for cpu_set in cpu_sets:
            split_values.extend(cpu_set.split(','))
        return split_values    
    
    @staticmethod
    def parse_event(log, name):
        try:

            event = {
                'event_name': None,
                'event_type': None,
                'event_level': log["event_level"],
                'event_datetime': log["event_datetime"],
                'event_source': name,
                'event_data': None,
            }

            if 'Connecting to node RPC url' in log['event_data']:
                event['event_name'] = 'Register RPC URL'
                event['event_type'] = 'Farmer'
                pattern = r'ws://([^ ]+)'
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None

                rpc_url = match.group(1)
                event['event_data'] = {
                    'Farmer Status': 'Initializing',
                    'RPC URL': rpc_url
                }
                    
            if 'l3_cache_groups' in log['event_data']:
                event['event_name'] = 'Detecting L3 Cache Groups'
                event['event_type'] = 'Farmer'
                pattern = r'l3_cache_groups=(\d+)'
                match = re.search(pattern, log['event_data'])
                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                l3_cache_groups_value = match.group(1)
                event['event_data'] = {
                    "Farmer Status": "Initializing",
                    'L3 Cache Groups': l3_cache_groups_value
                }

            if 'plotting_thread_pool_core_indices' in log['event_data']:
                event['event_name'] = 'Preparing Plotting Thread Pools'
                event['event_type'] = 'Farmer'

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
                    'Farmer Status': "Initializaing",
                    'Plotting CPU Sets': ', '.join(plotting_values),
                    'Replotting CPU Sets': ', '.join(replotting_values)
                }

            if 'Checking plot cache contents' in log['event_data']:
                event['event_name'] = 'Checking Plot Cache Contents'
                event['event_type'] = 'Farmer'
                event['event_data'] = {
                    'Farmer Status': 'Checking Plot Cache Contents'
                }

            if 'Finished checking plot cache contents' in log['event_data']:
                event['event_name'] = 'Finished Checking Plot Cache Contents'
                event['event_type'] = 'Farmer'
                event['event_data'] = {
                    'Status': 'Finished Checking Plot Cache Contents'
                }

            if 'Benchmarking faster proving method' in log['event_data']:
                event['event_name'] = 'Benchmarking Proving Method'
                event['event_type'] = 'Farm'
                event['event_data'] = {
                    'Status': 'Benchmarking Proving Methods'
                }

            if 'Subscribing to slot info notifications' in log['event_data']:
                event['event_name'] = 'Subscribing to Slot Info Notifications'
                event['event_type'] = 'Farm'

                farm_index_pattern = r'farm_index=(\d+)'
                farm_index_match = re.search(farm_index_pattern, log['event_data'])

                if not farm_index_match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None

                event['event_data'] = {
                    'Status': 'Subscribing to Slot Info Notifications',
                    'Farm Index': farm_index_match.group(1)
                }

            if 'Subscribing to reward signing notifications' in log['event_data']:
                event['event_name'] = 'Subscribing to Reward Signing Notifications'
                event['event_type'] = 'Farm'

                farm_index_pattern = r'farm_index=(\d+)'
                farm_index_match = re.search(farm_index_pattern, log['event_data'])

                if not farm_index_match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None

                event['event_data'] = {
                    'Status': 'Subscribing to Reward Signing Notifications',
                    'Farm Index': farm_index_match.group(1)
                }

            if 'Subscribing to archived segments' in log['event_data']:
                event['event_name'] = 'Subscribing to Archived Segments'
                event['event_type'] = 'Farm'

                farm_index_pattern = r'farm_index=(\d+)'
                farm_index_match = re.search(farm_index_pattern, log['event_data'])

                if not farm_index_match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None

                event['event_data'] = {
                    'Status': 'Subscribing to Archived Segments',
                    'Farm Index': farm_index_match.group(1)
                }

            if 'fastest_mode' in log['event_data']:
                event['event_name'] = 'Found Fastest Mode'
                event['event_type'] = 'Farm'
                pattern = r"\{farm_index=(\d+)\}.*fastest_mode=(\w+)"
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                event['event_data'] = {
                    'Farm Index': match.group(1),
                    'Fastest Mode': match.group(2),
                    'Farm Status': 'Initializing'
                }

            if 'ID:' in log['event_data']:
                event['event_name'] = 'Register Farm ID'
                event['event_type'] = 'Farm'
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
                    'Farm Index': farm_index_value,
                    'Farm ID': farm_id_value,
                    'Farm Status': 'Initializing'
                }

            if 'Genesis hash:' in log['event_data']:
                event['event_name'] = 'Register Genesis Hash'
                event['event_type'] = 'Farm'

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
                    'Farm Index': farm_index_value,
                    'Farm Genesis Hash': genesis_hash_value,
                    'Farm Status': 'Initializing'
                }

            if 'Public key:' in log['event_data']:
                event['event_name'] = 'Register Public Key'
                event['event_type'] = 'Farm'

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
                    'Farm Index': farm_index_value,
                    'Farm Public Key': public_key_value,
                    'Farm Status': 'Initializing'
                }

            if 'Allocated space:' in log['event_data']:
                event['event_name'] = 'Register Allocated Space'
                event['event_type'] = 'Farm'
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
                    'Farm Index': int(match.group(1)),
                    'Farm Size': allocated_gib,
                    'Farm Status': 'Initializing'                
                }

            if 'Directory:' in log['event_data']:
                event['event_name'] = 'Register Directory'
                event['event_type'] = 'Farm'

                pattern = r'farm_index=(\d+).*Directory:\s+(.+)'
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                event['event_data'] = {
                    'Farm Index': int(match.group(1)),
                    'Farm Directory': match.group(2),
                    'Farm Status': 'Farming'
                }

            if 'Collecting already plotted pieces' in log['event_data']:
                event['event_name'] = 'Collecting Plotted Pieces'
                event['event_type'] = 'Farmer'
                event['event_data'] = {
                    'Status': 'Collecting Plotted Pieces'
                }

            if 'Finished collecting already plotted pieces successfully' in log['event_data']:
                event['event_name'] = 'Finished Collecting Plotted Pieces'
                event['event_type'] = 'Farmer'
                event['event_data'] = {
                    'Status': 'Finished Collecting Plotted Pieces'
                }

            if 'Initializing piece cache' in log['event_data']:
                event['event_name'] = 'Initializing Piece Cache'
                event['event_type'] = 'Farmer'
                event['event_data'] = {
                    'Status': 'Initializing Piece Cache'
                }

            if 'Synchronizing piece cache' in log['event_data']:
                event['event_name'] = 'Syncronizing Piece Cache'
                event['event_type'] = 'Farmer'
                event['event_data'] = {
                    'Status': 'Syncronizing Piece Cache'
                }

            if 'Piece cache sync' in log['event_data']:
                event['event_name'] = 'Piece Cache Sync'
                event['event_type'] = 'Farmer'

                pattern = r'Piece cache sync (\d+\.\d+)% complete'
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                event['event_data'] = {
                    'Farmer Status': 'Syncronizing Piece Cache',
                    'Piece Cache Percent': float(match.group(1))
                }

            if 'Finished piece cache synchronization' in log['event_data']:
                event['event_name'] = 'Finished Piece Cache Syncronization'
                event['event_type'] = 'Farmer'
                event['event_data'] = {
                    'Farmer Status': 'Piece Cache Syncronized',
                    'Piece Cache Percent': 100.00
                }

            if 'Plotting sector' in log['event_data']:
                event['event_name'] = 'Plotting Sector'
                event['event_type'] = 'Farm'

                pattern = r'farm_index=(\d+).*?(\d+\.\d+)% complete.*?sector_index=(\d+)'      
                match = re.search(pattern, log['event_data'])
                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                event['event_data'] = {
                    'Farm Index': int(match.group(1)),
                    'Plot Percentage': float(match.group(2)),
                    'Plot Current Sector': int(match.group(3)),
                    'Plot Type': 'Plot',
                    'Farm Status': 'Plotting Sector'
                }

            if 'Successfully signed reward hash' in log['event_data']:
                event['event_name'] = 'Signed Reward Hash'
                event['event_type'] = 'Farm'
                pattern = r'farm_index=(\d+).*hash\s(0x[0-9a-fA-F]+)'
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None

                event['event_data'] = {
                    'Farm Index': match.group(1),
                    'Reward Hash': match.group(2),
                    'Reward Type': 'Reward',
                }

            if 'Initial plotting complete' in log['event_data']:
                event['event_name'] = 'Initial Plotting Complete'
                event['event_type'] = 'Farm'
                pattern = r"farm_index=(\d+)"
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                event['event_data'] = {
                    'Farm Index': int(match.group(1)),
                    'Plot Percentage': 100,
                    'Plot Current Sector': None,
                    'Plot Type': 'Plot',
                    'Farm Status': 'Farming'
                }

            if 'Replotting sector' in log['event_data']:
                event['event_name'] = 'Replotting Sector'
                event['event_type'] = 'Farm'
                pattern = r'farm_index=(\d+).*?(\d+\.\d+)% complete.*?sector_index=(\d+)'      
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                event['event_data'] = {
                    'Farm Index': int(match.group(1)),
                    'Plot Percentage': float(match.group(2)),
                    'Plot Current Sector': int(match.group(3)),
                    'Plot Type': 'Replot',
                    'Farm Status': 'Replotting Sector'
                } 

            if 'Replotting complete' in log['event_data']:
                event['event_name'] = 'Replotting Complete'
                event['event_type'] = 'Farm'
                pattern = r"farm_index=(\d+)"
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                event['event_data'] = {
                    'Farm Index': int(match.group(1)),
                    'Plot Percentage': 100,
                    'Plot Current Sector': None,
                    'Plot Type': 'Replot',
                    'Farm Status': 'Farming'
                }

            if 'Failed to send solution' in log['event_data']:
                event['event_name'] = 'Failed to Send Solution'
                event['event_type'] = 'Farmer'

                pattern = r"farm_index=(\d+)"
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                event['event_data'] = {
                    'Farm Index': int(match.group(1)),
                    'Reward Hash': None,
                    'Reward Type': 'Failed',
                }

            if 'Plot sector request' in log['event_data'] or 'Finished plotting sector successfully' in log['event_data']:
                if 'Plot sector request' in log['event_data']: event['event_name'] = 'Plot Sector Request'
                else: event['event_name'] = 'Finished Plotting Sector'

                event['event_type'] = 'Farmer'

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
                    'Public Key': public_key,
                    'Sector Index': sector_index
                }

            if 'New cache discovered' in log['event_data']:
                event['event_name'] = 'New Cache Discovered'
                event['event_type'] = 'Farmer'

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
                    'Cache ID': cache_id
                }

            if 'Discovered new farm' in log['event_data'] or 'Farm initialized successfully' in log['event_data']:
                if 'Discovered new farm' in log['event_data']: event['event_name'] = 'Discovered New Farm'
                else: event['event_name'] = 'Farm Initialized Successfully'

                event['event_type'] = 'Farmer'

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
                    'Farm ID': farm_id,
                    'Farm Index': farm_index
                }

            if 'Idle' in log['event_data']:
                event['event_name'] = 'Idle'
                event['event_type'] = 'Node'
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
                    'Status': 'Idle',
                    'Peers': int(peers),
                    'Best': int(best),
                    'Target': None,
                    'Finalized': int(finalized),
                    'BPS': None,
                    'Down Speed': float(down_speed),
                    'Up Speed': float(up_speed),
                    'Down Unit': down_unit,
                    'Up Unit': up_unit
                }
            
            if 'Preparing' in log['event_data']:
                event['event_name'] = 'Preparing'
                event['event_type'] = 'Node'

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
                    'Status': 'Preparing',
                    'Peers': int(peers),
                    'Best': int(best),
                    'Target': int(target),
                    'Finalized': int(finalized),
                    'BPS': float(bps) if bps else None,
                    'Down Speed': float(down),
                    'Up Speed': float(up),
                    'Down Unit': down_unit,
                    'Up Unit': up_unit
                }

            if 'Syncing' in log['event_data']:
                event['event_name'] = 'Syncing'
                event['event_type'] = 'Node'

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
                    'Status': 'Syncing',
                    'Peers': int(peers),
                    'Best': int(best),
                    'Target': int(target),
                    'Finalized': int(finalized),
                    'BPS': float(bps) if bps else None,
                    'Down Speed': float(down_speed),
                    'Up Speed': float(up_speed),
                    'Down Unit': down_unit,
                    'Up Unit': up_unit
                }

            if 'Pending' in log['event_data']:
                event['event_name'] = 'Pending'
                event['event_type'] = 'Node'

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
                    'Status': 'Pending',
                    'Peers': int(peers),
                    'Best': int(best),
                    'Target': None,
                    'Finalized': int(finalized),
                    'BPS': None,
                    'Down Speed': float(down_speed),
                    'Up Speed': float(up_speed),
                    'Down Unit': down_unit,
                    'Up Unit': up_unit
                }

            if 'Claimed' in log['event_data']:
                event['event_name'] = 'Claim'
                event['event_type'] = 'Node'

                pattern = r'slot=(\d+)'
                match = re.search(pattern, log['event_data'])

                if not match:
                    logger.error(f"No match for: {log['event_data']}")
                    time.sleep(5)
                    return None
                
                slot = int(match.group(1))

                event['event_data'] = {
                    'Slot': slot,
                    'Claim Type': "Vote" if "vote" in log['event_data'] else "Block"
                }

            if not event['event_name'] and log["event_level"] != 'INFO': 
                event['event_name'] = log["event_level"]
                event['event_type'] = 'Farmer'
                event['event_data'] = {
                    'message': log['event_data']
                }

            if event['event_name'] and event['event_source'] == 'Delta Farmer': logger.info(event)
            if not event['event_name']: return None

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
    def handle_event(event, nexus_url):
        pass

    @staticmethod
    def start_parse(container_data, docker_client, stop_event, nexus_url):
        container_id = container_data['container_id']
        container_alias = container_data['container_alias']
        container_type = container_data['container_type']

        logger.info(f"Starting Stream Parser for {container_alias} of type {container_type}.")

        container = docker_client.containers.get(container_id)

        if not container:
            logger.error(f"Unable to get container for container id: {container_id}")
            sys.exit(1)

        while not stop_event.is_set():
            try:
                container.reload()

                if container.status != 'running':
                    logger.warn(f"Container must be running, current status: {container.status}")
                    stop_event.wait(30)
                    continue

                start = datetime.min.replace(tzinfo=timezone.utc)
                generator = container.logs(since=start, stdout=True, stderr=True, stream=True)
                for log in generator:
                    try:
                        if stop_event.is_set():
                            break

                        parsed_log = StreamParser.parse_log(log.decode('utf-8').strip())
                        if not parsed_log:
                            # logger.warn(f"Unable to parse log: {log}")
                            continue

                        event = StreamParser.parse_event(parsed_log, container_alias)
                        if not event:
                            continue



                    except Exception as e:
                        logger.error(f"Error in generator for container {container_alias}:", exc_info=e)

            except Exception as e:
                logger.error(f"Error in monitor_stream for container {container_alias}", exc_info=e)