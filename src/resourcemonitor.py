from src.logger import logger
from src.utils import Utils
from src.nexus import Nexus

class ResourceMonitor:
    @staticmethod
    def get_container_ip(container):
        try:
            # Get the network mode of the container
            network_mode = container.attrs.get('HostConfig', {}).get('NetworkMode', '')
            
            # Get the IP address based on the network mode
            container_ip = container.attrs.get('NetworkSettings', {}).get('Networks', {}).get(network_mode, {}).get('IPAddress', '')
            
            # If IP address is not found using the network mode, search through the networks
            if not container_ip:
                networks = container.attrs.get('NetworkSettings', {}).get('Networks', {})
                for network_name, network_data in networks.items():
                    if network_data.get('NetworkID') == network_mode:
                        container_ip = network_data.get('IPAddress', '')
                        break

            return container_ip
        except Exception as e:
            print(f"Error getting container IP: {e}")
            return None

    @staticmethod
    def monitor_host(docker_client, host_ip):
        try:
            host = docker_client.info()

            return {
                'host_name': host['Name'],
                'host_os': host['OperatingSystem'],
                'host_cpus': host['NCPU'],
                'host_memory': round(host['MemTotal'] / (1024 ** 3), 2),
                'host_ip': host_ip,
            }

        except Exception as e:
            logger.error("Error monitoring host:", exc_info=e)

    @staticmethod
    def monitor_container(docker_client, container_data, host_name):
        try:
            container = docker_client.containers.get(container_data['container_id'])
            container.reload()

            container_ip = ResourceMonitor.get_container_ip(container)

            stats = container.stats(stream=False)
            memory_usage = stats['memory_stats']['stats']['active_anon'] + stats['memory_stats']['stats']['active_file']
            memory_limit = stats['memory_stats']['limit']
            total_usage = stats['cpu_stats']['cpu_usage']['total_usage']
            system_cpu_usage = stats['cpu_stats']['system_cpu_usage']

            # Calculate memory usage percentage
            memory_usage_percentage = round((memory_usage / memory_limit) * 100, 2)

            # Calculate CPU usage percentage
            cpu_delta = total_usage - stats['precpu_stats']['cpu_usage']['total_usage']
            system_cpu_delta = system_cpu_usage - stats['precpu_stats']['system_cpu_usage']
            cpu_usage_percentage = round((cpu_delta / system_cpu_delta) * 100, 2)

            return {
                'container_id': container_data['container_id'],
                'host_name': host_name,
                'container_name': container_data['container_name'],
                'container_alias': container_data['container_alias'],
                'container_image': container.image.labels["org.opencontainers.image.version"],
                'container_status': container.status,
                'container_started_at': Utils.normalize_date(container.attrs.get('State').get('StartedAt')),
                'container_ip': container_ip,
                'container_mem_usage_pct': memory_usage_percentage,
                'container_cpu_usage_pct': cpu_usage_percentage
            }


        except Exception as e:
            logger.error("Error monitoring container:", exc_info=e)

    
    @staticmethod
    def start_monitor(containers, docker_client, stop_event, nexus_url, host_ip):
        host = ResourceMonitor.monitor_host(docker_client, host_ip)
        Nexus.upsert_entity(nexus_url, 'host', host)

        while not stop_event.is_set():
            for container_data in containers:
                container = ResourceMonitor.monitor_container(docker_client, container_data, host['host_name'])
                Nexus.upsert_entity(nexus_url, 'container', container)

            stop_event.wait(10)