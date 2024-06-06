import docker
import threading
import sys

from src.logger import logger
import src.constants as constants
from src.streamparser import StreamParser
from src.resourcemonitor import ResourceMonitor

class Hubble:
    def __init__(self, config) -> None:

        self.host_ip = config['host_ip']
        self.nexus_url = config['nexus_url']
        self.docker_client = docker.from_env()
        self.stop_event = threading.Event()
        self.containers = []

    def get_container_type(self, command):
        if 'cache' in command and 'cluster' in command:
            container_type = 'cluster_cache'
        elif 'controller' in command and 'cluster' in command:
            container_type = 'cluster_controller'
        elif 'farmer' in command and 'cluster' in command:
            container_type = 'cluster_farmer'
        elif 'plotter' in command and 'cluster' in command:
            container_type = 'cluster_plotter'
        else:
            container_type = 'farmer'

        return container_type

    def get_containers(self):
        try:
            containers = self.docker_client.containers.list(all=True)
            for container in containers:
                if 'subspace/node' in container.image.tags[0]:
                    container_label = container.attrs['Config']['Labels'].get('com.subspace.name')
                    self.containers.append({
                        'container_type': 'node',
                        'container_id': container.id,
                        'container_name': container.name,
                        'container_label': container_label,
                        'container_alias': container_label if container_label else container.name
                    })
                    

                elif 'subspace/farmer' in container.image.tags[0]:
                    container_label = container.attrs['Config']['Labels'].get('com.subspace.name')
                    self.containers.append({
                        'container_type': self.get_container_type(container.attrs['Config']['Cmd']),
                        'container_id': container.id,
                        'container_name': container.name,
                        'container_label': container_label,
                        'container_alias': container_label if container_label else container.name
                    })

        except Exception as e:
            logger.error(f'Error getting container:', exc_info=e)
            sys.exit(1)

    def run(self):
        try:
            self.get_containers()  # Assuming this method populates self.containers

            for container in self.containers:
                logger.info(container)

            

            threads = []

            # Start the ResourceMonitor in a separate thread
            resource_monitor_thread = threading.Thread(
                target=ResourceMonitor.start_monitor,
                args=(self.containers, self.docker_client, self.stop_event, self.nexus_url, self.host_ip)
            )
            threads.append(resource_monitor_thread)
            resource_monitor_thread.start()

            # Start a thread for each container
            for container in self.containers:
                if container['container_type'] in ['node', 'farmer', 'cluster_farmer', 'cluster_cache', 'cluster_plotter', 'cluster_controller']:
                    thread = threading.Thread(
                        target=StreamParser.start_parse,
                        args=(container, self.docker_client, self.stop_event, self.nexus_url)
                    )
                    threads.append(thread)
                    thread.start()

            # Join all threads to wait for their completion
            for thread in threads:
                thread.join()

        except KeyboardInterrupt:
            print("Stop signal received. Gracefully shutting down monitors.")
            self.stop_event.set()  # Ensure the stop event is set
