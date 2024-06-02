import sys

from datetime import datetime, timezone

from src.logger import logger

class StreamParser:
    @staticmethod
    def start_parse(container_data, docker_client, stop_event, nexus_url):
        logger.info(container_data)
        container_id = container_data['container_id']
        container_alias = container_data['container_label'] if container_data.get('container_label') else container_data['container_name']
        container_type = container_data['container_type']

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

                        if container_type != 'node':
                            logger.info(f"{container_alias}: {log.decode('utf-8').strip()}")

                    except Exception as e:
                        logger.error(f"Error in generator for container {container_alias}:", exc_info=e)

            except Exception as e:
                logger.error(f"Error in monitor_stream for container {container_alias}", exc_info=e)