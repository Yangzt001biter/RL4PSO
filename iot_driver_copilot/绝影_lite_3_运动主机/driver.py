import os
import yaml
import asyncio
import logging
import json
import signal

from kubernetes import client, config
from kubernetes.client.rest import ApiException

import paho.mqtt.client as mqtt
import aiohttp

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeviceShifuLite3")

# Environment variables (all required to run)
EDGEDEVICE_NAME = os.environ["EDGEDEVICE_NAME"]
EDGEDEVICE_NAMESPACE = os.environ["EDGEDEVICE_NAMESPACE"]
MQTT_BROKER = os.environ["MQTT_BROKER"]
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USERNAME = os.environ.get("MQTT_USERNAME")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
MQTT_TELEMETRY_TOPIC = os.environ.get("MQTT_TELEMETRY_TOPIC", "device/telemetry/state")
MQTT_COMMAND_TOPIC = os.environ.get("MQTT_COMMAND_TOPIC", "device/commands/control")
MQTT_RESPONSE_TOPIC = os.environ.get("MQTT_RESPONSE_TOPIC", "device/commands/response")
MQTT_QOS = int(os.environ.get("MQTT_QOS", "1"))
CONFIG_INSTRUCTIONS_PATH = "/etc/edgedevice/config/instructions"
# No device HTTP endpoint env - will get from EdgeDevice CRD

# Kubernetes CRD Constants
CRD_GROUP = "shifu.edgenesis.io"
CRD_VERSION = "v1alpha1"
CRD_PLURAL = "edgedevices"

class EdgeDeviceStatusUpdater:
    def __init__(self, device_name, namespace):
        config.load_incluster_config()
        self.api = client.CustomObjectsApi()
        self.device_name = device_name
        self.namespace = namespace

    async def set_phase(self, phase):
        # Patch the CRD status
        patch = {"status": {"edgeDevicePhase": phase}}
        try:
            self.api.patch_namespaced_custom_object_status(
                group=CRD_GROUP,
                version=CRD_VERSION,
                namespace=self.namespace,
                plural=CRD_PLURAL,
                name=self.device_name,
                body=patch,
            )
            logger.info(f"Set EdgeDevice status.phase to {phase}")
        except ApiException as e:
            logger.error(f"Failed to update EdgeDevice phase: {e}")

    def get_device_address(self):
        try:
            obj = self.api.get_namespaced_custom_object(
                group=CRD_GROUP,
                version=CRD_VERSION,
                namespace=self.namespace,
                plural=CRD_PLURAL,
                name=self.device_name,
            )
            return obj["spec"]["address"]
        except Exception as e:
            logger.error(f"Unable to get device address from EdgeDevice CRD: {e}")
            return None

class DeviceShifuLite3:
    def __init__(self):
        self.updater = EdgeDeviceStatusUpdater(EDGEDEVICE_NAME, EDGEDEVICE_NAMESPACE)
        self.config = self.load_instruction_config()
        self.device_address = None
        self.mqtt_client = None
        self.aiohttp_session = aiohttp.ClientSession()
        self.loop = asyncio.get_event_loop()
        self.connected = asyncio.Event()
        self.stop_event = asyncio.Event()

    def load_instruction_config(self):
        config_path = os.path.join(CONFIG_INSTRUCTIONS_PATH, "instructions.yaml")
        try:
            with open(config_path, "r") as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load instruction config: {e}")
            return {}

    def start(self):
        self.loop.run_until_complete(self.main())

    async def main(self):
        # Step 1: Get device address from EdgeDevice CRD
        await self.updater.set_phase("Pending")
        self.device_address = self.updater.get_device_address()
        if not self.device_address:
            await self.updater.set_phase("Unknown")
            return

        # Step 2: Connect to MQTT broker
        await self.updater.set_phase("Pending")
        if not await self.connect_mqtt():
            await self.updater.set_phase("Failed")
            return

        await self.updater.set_phase("Running")
        logger.info("DeviceShifuLite3 started and running.")

        # Step 3: Start telemetry polling
        telemetry_task = self.loop.create_task(self.poll_telemetry())

        # Step 4: Wait for stop event (SIGTERM/SIGINT)
        for sig in (signal.SIGINT, signal.SIGTERM):
            self.loop.add_signal_handler(sig, lambda: asyncio.ensure_future(self.shutdown()))
        await self.stop_event.wait()
        telemetry_task.cancel()
        await self.aiohttp_session.close()
        self.mqtt_client.disconnect()
        await asyncio.sleep(0.5)

    async def shutdown(self):
        logger.info("Shutting down DeviceShifuLite3 ...")
        await self.updater.set_phase("Unknown")
        self.stop_event.set()

    async def connect_mqtt(self):
        connected_event = asyncio.Event()

        def on_connect(client, userdata, flags, rc):
            if rc == 0:
                logger.info("Connected to MQTT broker")
                connected_event.set()
                # Subscribe to command topic
                client.subscribe(MQTT_COMMAND_TOPIC, qos=MQTT_QOS)
            else:
                logger.error(f"MQTT connection failed (rc={rc})")

        def on_disconnect(client, userdata, rc):
            logger.warning("Disconnected from MQTT broker")
            self.loop.create_task(self.updater.set_phase("Pending"))

        def on_message(client, userdata, msg):
            self.loop.create_task(self.handle_mqtt_command(msg))

        self.mqtt_client = mqtt.Client()
        if MQTT_USERNAME:
            self.mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        self.mqtt_client.on_connect = on_connect
        self.mqtt_client.on_disconnect = on_disconnect
        self.mqtt_client.on_message = on_message

        try:
            self.mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
            # Use loop_start for threading, but manage shutdown via asyncio
            self.mqtt_client.loop_start()
            try:
                await asyncio.wait_for(connected_event.wait(), timeout=10)
                return True
            except asyncio.TimeoutError:
                logger.error("Timeout connecting to MQTT broker")
                return False
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            return False

    async def poll_telemetry(self):
        interval = int(self.config.get("device/telemetry/state", {})
                       .get("protocolPropertyList", {}).get("pollingInterval", 2))
        while True:
            await asyncio.sleep(interval)
            try:
                telemetry = await self.fetch_telemetry()
                if telemetry is not None:
                    self.mqtt_client.publish(
                        MQTT_TELEMETRY_TOPIC,
                        json.dumps(telemetry),
                        qos=MQTT_QOS,
                    )
            except Exception as e:
                logger.error(f"Polling telemetry failed: {e}")

    async def fetch_telemetry(self):
        # HTTP GET telemetry endpoint
        url = f"http://{self.device_address}/api/v1/telemetry"
        try:
            async with self.aiohttp_session.get(url, timeout=3) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data
                else:
                    logger.error(f"Failed to fetch telemetry: HTTP {resp.status}")
        except Exception as e:
            logger.error(f"Error getting telemetry: {e}")
        return None

    async def handle_mqtt_command(self, msg):
        # Payload expected: { "command": "...", "params": {...} }
        try:
            payload = msg.payload.decode()
            command_obj = json.loads(payload)
            # Optionally consult config for command mapping/settings
            result = await self.send_device_command(command_obj)
            response = {
                "status": "success" if result else "failure",
                "command": command_obj.get("command"),
                "result": result
            }
        except Exception as e:
            response = {
                "status": "failure",
                "error": str(e)
            }
            logger.error(f"Error handling command: {e}")

        self.mqtt_client.publish(
            MQTT_RESPONSE_TOPIC,
            json.dumps(response),
            qos=MQTT_QOS
        )

    async def send_device_command(self, command_obj):
        # HTTP POST to device with command payload
        url = f"http://{self.device_address}/api/v1/command"
        try:
            async with self.aiohttp_session.post(
                url, json=command_obj, timeout=5
            ) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    logger.error(f"Device command failed: HTTP {resp.status}")
                    return None
        except Exception as e:
            logger.error(f"Error sending device command: {e}")
            return None

if __name__ == "__main__":
    shifu = DeviceShifuLite3()
    shifu.start()