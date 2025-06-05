import os
import sys
import yaml
import json
import asyncio
import signal
import logging
import threading

import requests
from kubernetes import client, config
from kubernetes.client import ApiException
import paho.mqtt.client as mqtt

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s'
)

# Environment Variables
EDGEDEVICE_NAME = os.environ['EDGEDEVICE_NAME']
EDGEDEVICE_NAMESPACE = os.environ['EDGEDEVICE_NAMESPACE']
MQTT_BROKER_HOST = os.environ.get('MQTT_BROKER_HOST', 'localhost')
MQTT_BROKER_PORT = int(os.environ.get('MQTT_BROKER_PORT', '1883'))
MQTT_USERNAME = os.environ.get('MQTT_USERNAME', None)
MQTT_PASSWORD = os.environ.get('MQTT_PASSWORD', None)
MQTT_CLIENT_ID = os.environ.get('MQTT_CLIENT_ID', 'device-shifu-mqtt-client')
MQTT_KEEPALIVE = int(os.environ.get('MQTT_KEEPALIVE', '60'))
MQTT_TELEMETRY_INTERVAL_SEC = int(os.environ.get('MQTT_TELEMETRY_INTERVAL_SEC', '2'))

CONFIGMAP_INSTRUCTIONS_PATH = '/etc/edgedevice/config/instructions'

# DeviceShifu MQTT topics & QoS
TOPICS = {
    "device/commands/estop": 1,      # publish
    "device/commands/state": 1,      # publish
    "device/commands/action": 1,     # publish
    "device/telemetry/data": 1,      # subscribe
    "device/telemetry/status": 1     # subscribe
}
# All topics can be made configurable via env, but using these as default per requirements.

# Kubernetes API setup
def load_k8s_config():
    try:
        config.load_incluster_config()
    except Exception as e:
        logging.error("Could not load in-cluster k8s config: %s", e)
        sys.exit(1)

load_k8s_config()
crd_api = client.CustomObjectsApi()

# Helper to update EdgeDevice phase
def update_edge_device_phase(phase):
    try:
        body = {"status": {"edgeDevicePhase": phase}}
        crd_api.patch_namespaced_custom_object_status(
            group="shifu.edgenesis.io",
            version="v1alpha1",
            namespace=EDGEDEVICE_NAMESPACE,
            plural="edgedevices",
            name=EDGEDEVICE_NAME,
            body=body
        )
        logging.info(f"Updated EdgeDevice phase to {phase}")
    except ApiException as e:
        logging.error(f"Failed to update EdgeDevice phase: {e}")

# Helper to get EdgeDevice CRD
def get_edge_device_crd():
    try:
        return crd_api.get_namespaced_custom_object(
            group="shifu.edgenesis.io",
            version="v1alpha1",
            namespace=EDGEDEVICE_NAMESPACE,
            plural="edgedevices",
            name=EDGEDEVICE_NAME
        )
    except ApiException as e:
        logging.error("Could not get EdgeDevice CRD: %s", e)
        return None

# Load instruction settings from ConfigMap (YAML)
def load_instruction_settings():
    try:
        with open(CONFIGMAP_INSTRUCTIONS_PATH, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.warning("Could not load instruction settings: %s", e)
        return {}

instruction_settings = load_instruction_settings()

# Device address (HTTP endpoint)
edge_device = get_edge_device_crd()
if edge_device is None:
    logging.error("Fatal: Could not get EdgeDevice CRD on startup. Exiting.")
    sys.exit(1)

device_address = edge_device.get('spec', {}).get('address', None)
if not device_address:
    logging.error("Fatal: Device address (.spec.address) not found in EdgeDevice CRD. Exiting.")
    sys.exit(1)

# --- MQTT Handlers ---

class DeviceShifuMQTTClient:
    def __init__(self):
        self.client = mqtt.Client(MQTT_CLIENT_ID)
        if MQTT_USERNAME:
            self.client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

        self.connected = False
        self.should_stop = False

        # Track last known device phase
        self.device_phase = "Pending"
        update_edge_device_phase(self.device_phase)

        # Start background telemetry task
        self.telemetry_thread = threading.Thread(target=self.telemetry_publisher_loop)
        self.telemetry_thread.daemon = True

    def connect(self):
        try:
            self.client.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, MQTT_KEEPALIVE)
            self.client.loop_start()
            self.telemetry_thread.start()
        except Exception as e:
            logging.error("MQTT Connection failed: %s", e)
            update_edge_device_phase("Failed")
            sys.exit(1)

    def disconnect(self):
        self.should_stop = True
        self.client.loop_stop()
        self.client.disconnect()
        update_edge_device_phase("Pending")

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to MQTT broker")
            self.connected = True
            update_edge_device_phase("Running")
            for topic, qos in TOPICS.items():
                if topic.startswith("device/telemetry/"):
                    self.client.subscribe(topic, qos)
                    logging.info(f"Subscribed to topic: {topic} (QoS {qos})")
        else:
            logging.error(f"Failed to connect to MQTT broker: {rc}")
            update_edge_device_phase("Failed")

    def on_disconnect(self, client, userdata, rc):
        self.connected = False
        update_edge_device_phase("Pending")
        logging.warning(f"MQTT Disconnected with rc={rc}")

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        try:
            payload = msg.payload.decode('utf-8')
            data = json.loads(payload)
        except Exception as e:
            logging.warning(f"Invalid JSON on topic {topic}: {e}")
            return

        if topic == "device/telemetry/data":
            # User does not send to this topic, ignore
            return
        if topic == "device/telemetry/status":
            # User does not send to this topic, ignore
            return

        # Handle command topics
        if topic == "device/commands/estop":
            self.handle_estop_command(data)
        elif topic == "device/commands/state":
            self.handle_state_command(data)
        elif topic == "device/commands/action":
            self.handle_action_command(data)
        else:
            logging.warning(f"Unknown topic: {topic}")

    # --- Command Handlers ---

    def handle_estop_command(self, data):
        settings = instruction_settings.get('api1', {}).get('protocolPropertyList', {})
        url = f"{device_address}/estop"
        try:
            resp = requests.post(url, json=data, timeout=5)
            self.publish_result("device/commands/estop/response", resp)
        except Exception as e:
            self.publish_error("device/commands/estop/response", str(e))

    def handle_state_command(self, data):
        settings = instruction_settings.get('api2', {}).get('protocolPropertyList', {})
        url = f"{device_address}/state"
        try:
            resp = requests.post(url, json=data, timeout=5)
            self.publish_result("device/commands/state/response", resp)
        except Exception as e:
            self.publish_error("device/commands/state/response", str(e))

    def handle_action_command(self, data):
        settings = instruction_settings.get('api3', {}).get('protocolPropertyList', {})
        url = f"{device_address}/action"
        try:
            resp = requests.post(url, json=data, timeout=5)
            self.publish_result("device/commands/action/response", resp)
        except Exception as e:
            self.publish_error("device/commands/action/response", str(e))

    def publish_result(self, topic, resp):
        try:
            payload = {
                "status_code": resp.status_code,
                "response": resp.json() if resp.headers.get("Content-Type", "").startswith("application/json") else resp.text
            }
        except Exception:
            payload = {
                "status_code": getattr(resp, 'status_code', None),
                "response": resp.text if hasattr(resp, 'text') else str(resp)
            }
        self.client.publish(topic, json.dumps(payload), qos=1)

    def publish_error(self, topic, err_msg):
        self.client.publish(topic, json.dumps({"error": err_msg}), qos=1)

    # --- Telemetry Publisher ---

    def telemetry_publisher_loop(self):
        while not self.should_stop:
            try:
                # Telemetry: /data (aggregated sensor data)
                url_data = f"{device_address}/telemetry/data"
                resp_data = requests.get(url_data, timeout=5)
                if resp_data.status_code == 200:
                    payload = resp_data.json() if resp_data.headers.get("Content-Type", "").startswith("application/json") else resp_data.text
                    self.client.publish("device/telemetry/data", json.dumps(payload), qos=1)
                # Telemetry: /status (overall robot status)
                url_status = f"{device_address}/telemetry/status"
                resp_status = requests.get(url_status, timeout=5)
                if resp_status.status_code == 200:
                    payload = resp_status.json() if resp_status.headers.get("Content-Type", "").startswith("application/json") else resp_status.text
                    self.client.publish("device/telemetry/status", json.dumps(payload), qos=1)
            except Exception as e:
                logging.warning(f"Telemetry publish error: {e}")
                update_edge_device_phase("Unknown")
            finally:
                for _ in range(MQTT_TELEMETRY_INTERVAL_SEC * 10):
                    if self.should_stop:
                        return
                    asyncio.sleep(0.1)

# Graceful shutdown handler
def signal_handler(sig, frame):
    logging.info("Terminating DeviceShifu driver...")
    shifu.disconnect()
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# --- Main ---

if __name__ == "__main__":
    shifu = DeviceShifuMQTTClient()
    shifu.connect()
    while True:
        try:
            # Keep alive
            signal.pause()
        except KeyboardInterrupt:
            signal_handler(None, None)