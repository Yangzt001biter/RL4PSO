import os
import yaml
import json
import threading
import time
import logging
from kubernetes import client, config
from kubernetes.client.rest import ApiException
import paho.mqtt.client as mqtt
import requests

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeviceShifuLite3")

# Environment Variables
MQTT_BROKER_HOST = os.environ.get("MQTT_BROKER_HOST", "127.0.0.1")
MQTT_BROKER_PORT = int(os.environ.get("MQTT_BROKER_PORT", "1883"))
MQTT_CLIENT_ID = os.environ.get("MQTT_CLIENT_ID", "deviceshifu-lite3")
MQTT_USERNAME = os.environ.get("MQTT_USERNAME")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
MQTT_TELEMETRY_STATE_TOPIC = os.environ.get("MQTT_TELEMETRY_STATE_TOPIC", "device/telemetry/state")
MQTT_TELEMETRY_STATUS_TOPIC = os.environ.get("MQTT_TELEMETRY_STATUS_TOPIC", "device/telemetry/status")
MQTT_COMMANDS_CONTROL_TOPIC = os.environ.get("MQTT_COMMANDS_CONTROL_TOPIC", "device/commands/control")
MQTT_COMMANDS_RESULT_TOPIC = os.environ.get("MQTT_COMMANDS_RESULT_TOPIC", "device/commands/result")
MQTT_QOS = int(os.environ.get("MQTT_QOS", "1"))

EDGEDEVICE_NAME = os.environ["EDGEDEVICE_NAME"]
EDGEDEVICE_NAMESPACE = os.environ["EDGEDEVICE_NAMESPACE"]

INSTRUCTIONS_FILE = "/etc/edgedevice/config/instructions"

KUBERNETES_SERVICE_HOST = os.environ.get("KUBERNETES_SERVICE_HOST")
KUBERNETES_SERVICE_PORT = os.environ.get("KUBERNETES_SERVICE_PORT")

# Load Config Instructions
def load_instructions():
    try:
        with open(INSTRUCTIONS_FILE, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load instructions: {e}")
        return {}

instructions = load_instructions()

# Kubernetes CRD Client
class EdgeDeviceCRD:
    def __init__(self):
        config.load_incluster_config()
        self.api = client.CustomObjectsApi()
        self.group = "shifu.edgenesis.io"
        self.version = "v1alpha1"
        self.namespace = EDGEDEVICE_NAMESPACE
        self.plural = "edgedevices"
        self.name = EDGEDEVICE_NAME

    def get(self):
        try:
            return self.api.get_namespaced_custom_object(
                group=self.group,
                version=self.version,
                namespace=self.namespace,
                plural=self.plural,
                name=self.name,
            )
        except ApiException as e:
            logger.error(f"K8s get EdgeDevice error: {e}")
            return None

    def update_status(self, phase):
        for _ in range(3):
            try:
                body = {"status": {"edgeDevicePhase": phase}}
                self.api.patch_namespaced_custom_object_status(
                    group=self.group,
                    version=self.version,
                    namespace=self.namespace,
                    plural=self.plural,
                    name=self.name,
                    body=body,
                )
                logger.info(f"Updated EdgeDevice status to {phase}")
                return
            except ApiException as e:
                logger.error(f"K8s update status error: {e}")
                time.sleep(1)

# Device HTTP Address (from CRD)
def get_device_http_address():
    crd = EdgeDeviceCRD()
    edge_device = crd.get()
    if not edge_device:
        return None
    try:
        return edge_device["spec"]["address"]
    except Exception as e:
        logger.error(f"Failed to get device address: {e}")
        return None

# DeviceShifu Class
class DeviceShifuLite3:
    def __init__(self):
        self.crd = EdgeDeviceCRD()
        self.device_address = None
        self.mqtt = None
        self.running = True
        self.status_phase = "Pending"
        self.lock = threading.Lock()
        self.last_device_check = 0

    def update_status(self, phase):
        with self.lock:
            if self.status_phase != phase:
                self.status_phase = phase
                self.crd.update_status(phase)

    def connect_device(self):
        self.device_address = get_device_http_address()
        if not self.device_address:
            self.update_status("Unknown")
            return False
        # Try to check device HTTP endpoint
        try:
            resp = requests.get(f"{self.device_address}/healthz", timeout=2)
            if resp.status_code == 200:
                self.update_status("Running")
                return True
            else:
                self.update_status("Failed")
                return False
        except Exception:
            self.update_status("Pending")
            return False

    def device_heartbeat(self):
        while self.running:
            ok = self.connect_device()
            if not ok:
                self.update_status("Pending")
            time.sleep(10)

    def on_connect(self, client, userdata, flags, rc):
        logger.info(f"Connected to MQTT broker with result code {rc}")
        # Subscriptions
        client.subscribe(MQTT_COMMANDS_CONTROL_TOPIC, qos=MQTT_QOS)
        client.subscribe(MQTT_TELEMETRY_STATE_TOPIC, qos=MQTT_QOS)
        client.subscribe(MQTT_TELEMETRY_STATUS_TOPIC, qos=MQTT_QOS)
        self.update_status("Running")

    def on_disconnect(self, client, userdata, rc):
        logger.warning("Disconnected from MQTT broker")
        self.update_status("Pending")

    def on_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode()
            topic = msg.topic
            logger.info(f"Received MQTT message on {topic}")
            if topic == MQTT_COMMANDS_CONTROL_TOPIC:
                self.handle_command(payload)
            elif topic in [MQTT_TELEMETRY_STATE_TOPIC, MQTT_TELEMETRY_STATUS_TOPIC]:
                # Telemetry topics: just forward or process as needed
                logger.debug(f"Telemetry: {payload}")
            else:
                logger.debug(f"Unknown topic: {topic}")
        except Exception as e:
            logger.error(f"Error in on_message: {e}")

    def handle_command(self, payload):
        # Convert MQTT command to HTTP device request
        try:
            data = json.loads(payload)
            cmd = data.get("cmd")
            params = data.get("params", {})

            api_settings = instructions.get("device/commands/control", {})
            http_method = api_settings.get("protocolPropertyList", {}).get("httpMethod", "POST")
            http_path = api_settings.get("protocolPropertyList", {}).get("httpPath", "/api/v1/command")

            if not self.device_address:
                self.update_status("Unknown")
                result = {"status": "error", "message": "Device not connected"}
                self.publish_result(result)
                return

            url = f"{self.device_address}{http_path}"
            http_headers = {"Content-Type": "application/json"}

            request_body = {
                "cmd": cmd,
                "params": params
            }

            resp = requests.request(
                http_method,
                url,
                headers=http_headers,
                data=json.dumps(request_body),
                timeout=3
            )
            if resp.status_code == 200:
                result = {"status": "success", "response": resp.json()}
            else:
                result = {"status": "error", "code": resp.status_code, "response": resp.text}
            self.publish_result(result)
        except Exception as e:
            logger.error(f"Failed to handle command: {e}")
            result = {"status": "error", "message": str(e)}
            self.publish_result(result)

    def publish_result(self, result):
        try:
            self.mqtt.publish(MQTT_COMMANDS_RESULT_TOPIC, json.dumps(result), qos=MQTT_QOS)
        except Exception as e:
            logger.error(f"Failed to publish result: {e}")

    def setup_mqtt(self):
        self.mqtt = mqtt.Client(client_id=MQTT_CLIENT_ID, clean_session=True)
        if MQTT_USERNAME and MQTT_PASSWORD:
            self.mqtt.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        self.mqtt.on_connect = self.on_connect
        self.mqtt.on_disconnect = self.on_disconnect
        self.mqtt.on_message = self.on_message

        while self.running:
            try:
                self.mqtt.connect(MQTT_BROKER_HOST, MQTT_BROKER_PORT, 60)
                break
            except Exception as e:
                logger.error(f"MQTT connect error: {e}")
                self.update_status("Pending")
                time.sleep(5)
        self.mqtt.loop_start()

    def start(self):
        # Start device heartbeat/status update thread
        threading.Thread(target=self.device_heartbeat, daemon=True).start()
        # Setup MQTT
        self.setup_mqtt()
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.running = False
            self.mqtt.loop_stop()

if __name__ == "__main__":
    shifu = DeviceShifuLite3()
    shifu.start()