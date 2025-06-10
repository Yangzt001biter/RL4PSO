import os
import sys
import yaml
import json
import socket
import threading
import logging
import time

from kubernetes import client, config
from kubernetes.client.rest import ApiException
import paho.mqtt.client as mqtt

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("device_shifu")

# Constants
INSTRUCTION_PATH = '/etc/edgedevice/config/instructions'
API_GROUP = 'shifu.edgenesis.io'
API_VERSION = 'v1alpha1'
EDGEDEVICE_CRD = 'edgedevices'
EDGEDEVICE_PHASE_FIELD = 'edgeDevicePhase'
PHASE_PENDING = 'Pending'
PHASE_RUNNING = 'Running'
PHASE_FAILED = 'Failed'
PHASE_UNKNOWN = 'Unknown'

# MQTT Topics from API info
MQTT_HEARTBEAT_TOPIC = "device/commands/heartbeat"
MQTT_STATE_TOPIC = "device/telemetry/state"
MQTT_CONTROL_TOPIC = "device/commands/control"

# Environment variables (all required)
EDGEDEVICE_NAME = os.environ.get("EDGEDEVICE_NAME")
EDGEDEVICE_NAMESPACE = os.environ.get("EDGEDEVICE_NAMESPACE")
MQTT_BROKER_ADDRESS = os.environ.get("MQTT_BROKER_ADDRESS")
UDP_DEVICE_IP = os.environ.get("UDP_DEVICE_IP")
UDP_DEVICE_PORT = int(os.environ.get("UDP_DEVICE_PORT", "0"))
UDP_LOCAL_PORT = int(os.environ.get("UDP_LOCAL_PORT", "0"))

if not EDGEDEVICE_NAME or not EDGEDEVICE_NAMESPACE or not MQTT_BROKER_ADDRESS or not UDP_DEVICE_IP or not UDP_DEVICE_PORT or not UDP_LOCAL_PORT:
    logger.error("Missing required environment variables.")
    sys.exit(1)

# Load Kubernetes in-cluster config
config.load_incluster_config()
k8s_api = client.CustomObjectsApi()

# Load instructions from ConfigMap
def load_instructions():
    instructions = {}
    for root, dirs, files in os.walk(INSTRUCTION_PATH):
        for file in files:
            if file.endswith('.yaml') or file.endswith('.yml'):
                with open(os.path.join(root, file), 'r') as f:
                    d = yaml.safe_load(f)
                    instructions.update(d)
    return instructions

instructions = load_instructions()

# Kubernetes EdgeDevice status updater
def update_edgedevice_phase(phase):
    try:
        body = {
            "status": {
                EDGEDEVICE_PHASE_FIELD: phase
            }
        }
        api_response = k8s_api.patch_namespaced_custom_object_status(
            group=API_GROUP,
            version=API_VERSION,
            namespace=EDGEDEVICE_NAMESPACE,
            plural=EDGEDEVICE_CRD,
            name=EDGEDEVICE_NAME,
            body=body
        )
    except ApiException as e:
        logger.error(f"Kubernetes API exception: {e}")

def get_edgedevice_address():
    try:
        obj = k8s_api.get_namespaced_custom_object(
            group=API_GROUP,
            version=API_VERSION,
            namespace=EDGEDEVICE_NAMESPACE,
            plural=EDGEDEVICE_CRD,
            name=EDGEDEVICE_NAME
        )
        addr = obj.get("spec", {}).get("address", "")
        return addr
    except ApiException as e:
        logger.error(f"Kubernetes API exception: {e}")
        return ""

# UDP Communication
class UDPDeviceClient(threading.Thread):
    def __init__(self, device_ip, device_port, local_port):
        super().__init__(daemon=True)
        self.device_ip = device_ip
        self.device_port = device_port
        self.local_port = local_port
        self.sock = None
        self.running = False
        self.listeners = []
        self.last_recv_time = 0

    def run(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(('', self.local_port))
        self.sock.settimeout(1)
        self.running = True
        logger.info(f"UDP client listening on port {self.local_port}")
        while self.running:
            try:
                data, addr = self.sock.recvfrom(4096)
                self.last_recv_time = time.time()
                for callback in self.listeners:
                    callback(data)
            except socket.timeout:
                continue
            except Exception as e:
                logger.error(f"UDP receive error: {e}")
                self.running = False
                break

    def stop(self):
        self.running = False
        if self.sock:
            self.sock.close()

    def send(self, data: bytes):
        try:
            self.sock.sendto(data, (self.device_ip, self.device_port))
        except Exception as e:
            logger.error(f"UDP send error: {e}")

    def add_listener(self, callback):
        self.listeners.append(callback)

# Protocol conversion helpers
def build_udp_command_from_json(api, payload_json):
    # Placeholder: In actual implementation, map JSON to device's UDP binary protocol
    # For demo, just encode the JSON as UTF-8
    # For heartbeat, control etc, you may want to adjust this
    return json.dumps(payload_json).encode('utf-8')

def parse_udp_to_json(data_bytes):
    # Placeholder: In actual implementation, decode device binary protocol -> JSON
    # For demo, just decode as utf-8 JSON
    try:
        return json.loads(data_bytes.decode('utf-8'))
    except Exception:
        # fallback: hex dump
        return {"raw": data_bytes.hex()}

# MQTT Handlers
class DeviceShifuMQTT:
    def __init__(self, broker_addr, udp_client):
        self.client = mqtt.Client()
        self.broker_addr = broker_addr
        self.udp_client = udp_client
        self.connected = False

    def on_connect(self, client, userdata, flags, rc):
        logger.info("Connected to MQTT broker")
        self.connected = True
        # Subscribe to state topic (as a publisher for users)
        client.subscribe(MQTT_STATE_TOPIC, qos=1)
        # Subscribe to control and heartbeat commands (for device)
        client.subscribe(MQTT_CONTROL_TOPIC, qos=1)
        client.subscribe(MQTT_HEARTBEAT_TOPIC, qos=1)

    def on_disconnect(self, client, userdata, rc):
        logger.warning("Disconnected from MQTT broker")
        self.connected = False

    def on_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode('utf-8')
            data = json.loads(payload)
        except Exception as e:
            logger.error(f"MQTT message decode error: {e}")
            return
        logger.info(f"MQTT RX: Topic={msg.topic} Payload={data}")
        # Convert MQTT -> UDP and send to device
        if msg.topic == MQTT_CONTROL_TOPIC:
            udp_payload = build_udp_command_from_json("control", data)
            self.udp_client.send(udp_payload)
        elif msg.topic == MQTT_HEARTBEAT_TOPIC:
            udp_payload = build_udp_command_from_json("heartbeat", data)
            self.udp_client.send(udp_payload)
        # For telemetry/state, only publish (not handled here)

    def publish_state(self, payload):
        if self.connected:
            self.client.publish(MQTT_STATE_TOPIC, json.dumps(payload), qos=1)

    def start(self):
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        host, port = self.broker_addr.split(":")
        self.client.connect(host, int(port), keepalive=60)
        self.client.loop_start()

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()

# Main logic
def main():
    logger.info("Starting DeviceShifu for DEEP Robotics Jueying Lite3 Motion Host")
    update_edgedevice_phase(PHASE_PENDING)

    # UDP client
    udp_client = UDPDeviceClient(UDP_DEVICE_IP, UDP_DEVICE_PORT, UDP_LOCAL_PORT)
    udp_client.start()

    # MQTT client
    mqtt_client = DeviceShifuMQTT(MQTT_BROKER_ADDRESS, udp_client)
    mqtt_client.start()

    # Device state monitoring loop
    device_connected = False

    def udp_to_mqtt_callback(data):
        # Parse UDP device telemetry and publish on MQTT topic
        try:
            state_json = parse_udp_to_json(data)
            mqtt_client.publish_state(state_json)
        except Exception as e:
            logger.error(f"UDP->MQTT conversion error: {e}")

    udp_client.add_listener(udp_to_mqtt_callback)

    try:
        while True:
            # Check UDP device connectivity (last receive timestamp)
            now = time.time()
            if udp_client.last_recv_time and now - udp_client.last_recv_time < 5:
                if not device_connected:
                    update_edgedevice_phase(PHASE_RUNNING)
                    device_connected = True
            else:
                if device_connected:
                    update_edgedevice_phase(PHASE_PENDING)
                    device_connected = False
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down DeviceShifu...")
    except Exception as e:
        logger.error(f"Main loop error: {e}")
        update_edgedevice_phase(PHASE_FAILED)
    finally:
        mqtt_client.stop()
        udp_client.stop()
        update_edgedevice_phase(PHASE_UNKNOWN)

if __name__ == "__main__":
    main()