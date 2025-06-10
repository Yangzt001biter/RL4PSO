import os
import socket
import json
import yaml
import time
import threading
import logging

import paho.mqtt.client as mqtt
from kubernetes import client, config, watch

# Configuration
CONFIGMAP_DIR = "/etc/edgedevice/config/instructions"

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("device-shifu")

# Environment Variables
MQTT_BROKER_ADDRESS = os.environ["MQTT_BROKER_ADDRESS"]
EDGEDEVICE_NAME = os.environ["EDGEDEVICE_NAME"]
EDGEDEVICE_NAMESPACE = os.environ["EDGEDEVICE_NAMESPACE"]
UDP_DEVICE_IP = os.environ.get("UDP_DEVICE_IP")
UDP_DEVICE_PORT = int(os.environ.get("UDP_DEVICE_PORT", "0"))
UDP_LOCAL_PORT = int(os.environ.get("UDP_LOCAL_PORT", "0"))

EDGEDEVICE_CRD_GROUP = "shifu.edgenesis.io"
EDGEDEVICE_CRD_VERSION = "v1alpha1"
EDGEDEVICE_CRD_PLURAL = "edgedevices"

# MQTT Topics
TELEMETRY_TOPIC = "device/telemetry/state"
COMMAND_HEARTBEAT_TOPIC = "device/commands/heartbeat"
COMMAND_CONTROL_TOPIC = "device/commands/control"

# DeviceShifu Phases
PHASE_PENDING = "Pending"
PHASE_RUNNING = "Running"
PHASE_FAILED = "Failed"
PHASE_UNKNOWN = "Unknown"

# Parse settings from ConfigMap
def load_api_settings():
    instructions = {}
    if os.path.isdir(CONFIGMAP_DIR):
        for fname in os.listdir(CONFIGMAP_DIR):
            with open(os.path.join(CONFIGMAP_DIR, fname), "r") as f:
                data = yaml.safe_load(f)
                if data:
                    instructions.update(data)
    return instructions

API_SETTINGS = load_api_settings()

# Kubernetes API setup (in-cluster)
config.load_incluster_config()
k8s = client.CustomObjectsApi()

def update_device_phase(phase):
    body = {
        "status": {
            "edgeDevicePhase": phase
        }
    }
    try:
        k8s.patch_namespaced_custom_object_status(
            group=EDGEDEVICE_CRD_GROUP,
            version=EDGEDEVICE_CRD_VERSION,
            namespace=EDGEDEVICE_NAMESPACE,
            plural=EDGEDEVICE_CRD_PLURAL,
            name=EDGEDEVICE_NAME,
            body=body
        )
        logger.info(f"Updated device phase: {phase}")
    except Exception as e:
        logger.error(f"Failed to update device phase: {e}")

def get_device_address():
    try:
        obj = k8s.get_namespaced_custom_object(
            group=EDGEDEVICE_CRD_GROUP,
            version=EDGEDEVICE_CRD_VERSION,
            namespace=EDGEDEVICE_NAMESPACE,
            plural=EDGEDEVICE_CRD_PLURAL,
            name=EDGEDEVICE_NAME
        )
        addr = obj.get("spec", {}).get("address")
        return addr
    except Exception as e:
        logger.error(f"Failed to get device address from CRD: {e}")
        return None

# UDP Communication Handler
class UDPDeviceClient:
    def __init__(self, device_ip, device_port, local_port):
        self.device_addr = (device_ip, device_port)
        self.local_port = local_port
        self.sock = None
        self.connected = False

    def connect(self):
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            if self.local_port:
                self.sock.bind(('', self.local_port))
            self.sock.settimeout(2)
            self.connected = True
            logger.info(f"UDP client bound to port {self.local_port}, device {self.device_addr}")
            update_device_phase(PHASE_RUNNING)
        except Exception as e:
            logger.error(f"UDP connection failed: {e}")
            self.connected = False
            update_device_phase(PHASE_FAILED)

    def close(self):
        if self.sock:
            self.sock.close()
        self.connected = False
        update_device_phase(PHASE_PENDING)

    def send(self, payload: bytes):
        if not self.connected:
            raise RuntimeError("UDP client not connected")
        self.sock.sendto(payload, self.device_addr)

    def recv(self, bufsize=4096):
        try:
            data, _ = self.sock.recvfrom(bufsize)
            return data
        except socket.timeout:
            return None

# Protocol conversion: UDP binary <-> JSON
def udp_binary_to_json(data: bytes) -> dict:
    # Placeholder: Implement binary protocol parsing here
    # For now, just return dummy
    return {"raw_binary": data.hex()}

def json_to_udp_binary(json_obj: dict) -> bytes:
    # Placeholder: Implement binary protocol packing here
    # For now, just send as JSON-encoded bytes
    return json.dumps(json_obj).encode("utf-8")

# MQTT Handler
class MQTTShifuHandler:
    def __init__(self, broker, udp_client: UDPDeviceClient):
        self.broker = broker
        self.udp_client = udp_client
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.topics = [
            (COMMAND_HEARTBEAT_TOPIC, 1),
            (COMMAND_CONTROL_TOPIC, 1),
        ]
        self.telemetry_topic = TELEMETRY_TOPIC

    def start(self):
        self.client.connect(self.broker.split(":")[0], int(self.broker.split(":")[1]))
        self.client.loop_start()

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()

    def on_connect(self, client, userdata, flags, rc):
        logger.info("Connected to MQTT broker")
        # Subscribe to command topics
        for topic, qos in self.topics:
            client.subscribe(topic, qos=qos)
            logger.info(f"Subscribed to topic: {topic} (QoS: {qos})")

    def on_message(self, client, userdata, msg):
        logger.info(f"Received MQTT message on {msg.topic}")
        try:
            payload = msg.payload.decode("utf-8")
            data = json.loads(payload)
            # Protocol conversion: JSON to UDP binary
            udp_payload = json_to_udp_binary(data)
            self.udp_client.send(udp_payload)
        except Exception as e:
            logger.error(f"Failed to process MQTT message: {e}")

    def publish_telemetry(self, telemetry_json: dict):
        self.client.publish(self.telemetry_topic, json.dumps(telemetry_json), qos=1)
        logger.info(f"Published telemetry to {self.telemetry_topic}")

# Telemetry Worker
def telemetry_worker(udp_client: UDPDeviceClient, mqtt_handler: MQTTShifuHandler, settings):
    while True:
        try:
            data = udp_client.recv()
            if data:
                telemetry = udp_binary_to_json(data)
                mqtt_handler.publish_telemetry(telemetry)
            else:
                # Optionally, send heartbeat or poll device
                pass
        except Exception as e:
            logger.error(f"Telemetry worker error: {e}")
            update_device_phase(PHASE_FAILED)
            time.sleep(2)

def main():
    update_device_phase(PHASE_PENDING)
    # Get device UDP address from CRD if not set in env
    if not (UDP_DEVICE_IP and UDP_DEVICE_PORT):
        addr = get_device_address()
        if addr and ":" in addr:
            UDP_DEVICE_IP, UDP_DEVICE_PORT = addr.split(":")
            UDP_DEVICE_PORT = int(UDP_DEVICE_PORT)
        else:
            logger.error("UDP device address not set or invalid.")
            update_device_phase(PHASE_UNKNOWN)
            return

    udp_client = UDPDeviceClient(UDP_DEVICE_IP, UDP_DEVICE_PORT, UDP_LOCAL_PORT)
    udp_client.connect()
    if not udp_client.connected:
        update_device_phase(PHASE_FAILED)
        return

    mqtt_handler = MQTTShifuHandler(MQTT_BROKER_ADDRESS, udp_client)
    mqtt_handler.start()

    telemetry_settings = API_SETTINGS.get("device/telemetry/state", {})
    telemetry_thread = threading.Thread(
        target=telemetry_worker,
        args=(udp_client, mqtt_handler, telemetry_settings),
        daemon=True
    )
    telemetry_thread.start()

    # Health monitoring loop
    while True:
        try:
            if not udp_client.connected:
                update_device_phase(PHASE_FAILED)
                break
            time.sleep(5)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(f"Main loop error: {e}")
            update_device_phase(PHASE_UNKNOWN)
            break

    mqtt_handler.stop()
    udp_client.close()

if __name__ == "__main__":
    main()