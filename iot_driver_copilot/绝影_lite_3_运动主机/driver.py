import os
import sys
import time
import yaml
import json
import threading
import traceback

import paho.mqtt.client as mqtt
import requests

from kubernetes import client, config
from kubernetes.client.rest import ApiException

INSTRUCTIONS_PATH = '/etc/edgedevice/config/instructions'
EDGEDEVICE_CRD_GROUP = 'shifu.edgenesis.io'
EDGEDEVICE_CRD_VERSION = 'v1alpha1'
EDGEDEVICE_CRD_PLURAL = 'edgedevices'

STATUS_PENDING = "Pending"
STATUS_RUNNING = "Running"
STATUS_FAILED = "Failed"
STATUS_UNKNOWN = "Unknown"

# Load configuration from environment variables
MQTT_BROKER_ADDRESS = os.environ.get('MQTT_BROKER_ADDRESS', 'localhost')
MQTT_BROKER_PORT = int(os.environ.get('MQTT_BROKER_PORT', '1883'))
MQTT_USERNAME = os.environ.get('MQTT_USERNAME', '')
MQTT_PASSWORD = os.environ.get('MQTT_PASSWORD', '')
MQTT_CLIENT_ID = os.environ.get('MQTT_CLIENT_ID', 'device-shifu-lite3')
MQTT_KEEPALIVE = int(os.environ.get('MQTT_KEEPALIVE', '60'))
MQTT_COMMANDS_TOPIC = os.environ.get('MQTT_COMMANDS_TOPIC', 'device/commands/control')
MQTT_TELEMETRY_TOPIC = os.environ.get('MQTT_TELEMETRY_TOPIC', 'device/telemetry/state')
MQTT_TELEMETRY_RESULT_TOPIC = os.environ.get('MQTT_TELEMETRY_RESULT_TOPIC', 'device/telemetry/result')
MQTT_COMMANDS_RESULT_TOPIC = os.environ.get('MQTT_COMMANDS_RESULT_TOPIC', 'device/commands/result')

EDGEDEVICE_NAME = os.environ['EDGEDEVICE_NAME']
EDGEDEVICE_NAMESPACE = os.environ['EDGEDEVICE_NAMESPACE']

# Kubernetes in-cluster config
config.load_incluster_config()
k8s_api = client.CustomObjectsApi()

def get_edgedevice():
    try:
        ed = k8s_api.get_namespaced_custom_object(
            group=EDGEDEVICE_CRD_GROUP,
            version=EDGEDEVICE_CRD_VERSION,
            namespace=EDGEDEVICE_NAMESPACE,
            plural=EDGEDEVICE_CRD_PLURAL,
            name=EDGEDEVICE_NAME
        )
        return ed
    except Exception as e:
        return None

def update_edgedevice_phase(phase):
    retry = 0
    while retry < 3:
        try:
            body = {"status": {"edgeDevicePhase": phase}}
            k8s_api.patch_namespaced_custom_object_status(
                group=EDGEDEVICE_CRD_GROUP,
                version=EDGEDEVICE_CRD_VERSION,
                namespace=EDGEDEVICE_NAMESPACE,
                plural=EDGEDEVICE_CRD_PLURAL,
                name=EDGEDEVICE_NAME,
                body=body
            )
            return True
        except ApiException as e:
            retry += 1
            time.sleep(1)
        except Exception as e:
            break
    return False

def get_device_address():
    ed = get_edgedevice()
    if ed is None:
        return None
    return ed.get('spec', {}).get('address', None)

def load_instruction_config():
    try:
        with open(INSTRUCTIONS_PATH, "r") as f:
            raw = f.read()
            return yaml.safe_load(raw)
    except Exception:
        return {}

# Device connectivity check
def check_device_connection(device_address):
    try:
        # Try GET /health or /status or root (as available)
        url = f"{device_address.rstrip('/')}/health"
        r = requests.get(url, timeout=2)
        if r.status_code == 200:
            return True
    except Exception:
        pass
    try:
        url = f"{device_address.rstrip('/')}/status"
        r = requests.get(url, timeout=2)
        if r.status_code == 200:
            return True
    except Exception:
        pass
    try:
        url = device_address.rstrip('/')
        r = requests.get(url, timeout=2)
        if r.status_code == 200:
            return True
    except Exception:
        pass
    return False

class DeviceShifuLite3:
    def __init__(self):
        self.mqtt_client = None
        self.running = True
        self.device_address = None
        self.instruction_config = load_instruction_config()
        self.device_connected = False
        self.last_status = STATUS_UNKNOWN

    def start(self):
        # Periodic device connection check and phase update
        threading.Thread(target=self.device_status_monitor, daemon=True).start()
        # MQTT thread
        self.init_mqtt()
        while self.running:
            time.sleep(1)

    def device_status_monitor(self):
        while self.running:
            ed = get_edgedevice()
            if ed is None:
                phase = STATUS_UNKNOWN
                self.device_address = None
            else:
                self.device_address = ed.get('spec', {}).get('address', None)
                if self.device_address is None:
                    phase = STATUS_PENDING
                else:
                    connected = check_device_connection(self.device_address)
                    if connected:
                        phase = STATUS_RUNNING
                        self.device_connected = True
                    else:
                        phase = STATUS_FAILED
                        self.device_connected = False
            if phase != self.last_status:
                update_edgedevice_phase(phase)
                self.last_status = phase
            time.sleep(5)

    def init_mqtt(self):
        self.mqtt_client = mqtt.Client(MQTT_CLIENT_ID)
        if MQTT_USERNAME:
            self.mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_disconnect = self.on_disconnect
        self.mqtt_client.connect_async(MQTT_BROKER_ADDRESS, MQTT_BROKER_PORT, MQTT_KEEPALIVE)
        self.mqtt_client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        # Subscribe to commands and telemetry requests
        client.subscribe(MQTT_COMMANDS_TOPIC, qos=1)
        client.subscribe(MQTT_TELEMETRY_TOPIC, qos=1)

    def on_disconnect(self, client, userdata, rc):
        # Try to reconnect
        while self.running:
            try:
                client.reconnect()
                break
            except Exception:
                time.sleep(2)

    def on_message(self, client, userdata, msg):
        try:
            payload = msg.payload.decode()
            if msg.topic == MQTT_COMMANDS_TOPIC:
                # User sent a device command
                resp = self.handle_command(payload)
                client.publish(MQTT_COMMANDS_RESULT_TOPIC, json.dumps(resp), qos=1)
            elif msg.topic == MQTT_TELEMETRY_TOPIC:
                # User requests telemetry
                resp = self.handle_telemetry(payload)
                client.publish(MQTT_TELEMETRY_RESULT_TOPIC, json.dumps(resp), qos=1)
        except Exception as e:
            traceback.print_exc()

    def handle_command(self, payload):
        if not self.device_connected or not self.device_address:
            return {"status": "error", "reason": "Device not connected"}
        try:
            data = json.loads(payload)
        except Exception:
            return {"status": "error", "reason": "Invalid JSON"}
        # From instruction config, find settings if needed
        settings = self.instruction_config.get('api2', {}).get('protocolPropertyList', {})
        # For this device, simply forward the JSON as HTTP POST
        try:
            url = f"{self.device_address.rstrip('/')}/commands/control"
            r = requests.post(url, json=data, timeout=5, headers={"Content-Type": "application/json"})
            return {"status": "ok", "resp": r.json() if r.headers.get("Content-Type","").startswith("application/json") else r.text}
        except Exception as e:
            return {"status": "error", "reason": str(e)}

    def handle_telemetry(self, payload):
        if not self.device_connected or not self.device_address:
            return {"status": "error", "reason": "Device not connected"}
        # Instructed via instruction config for telemetry
        settings = self.instruction_config.get('api1', {}).get('protocolPropertyList', {})
        # For this device, simply GET /telemetry/state
        try:
            url = f"{self.device_address.rstrip('/')}/telemetry/state"
            r = requests.get(url, timeout=5)
            return {"status": "ok", "resp": r.json() if r.headers.get("Content-Type","").startswith("application/json") else r.text}
        except Exception as e:
            return {"status": "error", "reason": str(e)}

if __name__ == "__main__":
    try:
        shifu = DeviceShifuLite3()
        shifu.start()
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        traceback.print_exc()
        sys.exit(1)