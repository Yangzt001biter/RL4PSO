import os
import sys
import time
import threading
import yaml
import json
from kubernetes import client, config, watch
import paho.mqtt.client as mqtt

# ==== Constants and Config ====
EDGEDEVICE_NAME = os.getenv('EDGEDEVICE_NAME')
EDGEDEVICE_NAMESPACE = os.getenv('EDGEDEVICE_NAMESPACE')
MQTT_BROKER_ADDRESS = os.getenv('MQTT_BROKER_ADDRESS')
INSTRUCTION_PATH = "/etc/edgedevice/config/instructions"

if not EDGEDEVICE_NAME or not EDGEDEVICE_NAMESPACE or not MQTT_BROKER_ADDRESS:
    print("Required environment variables missing.", file=sys.stderr)
    sys.exit(1)

# ==== Kubernetes Setup ====
# Load in-cluster config
config.load_incluster_config()
crd_api = client.CustomObjectsApi()

EDGEDEVICE_CRD_GROUP = "shifu.edgenesis.io"
EDGEDEVICE_CRD_VERSION = "v1alpha1"
EDGEDEVICE_CRD_PLURAL = "edgedevices"

# ==== DeviceShifu Status Management ====
def update_edge_device_phase(phase):
    for _ in range(3):
        try:
            body = {
                "status": {
                    "edgeDevicePhase": phase
                }
            }
            crd_api.patch_namespaced_custom_object_status(
                group=EDGEDEVICE_CRD_GROUP,
                version=EDGEDEVICE_CRD_VERSION,
                namespace=EDGEDEVICE_NAMESPACE,
                plural=EDGEDEVICE_CRD_PLURAL,
                name=EDGEDEVICE_NAME,
                body=body
            )
            return
        except Exception as e:
            time.sleep(1)

def get_edgedevice_resource():
    return crd_api.get_namespaced_custom_object(
        group=EDGEDEVICE_CRD_GROUP,
        version=EDGEDEVICE_CRD_VERSION,
        namespace=EDGEDEVICE_NAMESPACE,
        plural=EDGEDEVICE_CRD_PLURAL,
        name=EDGEDEVICE_NAME
    )

def get_device_address():
    resource = get_edgedevice_resource()
    return resource.get("spec", {}).get("address", "")

# ==== Load API Settings from ConfigMap ====
def load_api_settings():
    try:
        with open(INSTRUCTION_PATH, "r") as f:
            return yaml.safe_load(f)
    except Exception:
        return {}

api_settings = load_api_settings()

# ==== MQTT Client Management ====
class MQTTDeviceClient:
    def __init__(self, broker_address):
        self.broker_host, self.broker_port = self._parse_broker_address(broker_address)
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.connected = False
        self.device_status = "Pending"
        self.subscriptions = {}
        self._lock = threading.Lock()
        self._connect_thread = None

    def _parse_broker_address(self, addr):
        if ':' in addr:
            host, port = addr.split(':', 1)
            return host, int(port)
        return addr, 1883

    def connect(self):
        self._connect_thread = threading.Thread(target=self._connect_loop, daemon=True)
        self._connect_thread.start()

    def _connect_loop(self):
        while True:
            try:
                self.client.connect(self.broker_host, self.broker_port, keepalive=60)
                self.client.loop_start()
                break
            except Exception:
                update_edge_device_phase("Failed")
                time.sleep(5)

    def on_connect(self, client, userdata, flags, rc):
        with self._lock:
            self.connected = True if rc == 0 else False
            self.device_status = "Running" if self.connected else "Failed"
            update_edge_device_phase(self.device_status)
            for topic, subinfo in self.subscriptions.items():
                self.client.subscribe(topic, subinfo['qos'])

    def on_disconnect(self, client, userdata, rc):
        with self._lock:
            self.connected = False
            self.device_status = "Pending"
            update_edge_device_phase("Pending")
            # Attempt reconnect in background
            self._connect_thread = threading.Thread(target=self._connect_loop, daemon=True)
            self._connect_thread.start()

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        try:
            payload = msg.payload.decode()
            if topic in self.subscriptions:
                cb = self.subscriptions[topic].get('callback')
                if cb:
                    cb(topic, payload)
        except Exception:
            pass

    def publish(self, topic, payload, qos=1):
        if not self.connected:
            raise Exception("MQTT not connected")
        self.client.publish(topic, json.dumps(payload), qos=qos)

    def subscribe(self, topic, qos=1, callback=None):
        with self._lock:
            self.subscriptions[topic] = {'qos': qos, 'callback': callback}
            if self.connected:
                self.client.subscribe(topic, qos)

# ==== Device API Implementation ====
class SmartPlugAPI:
    def __init__(self, mqtt_client, api_settings):
        self.mqtt_client = mqtt_client
        self.api_settings = api_settings
        self._consumption_data = None
        self._consumption_lock = threading.Lock()
        self._setup_subscriptions()

    def _setup_subscriptions(self):
        consumption_api = self.api_settings.get("device/sensors/consumption", {})
        qos = consumption_api.get("protocolPropertyList", {}).get("qos", 1)
        self.mqtt_client.subscribe("device/sensors/consumption", qos=qos, callback=self._on_consumption)

    def _on_consumption(self, topic, payload):
        with self._consumption_lock:
            self._consumption_data = payload

    def get_power_consumption(self, timeout=5):
        # Wait for the latest message if not available
        start = time.time()
        while True:
            with self._consumption_lock:
                data = self._consumption_data
            if data:
                try:
                    return json.loads(data)
                except Exception:
                    return {"error": "Invalid JSON"}
            if time.time() - start > timeout:
                return {"error": "Timeout waiting for data"}
            time.sleep(0.1)

    def set_power_state(self, state):
        if state not in ('on', 'off'):
            return {"error": "Invalid state"}
        command_api = self.api_settings.get("device/commands/power", {})
        qos = command_api.get("protocolPropertyList", {}).get("qos", 1)
        payload = {"state": state}
        self.mqtt_client.publish("device/commands/power", payload, qos=qos)
        return {"result": "sent"}

# ==== Main DeviceShifu Entrypoint ====
def main():
    update_edge_device_phase("Pending")
    device_address = get_device_address()
    mqtt_client = MQTTDeviceClient(MQTT_BROKER_ADDRESS)
    mqtt_client.connect()
    api = SmartPlugAPI(mqtt_client, api_settings)

    # Simple HTTP server to expose APIs (since DeviceShifu expects HTTP)
    # Only expose the two required APIs
    from http.server import BaseHTTPRequestHandler, HTTPServer

    class Handler(BaseHTTPRequestHandler):
        def _set_headers(self, status=200):
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()

        def do_GET(self):
            if self.path == '/api/get_power_consumption':
                result = api.get_power_consumption()
                self._set_headers()
                self.wfile.write(json.dumps(result).encode())
            else:
                self._set_headers(404)
                self.wfile.write(json.dumps({"error": "Not found"}).encode())

        def do_POST(self):
            if self.path == '/api/set_power_state':
                length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(length)
                try:
                    data = json.loads(body)
                    state = data.get('state')
                except Exception:
                    self._set_headers(400)
                    self.wfile.write(json.dumps({"error": "Invalid payload"}).encode())
                    return
                result = api.set_power_state(state)
                self._set_headers()
                self.wfile.write(json.dumps(result).encode())
            else:
                self._set_headers(404)
                self.wfile.write(json.dumps({"error": "Not found"}).encode())

        def log_message(self, format, *args):
            return  # Silence logging

    # Start HTTP server (port configurable via env)
    port = int(os.getenv("DEVICE_SHIFU_HTTP_PORT", "8080"))
    server = HTTPServer(('', port), Handler)
    try:
        update_edge_device_phase("Running")
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        update_edge_device_phase("Pending")

if __name__ == "__main__":
    try:
        main()
    except Exception:
        update_edge_device_phase("Failed")
        sys.exit(1)