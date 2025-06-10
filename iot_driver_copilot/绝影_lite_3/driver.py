import os
import threading
import time
import yaml
import json
from flask import Flask, request, jsonify
import paho.mqtt.client as mqtt
from kubernetes import client, config

# --- Kubernetes CRD Handling ---

EDGEDEVICE_NAME = os.environ['EDGEDEVICE_NAME']
EDGEDEVICE_NAMESPACE = os.environ['EDGEDEVICE_NAMESPACE']
MQTT_BROKER_ADDRESS = os.environ['MQTT_BROKER_ADDRESS']

HTTP_SERVER_HOST = os.environ.get('HTTP_SERVER_HOST', '0.0.0.0')
HTTP_SERVER_PORT = int(os.environ.get('HTTP_SERVER_PORT', '8080'))

INSTRUCTION_CONFIG_PATH = '/etc/edgedevice/config/instructions'

PHASE_PENDING = "Pending"
PHASE_RUNNING = "Running"
PHASE_FAILED = "Failed"
PHASE_UNKNOWN = "Unknown"

# --- Load config map instructions ---
def load_api_settings(path):
    try:
        with open(path, 'r') as f:
            return yaml.safe_load(f)
    except Exception:
        return {}

api_settings = load_api_settings(INSTRUCTION_CONFIG_PATH)

# --- MQTT Client Handling ---
class MQTTManager:
    def __init__(self, broker_addr):
        self.broker_addr, self.broker_port = self._parse_broker_addr(broker_addr)
        self.client = mqtt.Client()
        self.connected = False
        self.subscriptions = {}  # topic: latest_msg
        self.connection_error = None

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

    def _parse_broker_addr(self, addr):
        if ':' in addr:
            host, port = addr.split(':', 1)
            return host, int(port)
        return addr, 1883

    def connect(self):
        try:
            self.client.connect(self.broker_addr, self.broker_port, 60)
            self.client.loop_start()
            # Wait for connection up to 3 seconds
            for _ in range(30):
                if self.connected:
                    return True
                time.sleep(0.1)
            return False
        except Exception as e:
            self.connection_error = str(e)
            return False

    def on_connect(self, client, userdata, flags, rc):
        self.connected = (rc == 0)

    def on_disconnect(self, client, userdata, rc):
        self.connected = False

    def subscribe(self, topic, qos=1):
        self.subscriptions[topic] = None
        self.client.subscribe(topic, qos=qos)

    def on_message(self, client, userdata, msg):
        if msg.topic in self.subscriptions:
            # Only store latest message per topic
            try:
                self.subscriptions[msg.topic] = msg.payload.decode('utf-8')
            except Exception:
                self.subscriptions[msg.topic] = msg.payload

    def get_latest(self, topic):
        return self.subscriptions.get(topic)

    def publish(self, topic, payload, qos=1):
        return self.client.publish(topic, payload, qos=qos)

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()

# --- Kubernetes CRD Status Updater ---
class KubeEdgeDeviceStatusUpdater(threading.Thread):
    def __init__(self, name, namespace, mqtt_manager):
        super().__init__(daemon=True)
        self.name = name
        self.namespace = namespace
        self.mqtt_manager = mqtt_manager
        self.stop_event = threading.Event()
        self.last_phase = None

        try:
            config.load_incluster_config()
        except Exception:
            config.load_kube_config()
        self.api = client.CustomObjectsApi()

    def run(self):
        while not self.stop_event.is_set():
            try:
                dev = self.api.get_namespaced_custom_object(
                    group="shifu.edgenesis.io",
                    version="v1alpha1",
                    namespace=self.namespace,
                    plural="edgedevices",
                    name=self.name
                )
                # Determine new phase
                if not self.mqtt_manager:
                    phase = PHASE_UNKNOWN
                elif self.mqtt_manager.connected:
                    phase = PHASE_RUNNING
                elif self.mqtt_manager.connection_error:
                    phase = PHASE_FAILED
                else:
                    phase = PHASE_PENDING
                if dev.get('status', {}).get('edgeDevicePhase') != phase:
                    body = {'status': {'edgeDevicePhase': phase}}
                    self.api.patch_namespaced_custom_object_status(
                        group="shifu.edgenesis.io",
                        version="v1alpha1",
                        namespace=self.namespace,
                        plural="edgedevices",
                        name=self.name,
                        body=body
                    )
                self.last_phase = phase
            except Exception:
                pass
            time.sleep(2)

    def stop(self):
        self.stop_event.set()

# --- Flask App Setup ---
app = Flask(__name__)

# --- DeviceShifu API Implementation ---
# API: Motion Command Publisher
@app.route('/api/motion', methods=['POST'])
def api_motion():
    payload = request.get_json(force=True)
    topic = "device/commands/motion"
    qos = api_settings.get('api_motion', {}).get('protocolPropertyList', {}).get('qos', 1)
    result = mqtt_manager.publish(topic, json.dumps(payload), qos=int(qos))
    return jsonify({'result': 'sent', 'mqtt_rc': result.rc})

# API: Heartbeat Publisher
@app.route('/api/heartbeat', methods=['POST'])
def api_heartbeat():
    payload = request.get_json(force=True)
    topic = "device/commands/heartbeat"
    qos = api_settings.get('api_heartbeat', {}).get('protocolPropertyList', {}).get('qos', 1)
    result = mqtt_manager.publish(topic, json.dumps(payload), qos=int(qos))
    return jsonify({'result': 'sent', 'mqtt_rc': result.rc})

# API: State Command Publisher
@app.route('/api/state', methods=['POST'])
def api_state():
    payload = request.get_json(force=True)
    topic = "device/commands/state"
    qos = api_settings.get('api_state', {}).get('protocolPropertyList', {}).get('qos', 1)
    result = mqtt_manager.publish(topic, json.dumps(payload), qos=int(qos))
    return jsonify({'result': 'sent', 'mqtt_rc': result.rc})

# API: Subscribe to robot state sensors (GET latest)
@app.route('/api/sensors/state', methods=['GET'])
def api_sensors_state():
    topic = "device/sensors/state"
    msg = mqtt_manager.get_latest(topic)
    if msg is None:
        return jsonify({'status': 'no_data_yet'}), 204
    try:
        obj = json.loads(msg)
    except Exception:
        obj = msg
    return jsonify(obj)

# API: Subscribe to joint sensors (GET latest)
@app.route('/api/sensors/joint', methods=['GET'])
def api_sensors_joint():
    topic = "device/sensors/joint"
    msg = mqtt_manager.get_latest(topic)
    if msg is None:
        return jsonify({'status': 'no_data_yet'}), 204
    try:
        obj = json.loads(msg)
    except Exception:
        obj = msg
    return jsonify(obj)

# --- Main Entrypoint ---
if __name__ == "__main__":
    mqtt_manager = MQTTManager(MQTT_BROKER_ADDRESS)
    mqtt_manager.connect()
    # Subscribe as per API
    mqtt_manager.subscribe("device/sensors/state", qos=1)
    mqtt_manager.subscribe("device/sensors/joint", qos=1)
    # Start Kubernetes CRD status updater
    kube_status_updater = KubeEdgeDeviceStatusUpdater(
        EDGEDEVICE_NAME, EDGEDEVICE_NAMESPACE, mqtt_manager
    )
    kube_status_updater.start()
    try:
        app.run(host=HTTP_SERVER_HOST, port=HTTP_SERVER_PORT, threaded=True)
    finally:
        kube_status_updater.stop()
        mqtt_manager.disconnect()