import os
import time
import threading
import yaml
import json

from kubernetes import client, config
from kubernetes.client.rest import ApiException

import paho.mqtt.client as mqtt
from flask import Flask, request, jsonify

# Constants
CONFIGMAP_INSTRUCTION_PATH = "/etc/edgedevice/config/instructions"
EDGEDEVICE_CRD_GROUP = "shifu.edgenesis.io"
EDGEDEVICE_CRD_VERSION = "v1alpha1"
EDGEDEVICE_CRD_PLURAL = "edgedevices"

# DeviceShifu Environment Variables
EDGEDEVICE_NAME = os.environ["EDGEDEVICE_NAME"]
EDGEDEVICE_NAMESPACE = os.environ["EDGEDEVICE_NAMESPACE"]
MQTT_BROKER_ADDRESS = os.environ["MQTT_BROKER_ADDRESS"]
MQTT_BROKER_HOST, MQTT_BROKER_PORT = MQTT_BROKER_ADDRESS.split(":")
MQTT_BROKER_PORT = int(MQTT_BROKER_PORT)

# Load ConfigMap settings
def load_instruction_settings():
    try:
        with open(CONFIGMAP_INSTRUCTION_PATH, "r") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}

instruction_settings = load_instruction_settings()

# Kubernetes API client setup (in-cluster)
config.load_incluster_config()
api = client.CustomObjectsApi()

def get_edgedevice():
    try:
        return api.get_namespaced_custom_object(
            group=EDGEDEVICE_CRD_GROUP,
            version=EDGEDEVICE_CRD_VERSION,
            namespace=EDGEDEVICE_NAMESPACE,
            plural=EDGEDEVICE_CRD_PLURAL,
            name=EDGEDEVICE_NAME
        )
    except ApiException:
        return None

def patch_edgedevice_status(phase):
    body = {"status": {"edgeDevicePhase": phase}}
    try:
        api.patch_namespaced_custom_object_status(
            group=EDGEDEVICE_CRD_GROUP,
            version=EDGEDEVICE_CRD_VERSION,
            namespace=EDGEDEVICE_NAMESPACE,
            plural=EDGEDEVICE_CRD_PLURAL,
            name=EDGEDEVICE_NAME,
            body=body
        )
    except ApiException:
        pass

# MQTT Client
class MQTTDeviceClient:
    def __init__(self, broker_host, broker_port):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client = mqtt.Client()
        self.connected = False
        self.subscriptions = {}
        self._connect_lock = threading.Lock()
        self.should_run = True
        self.status_phase = "Pending"
        self._setup_handlers()

    def _setup_handlers(self):
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected = True
            self.status_phase = "Running"
            patch_edgedevice_status("Running")
            for topic, qos in self.subscriptions.items():
                self.client.subscribe(topic, qos)
        else:
            self.connected = False
            self.status_phase = "Failed"
            patch_edgedevice_status("Failed")

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False
        if self.should_run:
            self.status_phase = "Pending"
            patch_edgedevice_status("Pending")

    def _on_message(self, client, userdata, msg):
        if msg.topic in self.subscriptions:
            callback = self.subscriptions[msg.topic]
            if callable(callback):
                try:
                    callback(msg.topic, msg.payload)
                except Exception:
                    pass

    def connect(self):
        with self._connect_lock:
            try:
                self.client.connect(self.broker_host, self.broker_port, 60)
                patch_edgedevice_status("Pending")
                threading.Thread(target=self.client.loop_forever, daemon=True).start()
            except Exception:
                self.connected = False
                self.status_phase = "Failed"
                patch_edgedevice_status("Failed")

    def disconnect(self):
        self.should_run = False
        try:
            self.client.disconnect()
        except Exception:
            pass
        self.connected = False
        patch_edgedevice_status("Pending")

    def publish(self, topic, payload, qos=1):
        if not self.connected:
            raise Exception("MQTT not connected")
        (rc, mid) = self.client.publish(topic, payload, qos=qos)
        return rc == mqtt.MQTT_ERR_SUCCESS

    def subscribe(self, topic, qos=1, callback=None):
        if not self.connected:
            raise Exception("MQTT not connected")
        self.subscriptions[topic] = callback
        self.client.subscribe(topic, qos)

    def set_status_unknown(self):
        self.status_phase = "Unknown"
        patch_edgedevice_status("Unknown")

# DeviceShifu API Implementation
app = Flask("device_shifu_driver")

mqtt_device = MQTTDeviceClient(MQTT_BROKER_HOST, MQTT_BROKER_PORT)
mqtt_device.connect()

# Helper: get protocolPropertyList settings for API
def get_api_settings(api_name):
    api_conf = instruction_settings.get(api_name, {})
    return api_conf.get("protocolPropertyList", {})

# API: /device/commands/movement [POST]
@app.route("/device/commands/movement", methods=["POST"])
def api_publish_movement():
    settings = get_api_settings("device/commands/movement")
    qos = int(settings.get("qos", 1))
    data = request.json
    try:
        payload = json.dumps(data)
        mqtt_device.publish("device/commands/movement", payload, qos)
        return jsonify({"result": "success"}), 200
    except Exception as e:
        return jsonify({"result": "fail", "reason": str(e)}), 500

# API: /device/commands/state [POST]
@app.route("/device/commands/state", methods=["POST"])
def api_publish_state():
    settings = get_api_settings("device/commands/state")
    qos = int(settings.get("qos", 1))
    data = request.json
    try:
        payload = json.dumps(data)
        mqtt_device.publish("device/commands/state", payload, qos)
        return jsonify({"result": "success"}), 200
    except Exception as e:
        return jsonify({"result": "fail", "reason": str(e)}), 500

# API: /device/sensors/telemetry [GET]
telemetry_data_buffer = []

def telemetry_callback(topic, payload):
    try:
        data = json.loads(payload)
        telemetry_data_buffer.append(data)
    except Exception:
        pass

@app.route("/device/sensors/telemetry", methods=["GET"])
def api_subscribe_telemetry():
    settings = get_api_settings("device/sensors/telemetry")
    qos = int(settings.get("qos", 1))
    topic = "device/sensors/telemetry"
    try:
        mqtt_device.subscribe(topic, qos=qos, callback=telemetry_callback)
        time.sleep(0.2)
        data = list(telemetry_data_buffer)
        telemetry_data_buffer.clear()
        return jsonify({"data": data}), 200
    except Exception as e:
        return jsonify({"result": "fail", "reason": str(e)}), 500

# Health check and EdgeDevice status update thread
def status_monitor():
    while True:
        try:
            device_obj = get_edgedevice()
            addr = device_obj.get("spec", {}).get("address")
            if not addr:
                mqtt_device.set_status_unknown()
            elif mqtt_device.connected:
                patch_edgedevice_status("Running")
            else:
                patch_edgedevice_status("Pending")
        except Exception:
            patch_edgedevice_status("Unknown")
        time.sleep(10)

threading.Thread(target=status_monitor, daemon=True).start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("SHIFU_SERVICE_HTTP_PORT", 8080)))