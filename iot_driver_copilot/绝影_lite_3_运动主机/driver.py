import os
import sys
import time
import threading
import yaml
import json
from flask import Flask, request, jsonify
import paho.mqtt.client as mqtt
from kubernetes import client as k8s_client, config as k8s_config

# Environment variables required
EDGEDEVICE_NAME = os.environ.get("EDGEDEVICE_NAME")
EDGEDEVICE_NAMESPACE = os.environ.get("EDGEDEVICE_NAMESPACE")
MQTT_BROKER_ADDRESS = os.environ.get("MQTT_BROKER_ADDRESS")
HTTP_SERVER_HOST = os.environ.get("HTTP_SERVER_HOST", "0.0.0.0")
HTTP_SERVER_PORT = int(os.environ.get("HTTP_SERVER_PORT", "8080"))
INSTRUCTION_PATH = "/etc/edgedevice/config/instructions"

if not EDGEDEVICE_NAME or not EDGEDEVICE_NAMESPACE or not MQTT_BROKER_ADDRESS:
    print("Missing required environment variables.", file=sys.stderr)
    sys.exit(1)

# Load API instructions from YAML
def load_api_instructions():
    try:
        with open(INSTRUCTION_PATH, "r") as f:
            return yaml.safe_load(f)
    except Exception:
        return {}

api_instructions = load_api_instructions()

# Kubernetes CRD client setup
def k8s_init():
    try:
        k8s_config.load_incluster_config()
    except Exception:
        print("K8s in-cluster config load failed.", file=sys.stderr)
        sys.exit(1)

k8s_init()
crd_api = k8s_client.CustomObjectsApi()

EDGEDEVICE_CRD_GROUP = "shifu.edgenesis.io"
EDGEDEVICE_CRD_VERSION = "v1alpha1"
EDGEDEVICE_CRD_PLURAL = "edgedevices"

def get_edgedevice():
    return crd_api.get_namespaced_custom_object(
        EDGEDEVICE_CRD_GROUP,
        EDGEDEVICE_CRD_VERSION,
        EDGEDEVICE_NAMESPACE,
        EDGEDEVICE_CRD_PLURAL,
        EDGEDEVICE_NAME,
    )

def update_edgedevice_phase(phase):
    for _ in range(3):
        try:
            body = {"status": {"edgeDevicePhase": phase}}
            crd_api.patch_namespaced_custom_object_status(
                EDGEDEVICE_CRD_GROUP,
                EDGEDEVICE_CRD_VERSION,
                EDGEDEVICE_NAMESPACE,
                EDGEDEVICE_CRD_PLURAL,
                EDGEDEVICE_NAME,
                body,
            )
            return
        except Exception:
            time.sleep(1)

# Get device address from EdgeDevice CRD
def get_device_address():
    try:
        ed = get_edgedevice()
        return ed.get("spec", {}).get("address", None)
    except Exception:
        return None

# MQTT Setup and Callbacks
mqtt_client = mqtt.Client()
mqtt_connected = threading.Event()
mqtt_failed = threading.Event()
telemetry_data = []
telemetry_lock = threading.Lock()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        mqtt_connected.set()
        mqtt_failed.clear()
        update_edgedevice_phase("Running")
        # Subscribe to telemetry topic
        client.subscribe("device/telemetry", qos=1)
    else:
        mqtt_failed.set()
        mqtt_connected.clear()
        update_edgedevice_phase("Failed")

def on_disconnect(client, userdata, rc):
    mqtt_connected.clear()
    if rc != 0:
        update_edgedevice_phase("Failed")
    else:
        update_edgedevice_phase("Pending")

def on_message(client, userdata, msg):
    if msg.topic == "device/telemetry":
        try:
            payload = msg.payload.decode("utf-8")
            data = json.loads(payload)
            with telemetry_lock:
                telemetry_data.append(data)
                if len(telemetry_data) > 100:
                    telemetry_data.pop(0)
        except Exception:
            pass

mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect
mqtt_client.on_message = on_message

def mqtt_connect_loop():
    broker_host, broker_port = MQTT_BROKER_ADDRESS.split(":")
    broker_port = int(broker_port)
    while True:
        try:
            device_address = get_device_address()
            if not device_address:
                update_edgedevice_phase("Unknown")
                time.sleep(5)
                continue
            mqtt_client.connect(broker_host, broker_port, keepalive=60)
            mqtt_client.loop_forever()
        except Exception:
            update_edgedevice_phase("Failed")
            time.sleep(5)

mqtt_thread = threading.Thread(target=mqtt_connect_loop, daemon=True)
mqtt_thread.start()

# Flask HTTP API
app = Flask(__name__)

def get_protocol_settings(api_name):
    return api_instructions.get(api_name, {}).get("protocolPropertyList", {})

@app.route("/api/motion", methods=["POST"])
def api_motion():
    # Publish motion control command to MQTT
    payload = request.get_json(force=True)
    settings = get_protocol_settings("api1")  # As per instructions YAML
    qos = int(settings.get("qos", 1))
    try:
        mqtt_client.publish("device/commands/motion", json.dumps(payload), qos=qos)
        return jsonify({"status": "sent", "topic": "device/commands/motion"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/heartbeat", methods=["POST"])
def api_heartbeat():
    # Publish heartbeat command to MQTT
    payload = request.get_json(force=True)
    settings = get_protocol_settings("api2")
    qos = int(settings.get("qos", 1))
    try:
        mqtt_client.publish("device/commands/heartbeat", json.dumps(payload), qos=qos)
        return jsonify({"status": "sent", "topic": "device/commands/heartbeat"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route("/api/telemetry", methods=["GET"])
def api_telemetry():
    # Return latest telemetry data received from device
    with telemetry_lock:
        data = telemetry_data[-1] if telemetry_data else {}
    return jsonify(data), 200

@app.route("/healthz", methods=["GET"])
def healthz():
    return jsonify({"mqtt_connected": mqtt_connected.is_set()}), 200

def phase_update_loop():
    # Monitor the MQTT connection and update EdgeDevice phase as needed
    last_phase = None
    while True:
        if mqtt_connected.is_set():
            phase = "Running"
        elif mqtt_failed.is_set():
            phase = "Failed"
        else:
            phase = "Pending"
        if phase != last_phase:
            update_edgedevice_phase(phase)
            last_phase = phase
        time.sleep(3)

phase_thread = threading.Thread(target=phase_update_loop, daemon=True)
phase_thread.start()

if __name__ == "__main__":
    app.run(host=HTTP_SERVER_HOST, port=HTTP_SERVER_PORT, threaded=True)