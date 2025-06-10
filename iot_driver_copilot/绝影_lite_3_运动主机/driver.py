import os
import threading
import time
import signal
import sys
import yaml
import json

from flask import Flask, request, jsonify
import paho.mqtt.client as mqtt

from kubernetes import client as k8s_client, config as k8s_config

# --- Configuration and Environment Variables ---
EDGEDEVICE_NAME = os.environ["EDGEDEVICE_NAME"]
EDGEDEVICE_NAMESPACE = os.environ["EDGEDEVICE_NAMESPACE"]
MQTT_BROKER_ADDRESS = os.environ["MQTT_BROKER_ADDRESS"]

HTTP_SERVER_HOST = os.environ.get("HTTP_SERVER_HOST", "0.0.0.0")
HTTP_SERVER_PORT = int(os.environ.get("HTTP_SERVER_PORT", 8080))

INSTRUCTIONS_PATH = "/etc/edgedevice/config/instructions"

# --- Kubernetes Client Initialization ---
def init_k8s_client():
    k8s_config.load_incluster_config()
    return k8s_client.CustomObjectsApi()

k8s_api = init_k8s_client()
CRD_GROUP = "shifu.edgenesis.io"
CRD_VERSION = "v1alpha1"
CRD_PLURAL = "edgedevices"

# --- Status Management ---
EDGEDEVICE_PHASES = ["Pending", "Running", "Failed", "Unknown"]

def update_device_phase(phase):
    body = {
        "status": {
            "edgeDevicePhase": phase
        }
    }
    try:
        k8s_api.patch_namespaced_custom_object_status(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=EDGEDEVICE_NAMESPACE,
            plural=CRD_PLURAL,
            name=EDGEDEVICE_NAME,
            body=body
        )
    except Exception as e:
        # Logging can be added here if desired
        pass

def get_device_address():
    try:
        obj = k8s_api.get_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=EDGEDEVICE_NAMESPACE,
            plural=CRD_PLURAL,
            name=EDGEDEVICE_NAME
        )
        return obj.get("spec", {}).get("address", None)
    except Exception:
        return None

# --- Instructions Loader ---
def load_instructions():
    try:
        with open(INSTRUCTIONS_PATH, "r") as f:
            return yaml.safe_load(f)
    except Exception:
        return {}

INSTRUCTIONS = load_instructions()

# --- MQTT Client Setup ---
mqtt_client = mqtt.Client()
mqtt_client_connected = threading.Event()
mqtt_last_connect_success = [False]
telemetry_buffer = []
telemetry_lock = threading.Lock()
MQTT_TELEMETRY_TOPIC = "device/telemetry"

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        mqtt_client_connected.set()
        mqtt_last_connect_success[0] = True
        try:
            client.subscribe(MQTT_TELEMETRY_TOPIC, qos=1)
        except Exception:
            pass
        update_device_phase("Running")
    else:
        mqtt_client_connected.clear()
        mqtt_last_connect_success[0] = False
        update_device_phase("Failed")

def on_disconnect(client, userdata, rc):
    mqtt_client_connected.clear()
    if rc != 0:
        update_device_phase("Failed")
    else:
        update_device_phase("Pending")

def on_telemetry(client, userdata, msg):
    try:
        data = msg.payload.decode("utf-8")
        with telemetry_lock:
            telemetry_buffer.append(json.loads(data))
            # Optionally limit buffer size
            if len(telemetry_buffer) > 100:
                telemetry_buffer.pop(0)
    except Exception:
        pass

mqtt_client.on_connect = on_connect
mqtt_client.on_disconnect = on_disconnect
mqtt_client.message_callback_add(MQTT_TELEMETRY_TOPIC, on_telemetry)

def mqtt_connect_loop():
    while True:
        try:
            broker_host, broker_port = MQTT_BROKER_ADDRESS.split(":")
            broker_port = int(broker_port)
            mqtt_client.connect(broker_host, broker_port, keepalive=60)
            mqtt_client.loop_start()
            # Wait for connection
            if not mqtt_client_connected.wait(timeout=10):
                update_device_phase("Failed")
        except Exception:
            update_device_phase("Failed")
        # Check connection every 10 seconds
        time.sleep(10)

mqtt_thread = threading.Thread(target=mqtt_connect_loop, daemon=True)
mqtt_thread.start()

# --- Flask App for HTTP API ---
app = Flask(__name__)

# --- Helper: Protocol Property Extraction ---
def get_api_settings(api_name):
    instructions = INSTRUCTIONS.get(api_name, {})
    return instructions.get('protocolPropertyList', {})

# --- API: /api/motion (maps to MQTT PUBLISH device/commands/motion) ---
@app.route("/api/motion", methods=["POST"])
def api_motion():
    if not mqtt_client_connected.is_set():
        return jsonify({"error": "MQTT not connected"}), 503
    payload = request.get_json(force=True)
    settings = get_api_settings("api1")
    qos = settings.get("qos", 1)
    topic = "device/commands/motion"
    try:
        mqtt_client.publish(topic, json.dumps(payload), qos=qos)
        return jsonify({"status": "published", "topic": topic}), 200
    except Exception:
        return jsonify({"error": "MQTT publish failed"}), 500

# --- API: /api/heartbeat (maps to MQTT PUBLISH device/commands/heartbeat) ---
@app.route("/api/heartbeat", methods=["POST"])
def api_heartbeat():
    if not mqtt_client_connected.is_set():
        return jsonify({"error": "MQTT not connected"}), 503
    payload = request.get_json(force=True)
    settings = get_api_settings("api2")
    qos = settings.get("qos", 1)
    topic = "device/commands/heartbeat"
    try:
        mqtt_client.publish(topic, json.dumps(payload), qos=qos)
        return jsonify({"status": "published", "topic": topic}), 200
    except Exception:
        return jsonify({"error": "MQTT publish failed"}), 500

# --- API: /api/telemetry (maps to MQTT SUBSCRIBE device/telemetry) ---
@app.route("/api/telemetry", methods=["GET"])
def api_telemetry():
    if not mqtt_client_connected.is_set():
        return jsonify({"error": "MQTT not connected"}), 503
    with telemetry_lock:
        return jsonify({"telemetry": telemetry_buffer[-10:]})

# --- Health Endpoint ---
@app.route("/healthz", methods=["GET"])
def healthz():
    phase = "Running" if mqtt_client_connected.is_set() else "Failed"
    return jsonify({"status": phase})

# --- Device Phase Updater (Pending/Unknown if not connected) ---
def device_phase_monitor():
    prev_phase = None
    while True:
        if mqtt_client_connected.is_set():
            phase = "Running"
        elif mqtt_last_connect_success[0]:
            phase = "Failed"
        else:
            phase = "Pending"
        if phase != prev_phase:
            update_device_phase(phase)
            prev_phase = phase
        time.sleep(5)

phase_thread = threading.Thread(target=device_phase_monitor, daemon=True)
phase_thread.start()

# --- Graceful Shutdown Handler ---
def signal_handler(sig, frame):
    update_device_phase("Unknown")
    try:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
    except Exception:
        pass
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# --- START FLASK SERVER ---
if __name__ == "__main__":
    # Set initial phase to Pending
    update_device_phase("Pending")
    app.run(host=HTTP_SERVER_HOST, port=HTTP_SERVER_PORT)