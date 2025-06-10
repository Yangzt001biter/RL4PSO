import os
import threading
import time
import yaml
import json

from flask import Flask, request, jsonify, Response, stream_with_context
import paho.mqtt.client as mqtt
from kubernetes import client as k8s_client, config as k8s_config

# --- Configuration and Constants ---

EDGEDEVICE_NAME = os.environ.get('EDGEDEVICE_NAME')
EDGEDEVICE_NAMESPACE = os.environ.get('EDGEDEVICE_NAMESPACE')
MQTT_BROKER_ADDRESS = os.environ.get('MQTT_BROKER_ADDRESS')
HTTP_SERVER_HOST = os.environ.get('HTTP_SERVER_HOST', '0.0.0.0')
HTTP_SERVER_PORT = int(os.environ.get('HTTP_SERVER_PORT', '8080'))

INSTRUCTION_CONFIG_PATH = '/etc/edgedevice/config/instructions'

EDGEDEVICE_CRD_GROUP = "shifu.edgenesis.io"
EDGEDEVICE_CRD_VERSION = "v1alpha1"
EDGEDEVICE_CRD_PLURAL = "edgedevices"

SUBSCRIBE_TOPIC = "lite3/sensors/state"
PUBLISH_TOPIC = "lite3/commands/control"

# --- Globals ---

app = Flask(__name__)

instruction_settings = {}
mqtt_client = None
mqtt_connected = threading.Event()
latest_state_payload = None
latest_state_lock = threading.Lock()
subscribers = []

# --- Kubernetes API Helpers ---

def k8s_init():
    try:
        k8s_config.load_incluster_config()
    except Exception:
        k8s_config.load_kube_config()
    api = k8s_client.CustomObjectsApi()
    return api

def fetch_edge_device(api):
    try:
        ed = api.get_namespaced_custom_object(
            group=EDGEDEVICE_CRD_GROUP,
            version=EDGEDEVICE_CRD_VERSION,
            namespace=EDGEDEVICE_NAMESPACE,
            plural=EDGEDEVICE_CRD_PLURAL,
            name=EDGEDEVICE_NAME,
        )
        return ed
    except Exception:
        return None

def update_edge_device_phase(api, phase):
    body = {
        "status": {
            "edgeDevicePhase": phase
        }
    }
    try:
        api.patch_namespaced_custom_object_status(
            group=EDGEDEVICE_CRD_GROUP,
            version=EDGEDEVICE_CRD_VERSION,
            namespace=EDGEDEVICE_NAMESPACE,
            plural=EDGEDEVICE_CRD_PLURAL,
            name=EDGEDEVICE_NAME,
            body=body
        )
    except Exception:
        pass

def get_device_address(api):
    ed = fetch_edge_device(api)
    if ed and 'spec' in ed and 'address' in ed['spec']:
        return ed['spec']['address']
    return None

# --- ConfigMap Parsing ---

def load_instruction_settings():
    global instruction_settings
    try:
        with open(INSTRUCTION_CONFIG_PATH, 'r') as f:
            instruction_settings = yaml.safe_load(f)
    except Exception:
        instruction_settings = {}

# --- MQTT Handling ---

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        mqtt_connected.set()
        # Subscribe to the telemetry topic
        qos = 1
        if "lite3/sensors/state" in instruction_settings:
            qos = instruction_settings["lite3/sensors/state"].get("protocolPropertyList", {}).get("qos", 1)
        client.subscribe(SUBSCRIBE_TOPIC, qos=int(qos))
    else:
        mqtt_connected.clear()

def on_disconnect(client, userdata, rc):
    mqtt_connected.clear()

def on_message(client, userdata, msg):
    global latest_state_payload, subscribers
    if msg.topic == SUBSCRIBE_TOPIC:
        payload = msg.payload.decode('utf-8')
        with latest_state_lock:
            latest_state_payload = payload
        # Notify all SSE subscribers
        for q in list(subscribers):
            try:
                q.put(payload)
            except Exception:
                pass

def mqtt_thread():
    global mqtt_client
    while True:
        try:
            mqtt_client.loop_forever()
        except Exception:
            time.sleep(5)

def mqtt_connect_and_monitor_status():
    k8s_api = k8s_init()
    phase = "Pending"
    prev_phase = None
    while True:
        try:
            if mqtt_connected.is_set():
                phase = "Running"
            else:
                phase = "Pending"
            if not mqtt_client.is_connected():
                try:
                    mqtt_client.reconnect()
                except Exception:
                    phase = "Failed"
            # Check device reachability via CRD
            device_address = get_device_address(k8s_api)
            if not device_address:
                phase = "Unknown"
        except Exception:
            phase = "Unknown"
        if phase != prev_phase:
            update_edge_device_phase(k8s_api, phase)
            prev_phase = phase
        time.sleep(5)

# --- Flask API Endpoints ---

@app.route('/api/telemetry', methods=['GET'])
def api_telemetry():
    # Returns the latest state payload (JSON)
    with latest_state_lock:
        if latest_state_payload:
            return Response(latest_state_payload, content_type='application/json')
        else:
            return jsonify({"error": "No telemetry data received yet"}), 404

@app.route('/api/telemetry/stream', methods=['GET'])
def api_telemetry_stream():
    # Server-Sent Events stream for real-time telemetry
    def event_stream():
        from queue import Queue, Empty
        q = Queue()
        subscribers.append(q)
        try:
            while True:
                try:
                    payload = q.get(timeout=30)
                    yield f'data: {payload}\n\n'
                except Empty:
                    yield ": keepalive\n\n"
        finally:
            subscribers.remove(q)
    headers = {"Content-Type": "text/event-stream"}
    return Response(stream_with_context(event_stream()), headers=headers)

@app.route('/api/command', methods=['POST'])
def api_command():
    # Publishes a command (JSON payload) to the MQTT command topic
    try:
        payload = request.get_json(force=True)
        qos = 1
        if "lite3/commands/control" in instruction_settings:
            qos = instruction_settings["lite3/commands/control"].get("protocolPropertyList", {}).get("qos", 1)
        mqtt_client.publish(PUBLISH_TOPIC, json.dumps(payload), qos=int(qos))
        return jsonify({"result": "Command published"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/health', methods=['GET'])
def api_health():
    return jsonify({"status": "ok", "mqtt_connected": mqtt_connected.is_set()}), 200

# --- Main Entrypoint ---

def main():
    global mqtt_client
    assert EDGEDEVICE_NAME, "EDGEDEVICE_NAME env required"
    assert EDGEDEVICE_NAMESPACE, "EDGEDEVICE_NAMESPACE env required"
    assert MQTT_BROKER_ADDRESS, "MQTT_BROKER_ADDRESS env required"
    load_instruction_settings()

    # MQTT Setup
    mqtt_client_id = f"{EDGEDEVICE_NAME}-{int(time.time())}"
    mqtt_client = mqtt.Client(mqtt_client_id)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message

    # Optionally set MQTT username/password
    mqtt_user = os.environ.get('MQTT_USERNAME')
    mqtt_pass = os.environ.get('MQTT_PASSWORD')
    if mqtt_user and mqtt_pass:
        mqtt_client.username_pw_set(mqtt_user, mqtt_pass)

    # Connect to MQTT broker
    broker_host, broker_port = MQTT_BROKER_ADDRESS.split(":")
    mqtt_client.connect(broker_host, int(broker_port))

    # Start MQTT thread
    t = threading.Thread(target=mqtt_thread, daemon=True)
    t.start()

    # Start Kubernetes status monitor thread
    t2 = threading.Thread(target=mqtt_connect_and_monitor_status, daemon=True)
    t2.start()

    # Start Flask HTTP server
    app.run(host=HTTP_SERVER_HOST, port=HTTP_SERVER_PORT, threaded=True)

if __name__ == "__main__":
    main()