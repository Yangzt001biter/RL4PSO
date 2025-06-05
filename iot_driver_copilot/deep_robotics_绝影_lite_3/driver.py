import os
import sys
import yaml
import time
import json
import threading
import logging

import requests
import paho.mqtt.client as mqtt

from kubernetes import client, config

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DeviceShifu")

# Environment variables
EDGEDEVICE_NAME = os.environ['EDGEDEVICE_NAME']
EDGEDEVICE_NAMESPACE = os.environ['EDGEDEVICE_NAMESPACE']
MQTT_BROKER = os.environ['MQTT_BROKER']
MQTT_PORT = int(os.environ.get('MQTT_PORT', 1883))
MQTT_USERNAME = os.environ.get('MQTT_USERNAME', None)
MQTT_PASSWORD = os.environ.get('MQTT_PASSWORD', None)
MQTT_CLIENT_ID = os.environ.get('MQTT_CLIENT_ID', "device-shifu-client")
MQTT_KEEPALIVE = int(os.environ.get('MQTT_KEEPALIVE', 60))
TELEMETRY_PUB_INTERVAL = int(os.environ.get('TELEMETRY_PUB_INTERVAL', 2))
INSTRUCTION_PATH = '/etc/edgedevice/config/instructions'

# MQTT Topics (can be overridden by env)
TOPIC_COMMAND_ESTOP = os.environ.get('TOPIC_COMMAND_ESTOP', 'device/commands/estop')
TOPIC_COMMAND_STATE = os.environ.get('TOPIC_COMMAND_STATE', 'device/commands/state')
TOPIC_COMMAND_ACTION = os.environ.get('TOPIC_COMMAND_ACTION', 'device/commands/action')
TOPIC_TELEMETRY_DATA = os.environ.get('TOPIC_TELEMETRY_DATA', 'device/telemetry/data')
TOPIC_TELEMETRY_STATUS = os.environ.get('TOPIC_TELEMETRY_STATUS', 'device/telemetry/status')

# DeviceShifu <--> Real Device HTTP
HTTP_TIMEOUT = int(os.environ.get('HTTP_TIMEOUT', 5))

# Kubernetes client setup (in-cluster)
def k8s_init():
    try:
        config.load_incluster_config()
    except Exception as e:
        logger.error(f"K8s incluster config failed: {e}")
        sys.exit(1)
    return client.CustomObjectsApi()

k8s_api = k8s_init()

# CRD constants
CRD_GROUP = "shifu.edgenesis.io"
CRD_VERSION = "v1alpha1"
CRD_PLURAL = "edgedevices"

def update_edge_device_phase(phase):
    """Update .status.edgeDevicePhase in EdgeDevice CR."""
    for _ in range(3):
        try:
            body = {'status': {'edgeDevicePhase': phase}}
            k8s_api.patch_namespaced_custom_object_status(
                group=CRD_GROUP,
                version=CRD_VERSION,
                namespace=EDGEDEVICE_NAMESPACE,
                plural=CRD_PLURAL,
                name=EDGEDEVICE_NAME,
                body=body
            )
            logger.info(f"Updated EdgeDevice status.phase to {phase}")
            return
        except Exception as e:
            logger.warning(f"Failed to update EdgeDevice phase: {e}")
            time.sleep(1)
    logger.error("Giving up updating EdgeDevice status after 3 tries.")

def get_device_address():
    """Fetch .spec.address from EdgeDevice CR"""
    try:
        obj = k8s_api.get_namespaced_custom_object(
            group=CRD_GROUP,
            version=CRD_VERSION,
            namespace=EDGEDEVICE_NAMESPACE,
            plural=CRD_PLURAL,
            name=EDGEDEVICE_NAME
        )
        address = obj['spec']['address']
        logger.info(f"Device HTTP address: {address}")
        return address
    except Exception as e:
        logger.error(f"Error fetching EdgeDevice address: {e}")
        update_edge_device_phase("Unknown")
        sys.exit(1)

def load_instruction_config():
    path = os.path.join(INSTRUCTION_PATH, "instructions.yaml")
    try:
        with open(path, 'r') as f:
            cfg = yaml.safe_load(f)
        logger.info("Loaded instruction config successfully.")
        return cfg
    except Exception as e:
        logger.warning(f"Failed to load instruction config: {e}")
        return {}

instruction_cfg = load_instruction_config()
device_http_addr = get_device_address()

# MQTT <-> Device HTTP protocol mapping
def device_http_post(endpoint, data):
    url = f"{device_http_addr.rstrip('/')}/{endpoint.lstrip('/')}"
    try:
        response = requests.post(url, json=data, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        return response.json(), None
    except Exception as e:
        logger.warning(f"HTTP POST {url} failed: {e}")
        return None, str(e)

def device_http_get(endpoint):
    url = f"{device_http_addr.rstrip('/')}/{endpoint.lstrip('/')}"
    try:
        response = requests.get(url, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        return response.json(), None
    except Exception as e:
        logger.warning(f"HTTP GET {url} failed: {e}")
        return None, str(e)

# MQTT setup
mqtt_client = mqtt.Client(client_id=MQTT_CLIENT_ID, clean_session=True)
if MQTT_USERNAME:
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

mqtt_connected = threading.Event()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("MQTT connected successfully.")
        mqtt_connected.set()
        update_edge_device_phase("Running")
        # Subscribe to command topics
        client.subscribe([(TOPIC_COMMAND_ESTOP, 1), (TOPIC_COMMAND_STATE, 1), (TOPIC_COMMAND_ACTION, 1)])
    else:
        logger.error(f"MQTT connection failed: {rc}")
        update_edge_device_phase("Failed")

def on_disconnect(client, userdata, rc):
    logger.warning("MQTT disconnected.")
    update_edge_device_phase("Pending")
    mqtt_connected.clear()

def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload.decode()
    logger.info(f"Received MQTT message: topic={topic}, payload={payload}")
    try:
        data = json.loads(payload)
    except Exception as e:
        logger.warning(f"Invalid JSON payload: {e}")
        return

    if topic == TOPIC_COMMAND_ESTOP:
        handle_command_estop(client, data)
    elif topic == TOPIC_COMMAND_STATE:
        handle_command_state(client, data)
    elif topic == TOPIC_COMMAND_ACTION:
        handle_command_action(client, data)

def handle_command_estop(client, data):
    endpoint = instruction_cfg.get('api1', {}).get('protocolPropertyList', {}).get('endpoint', 'estop')
    resp, err = device_http_post(endpoint, data)
    publish_result(client, TOPIC_COMMAND_ESTOP + "/result", resp, err)

def handle_command_state(client, data):
    endpoint = instruction_cfg.get('api2', {}).get('protocolPropertyList', {}).get('endpoint', 'state')
    resp, err = device_http_post(endpoint, data)
    publish_result(client, TOPIC_COMMAND_STATE + "/result", resp, err)

def handle_command_action(client, data):
    endpoint = instruction_cfg.get('api3', {}).get('protocolPropertyList', {}).get('endpoint', 'action')
    resp, err = device_http_post(endpoint, data)
    publish_result(client, TOPIC_COMMAND_ACTION + "/result", resp, err)

def publish_result(client, topic, resp, err):
    result = {
        "success": err is None,
        "response": resp if err is None else None,
        "error": err
    }
    client.publish(topic, json.dumps(result), qos=1)

def telemetry_publisher():
    """Periodically publish telemetry data and status."""
    while True:
        # Telemetry data
        endpoint_data = instruction_cfg.get('api4', {}).get('protocolPropertyList', {}).get('endpoint', 'telemetry/data')
        data, err = device_http_get(endpoint_data)
        payload = json.dumps(data) if data else json.dumps({"error": err})
        mqtt_client.publish(TOPIC_TELEMETRY_DATA, payload, qos=1)

        # Telemetry status
        endpoint_status = instruction_cfg.get('api5', {}).get('protocolPropertyList', {}).get('endpoint', 'telemetry/status')
        status, err = device_http_get(endpoint_status)
        payload = json.dumps(status) if status else json.dumps({"error": err})
        mqtt_client.publish(TOPIC_TELEMETRY_STATUS, payload, qos=1)

        time.sleep(TELEMETRY_PUB_INTERVAL)

def main():
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message

    update_edge_device_phase("Pending")
    while True:
        try:
            mqtt_client.connect(MQTT_BROKER, MQTT_PORT, MQTT_KEEPALIVE)
            break
        except Exception as e:
            logger.warning(f"MQTT connect failed: {e}")
            update_edge_device_phase("Failed")
            time.sleep(3)

    # Start MQTT loop
    mqtt_client.loop_start()

    # Wait for MQTT connection
    if not mqtt_connected.wait(timeout=15):
        logger.error("MQTT connection timeout.")
        update_edge_device_phase("Failed")
        sys.exit(1)

    # Start telemetry publishing thread
    telemetry_thread = threading.Thread(target=telemetry_publisher, daemon=True)
    telemetry_thread.start()

    try:
        while True:
            time.sleep(3600)
    except KeyboardInterrupt:
        pass
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        update_edge_device_phase("Pending")

if __name__ == '__main__':
    main()