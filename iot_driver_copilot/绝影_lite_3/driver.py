import os
import yaml
import json
import asyncio
import logging
from typing import Dict, Any
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
import paho.mqtt.client as mqtt

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variable configuration
EDGEDEVICE_NAME = os.environ["EDGEDEVICE_NAME"]
EDGEDEVICE_NAMESPACE = os.environ["EDGEDEVICE_NAMESPACE"]
MQTT_BROKER_ADDRESS = os.environ["MQTT_BROKER_ADDRESS"]

HTTP_SERVER_HOST = os.environ.get("HTTP_SERVER_HOST", "0.0.0.0")
HTTP_SERVER_PORT = int(os.environ.get("HTTP_SERVER_PORT", "8080"))

INSTRUCTION_CONFIG_PATH = "/etc/edgedevice/config/instructions"

# MQTT Topics (from API info)
MQTT_TOPICS = {
    "device/commands/motion": {"method": "PUBLISH", "qos": 1},
    "device/commands/heartbeat": {"method": "PUBLISH", "qos": 1},
    "device/commands/state": {"method": "PUBLISH", "qos": 1},
    "device/sensors/state": {"method": "SUBSCRIBE", "qos": 1},
    "device/sensors/joint": {"method": "SUBSCRIBE", "qos": 1},
}

# Phase strings
PHASE_PENDING = "Pending"
PHASE_RUNNING = "Running"
PHASE_FAILED = "Failed"
PHASE_UNKNOWN = "Unknown"

# FastAPI initialization
app = FastAPI(
    title="DeviceShifu MQTT-HTTP Driver for DEEP Robotics Quadruped",
    version="1.0.0"
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

# MQTT State
class MqttState:
    client: mqtt.Client = None
    connected: bool = False
    last_sensor_state: Dict = {}
    last_joint_state: Dict = {}
    loop: asyncio.AbstractEventLoop = None

mqtt_state = MqttState()

# Kubernetes CRD update helpers
def get_k8s_clients():
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    api = client.CustomObjectsApi()
    core = client.CoreV1Api()
    return api, core

def update_edgedevice_phase(phase: str):
    api, _ = get_k8s_clients()
    body = {"status": {"edgeDevicePhase": phase}}
    try:
        api.patch_namespaced_custom_object_status(
            group="shifu.edgenesis.io",
            version="v1alpha1",
            namespace=EDGEDEVICE_NAMESPACE,
            plural="edgedevices",
            name=EDGEDEVICE_NAME,
            body=body,
        )
        logger.info(f"EdgeDevice phase set to {phase}")
    except ApiException as e:
        logger.error(f"Failed to update EdgeDevice phase: {e}")

def get_edgedevice_spec():
    api, _ = get_k8s_clients()
    try:
        obj = api.get_namespaced_custom_object(
            group="shifu.edgenesis.io",
            version="v1alpha1",
            namespace=EDGEDEVICE_NAMESPACE,
            plural="edgedevices",
            name=EDGEDEVICE_NAME,
        )
        return obj.get("spec", {})
    except ApiException as e:
        logger.error(f"Failed to get EdgeDevice spec: {e}")
        return {}

def get_device_address():
    spec = get_edgedevice_spec()
    return spec.get("address", "")

# Instruction config loading
def load_instruction_config() -> Dict[str, Any]:
    try:
        with open(INSTRUCTION_CONFIG_PATH, "r") as f:
            return yaml.safe_load(f)
    except Exception as e:
        logger.warning(f"Failed to load instruction config: {e}")
        return {}

instruction_config = load_instruction_config()

# MQTT Callback Handlers
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        mqtt_state.connected = True
        update_edgedevice_phase(PHASE_RUNNING)
        logger.info("MQTT connected")
        client.subscribe([("device/sensors/state", 1), ("device/sensors/joint", 1)])
    else:
        mqtt_state.connected = False
        update_edgedevice_phase(PHASE_FAILED)
        logger.error(f"MQTT connection failed: {rc}")

def on_disconnect(client, userdata, rc):
    mqtt_state.connected = False
    update_edgedevice_phase(PHASE_PENDING if rc != 0 else PHASE_UNKNOWN)
    logger.warning(f"MQTT disconnected (rc={rc})")

def on_message(client, userdata, msg):
    topic = msg.topic
    payload = msg.payload
    try:
        content = payload.decode('utf-8')
        json_content = json.loads(content)
    except Exception:
        json_content = content
    if topic == "device/sensors/state":
        mqtt_state.last_sensor_state = json_content
    elif topic == "device/sensors/joint":
        mqtt_state.last_joint_state = json_content

def mqtt_connect_loop():
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message
    broker_host, broker_port = MQTT_BROKER_ADDRESS.split(":")
    broker_port = int(broker_port)
    try:
        mqtt_client.connect(broker_host, broker_port, keepalive=60)
    except Exception as e:
        logger.error(f"MQTT initial connection failed: {e}")
        update_edgedevice_phase(PHASE_FAILED)
        return None
    mqtt_client.loop_start()
    return mqtt_client

# MQTT publishing helper
def mqtt_publish(topic, payload, qos=1):
    if not mqtt_state.connected:
        raise HTTPException(status_code=503, detail="MQTT not connected")
    mqtt_state.client.publish(topic, json.dumps(payload), qos=qos)

# API Models
class MotionCommand(BaseModel):
    # Accepts arbitrary payload for the motion topic
    __root__: dict

class HeartbeatCommand(BaseModel):
    __root__: dict

class StateCommand(BaseModel):
    __root__: dict

# REST API Endpoints

@app.post("/device/commands/motion")
async def publish_motion_command(cmd: MotionCommand):
    settings = instruction_config.get("api1", {}).get("protocolPropertyList", {})
    qos = int(settings.get("qos", 1))
    # Use settingsA/settingsB if needed from config, here ignored for generality
    mqtt_publish("device/commands/motion", cmd.__root__, qos=qos)
    return {"status": "published", "topic": "device/commands/motion"}

@app.post("/device/commands/state")
async def publish_state_command(cmd: StateCommand):
    settings = instruction_config.get("api5", {}).get("protocolPropertyList", {})
    qos = int(settings.get("qos", 1))
    mqtt_publish("device/commands/state", cmd.__root__, qos=qos)
    return {"status": "published", "topic": "device/commands/state"}

@app.post("/device/commands/heartbeat")
async def publish_heartbeat(cmd: HeartbeatCommand):
    settings = instruction_config.get("api4", {}).get("protocolPropertyList", {})
    qos = int(settings.get("qos", 1))
    mqtt_publish("device/commands/heartbeat", cmd.__root__, qos=qos)
    return {"status": "published", "topic": "device/commands/heartbeat"}

@app.get("/device/sensors/state")
async def get_sensor_state():
    # Returns the last received sensor state
    if not mqtt_state.last_sensor_state:
        raise HTTPException(status_code=404, detail="No sensor state available")
    return mqtt_state.last_sensor_state

@app.get("/device/sensors/joint")
async def get_joint_state():
    if not mqtt_state.last_joint_state:
        raise HTTPException(status_code=404, detail="No joint state available")
    return mqtt_state.last_joint_state

@app.get("/healthz")
async def healthz():
    return {"mqtt_connected": mqtt_state.connected}

@app.get("/readiness")
async def readiness():
    return {"mqtt_connected": mqtt_state.connected}

@app.on_event("startup")
async def startup_event():
    update_edgedevice_phase(PHASE_PENDING)
    mqtt_state.loop = asyncio.get_event_loop()
    # Connect to MQTT in a thread
    def mqtt_thread():
        mqtt_state.client = mqtt_connect_loop()
    import threading
    threading.Thread(target=mqtt_thread, daemon=True).start()
    # Wait for connection or timeout
    await asyncio.sleep(2)
    if not mqtt_state.connected:
        update_edgedevice_phase(PHASE_FAILED)

@app.on_event("shutdown")
async def shutdown_event():
    if mqtt_state.client:
        mqtt_state.client.loop_stop()
        mqtt_state.client.disconnect()
    update_edgedevice_phase(PHASE_UNKNOWN)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HTTP_SERVER_HOST, port=HTTP_SERVER_PORT)