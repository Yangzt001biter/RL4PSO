import os
import yaml
import socket
import struct
import json
import threading
import time

from fastapi import FastAPI, Request, HTTPException, Response, status
from fastapi.responses import StreamingResponse, JSONResponse
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from pydantic import BaseModel

# --- Config and Environment ---

EDGEDEVICE_NAME = os.environ["EDGEDEVICE_NAME"]
EDGEDEVICE_NAMESPACE = os.environ["EDGEDEVICE_NAMESPACE"]

CONFIG_PATH = "/etc/edgedevice/config/instructions"
SERVER_HOST = os.environ.get("SERVER_HOST", "0.0.0.0")
SERVER_PORT = int(os.environ.get("SERVER_PORT", "8080"))

# --- Kubernetes CRD Setup ---

CRD_GROUP = "shifu.edgenesis.io"
CRD_VERSION = "v1alpha1"
CRD_PLURAL = "edgedevices"

def load_k8s_config():
    config.load_incluster_config()
    api = client.CustomObjectsApi()
    return api

def get_edgedevice_object(api):
    try:
        obj = api.get_namespaced_custom_object(
            CRD_GROUP, CRD_VERSION, EDGEDEVICE_NAMESPACE, CRD_PLURAL, EDGEDEVICE_NAME
        )
        return obj
    except ApiException as e:
        return None

def update_edgedevice_phase(api, phase):
    for _ in range(3):
        try:
            body = {"status": {"edgeDevicePhase": phase}}
            api.patch_namespaced_custom_object_status(
                CRD_GROUP, CRD_VERSION, EDGEDEVICE_NAMESPACE, CRD_PLURAL, EDGEDEVICE_NAME, body
            )
            return True
        except ApiException:
            time.sleep(1)
    return False

def get_edgedevice_address(api):
    obj = get_edgedevice_object(api)
    return obj["spec"]["address"] if obj and "address" in obj.get("spec", {}) else None

# --- ConfigMap Parsing ---

def load_instruction_config():
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

# --- UDP Protocol Layer ---

class RobotUDPClient:
    """Handles UDP communication with the robot."""

    def __init__(self, address, port, timeout=1.0):
        self.address = address
        self.port = port
        self.timeout = timeout
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(self.timeout)
        self.lock = threading.Lock()

    def send(self, payload: bytes):
        with self.lock:
            self.sock.sendto(payload, (self.address, self.port))

    def request_response(self, payload: bytes, response_size=4096):
        with self.lock:
            self.sock.sendto(payload, (self.address, self.port))
            try:
                data, _ = self.sock.recvfrom(response_size)
                return data
            except socket.timeout:
                return None

    def close(self):
        self.sock.close()

# --- API Models ---

class HeartbeatRequest(BaseModel):
    timestamp: float
    metadata: dict = None

class ActionRequest(BaseModel):
    action: str
    speed: float = None
    angle: float = None
    duration: float = None
    extra: dict = None

class ModeRequest(BaseModel):
    mode: str
    params: dict = None

class EStopRequest(BaseModel):
    reason: str = None
    extra: dict = None

# --- Device Protocol Mapping ---
# (These should be updated to match real binary protocol definitions as needed)

def encode_heartbeat(payload: dict, proto_cfg: dict):
    # Example: Command ID 0x01, 8-byte timestamp, no payload
    ts = payload.get("timestamp", time.time())
    return struct.pack("<B d", 0x01, ts)

def encode_estop(payload: dict, proto_cfg: dict):
    # Example: Command ID 0x02, 1 byte reason code
    return struct.pack("<B", 0x02)

def encode_action(payload: dict, proto_cfg: dict):
    # Example: Command ID 0x03, 1 byte action, 3 floats (speed, angle, duration)
    action_map = proto_cfg.get("action_map", {})
    action_id = action_map.get(payload.get("action", ""), 0)
    speed = float(payload.get("speed", 0.0))
    angle = float(payload.get("angle", 0.0))
    duration = float(payload.get("duration", 0.0))
    return struct.pack("<B B f f f", 0x03, action_id, speed, angle, duration)

def encode_mode(payload: dict, proto_cfg: dict):
    # Example: Command ID 0x04, 1 byte mode
    mode_map = proto_cfg.get("mode_map", {})
    mode_id = mode_map.get(payload.get("mode", ""), 0)
    return struct.pack("<B B", 0x04, mode_id)

def encode_state_request(proto_cfg: dict):
    # Example: Command ID 0x05
    return struct.pack("<B", 0x05)

def decode_state_response(data: bytes, proto_cfg: dict):
    # Example: decode a simple state struct (battery, joint angles, velocities)
    # This should be adapted to the actual robot binary protocol
    d = {}
    try:
        # Example: battery (float), 12 joint angles (12 floats), 12 joint velocities (12 floats)
        fmt = "<f 12f 12f"
        unpacked = struct.unpack(fmt, data[:(1+12+12)*4])
        d["battery_level"] = unpacked[0]
        d["joint_angles"] = list(unpacked[1:13])
        d["joint_velocities"] = list(unpacked[13:25])
        # Add more fields as per the protocol
    except Exception:
        d = {"error": "Failed to decode"}
    return d

# --- FastAPI Setup ---

app = FastAPI()
k8s_api = load_k8s_config()
instruction_cfg = load_instruction_config()
edgedevice_phase = "Unknown"

# --- UDP Client (lazy init) ---
udp_client = None
udp_client_lock = threading.Lock()

def ensure_udp_client():
    global udp_client
    if udp_client is None:
        addr = get_edgedevice_address(k8s_api)
        if not addr:
            raise RuntimeError("Device address not found in EdgeDevice CR spec")
        port = int(os.environ.get("DEVICE_UDP_PORT", "8808"))
        udp_client = RobotUDPClient(addr, port)
    return udp_client

# --- Device Connection Monitor ---

def connection_monitor():
    global edgedevice_phase
    while True:
        phase = "Unknown"
        try:
            cli = ensure_udp_client()
            payload = encode_heartbeat({"timestamp": time.time()}, instruction_cfg.get("cmd/heartbeat", {}).get("protocolPropertyList", {}))
            resp = cli.request_response(payload)
            if resp:
                phase = "Running"
            else:
                phase = "Failed"
        except Exception:
            phase = "Pending"
        if phase != edgedevice_phase:
            update_edgedevice_phase(k8s_api, phase)
            edgedevice_phase = phase
        time.sleep(5)

threading.Thread(target=connection_monitor, daemon=True).start()

# --- API Endpoints ---

@app.post("/cmd/heartbeat")
async def cmd_heartbeat(req: Request):
    try:
        payload = await req.json()
        proto_cfg = instruction_cfg.get("cmd/heartbeat", {}).get("protocolPropertyList", {})
        cli = ensure_udp_client()
        udp_payload = encode_heartbeat(payload, proto_cfg)
        resp = cli.request_response(udp_payload)
        if resp:
            return {"result": "ok"}
        else:
            raise HTTPException(status_code=504, detail="No response from device")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/cmd/estop")
async def cmd_estop(req: Request):
    try:
        payload = await req.json()
        proto_cfg = instruction_cfg.get("cmd/estop", {}).get("protocolPropertyList", {})
        cli = ensure_udp_client()
        udp_payload = encode_estop(payload, proto_cfg)
        resp = cli.request_response(udp_payload)
        if resp:
            return {"result": "ok"}
        else:
            raise HTTPException(status_code=504, detail="No response from device")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/cmd/action")
async def cmd_action(req: Request):
    try:
        payload = await req.json()
        proto_cfg = instruction_cfg.get("cmd/action", {}).get("protocolPropertyList", {})
        cli = ensure_udp_client()
        udp_payload = encode_action(payload, proto_cfg)
        resp = cli.request_response(udp_payload)
        if resp:
            return {"result": "ok"}
        else:
            raise HTTPException(status_code=504, detail="No response from device")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/cmd/mode")
async def cmd_mode(req: Request):
    try:
        payload = await req.json()
        proto_cfg = instruction_cfg.get("cmd/mode", {}).get("protocolPropertyList", {})
        cli = ensure_udp_client()
        udp_payload = encode_mode(payload, proto_cfg)
        resp = cli.request_response(udp_payload)
        if resp:
            return {"result": "ok"}
        else:
            raise HTTPException(status_code=504, detail="No response from device")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/state")
async def get_state():
    try:
        proto_cfg = instruction_cfg.get("state", {}).get("protocolPropertyList", {})
        cli = ensure_udp_client()
        udp_payload = encode_state_request(proto_cfg)
        resp = cli.request_response(udp_payload, response_size=2048)
        if resp:
            data = decode_state_response(resp, proto_cfg)
            return JSONResponse(content=data)
        else:
            raise HTTPException(status_code=504, detail="No response from device")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- Main ---

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host=SERVER_HOST, port=SERVER_PORT)