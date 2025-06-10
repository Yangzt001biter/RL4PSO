import os
import sys
import asyncio
import yaml
import json
import struct
import socket
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Request, Response, HTTPException, status
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

# DeviceShifu HTTP server for Deep Robotics control unit

# ---------------------------
# Environment & Configuration
# ---------------------------

EDGEDEVICE_NAME = os.environ.get('EDGEDEVICE_NAME')
EDGEDEVICE_NAMESPACE = os.environ.get('EDGEDEVICE_NAMESPACE')
DEVICE_HTTP_HOST = os.environ.get('DEVICE_HTTP_HOST', '0.0.0.0')
DEVICE_HTTP_PORT = int(os.environ.get('DEVICE_HTTP_PORT', '8080'))
DEVICE_UDP_IP = os.environ.get('DEVICE_UDP_IP')
DEVICE_UDP_PORT = int(os.environ.get('DEVICE_UDP_PORT', '8899'))
K8S_IN_CLUSTER = os.environ.get('KUBERNETES_SERVICE_HOST') is not None

CONFIGMAP_DIR = '/etc/edgedevice/config/instructions'

if not EDGEDEVICE_NAME or not EDGEDEVICE_NAMESPACE:
    print("EDGEDEVICE_NAME and EDGEDEVICE_NAMESPACE env vars must be set", file=sys.stderr)
    sys.exit(1)
if not DEVICE_UDP_IP or not DEVICE_UDP_PORT:
    print("DEVICE_UDP_IP and DEVICE_UDP_PORT env vars must be set", file=sys.stderr)
    sys.exit(1)

# ---------------------------
# ConfigMap Loader
# ---------------------------

def load_api_config() -> Dict[str, Any]:
    config_path = os.path.join(CONFIGMAP_DIR, 'instruction.yaml')
    if not os.path.exists(config_path):
        return {}
    with open(config_path, 'r') as f:
        return yaml.safe_load(f) or {}

api_protocol_settings = load_api_config()

# ---------------------------
# Kubernetes CRD Integration
# ---------------------------

class EdgeDeviceStatus:
    def __init__(self):
        if K8S_IN_CLUSTER:
            config.load_incluster_config()
        else:
            config.load_kube_config()
        self.api = client.CustomObjectsApi()

    def update_phase(self, phase: str):
        body = {"status": {"edgeDevicePhase": phase}}
        try:
            self.api.patch_namespaced_custom_object_status(
                group="shifu.edgenesis.io",
                version="v1alpha1",
                namespace=EDGEDEVICE_NAMESPACE,
                plural="edgedevices",
                name=EDGEDEVICE_NAME,
                body=body
            )
        except ApiException as e:
            print(f"Failed to update EdgeDevice status: {e}", file=sys.stderr)

    def get_device_address(self) -> Optional[str]:
        try:
            obj = self.api.get_namespaced_custom_object(
                group="shifu.edgenesis.io",
                version="v1alpha1",
                namespace=EDGEDEVICE_NAMESPACE,
                plural="edgedevices",
                name=EDGEDEVICE_NAME
            )
            return obj.get('spec', {}).get('address')
        except ApiException as e:
            print(f"Failed to get EdgeDevice CRD: {e}", file=sys.stderr)
            return None

edge_dev_status = EdgeDeviceStatus()

# ---------------------------
# UDP Protocol Definitions
# ---------------------------

class UDPClient:
    def __init__(self, ip: str, port: int, recv_timeout: float = 1.0):
        self.ip = ip
        self.port = port
        self.recv_timeout = recv_timeout
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(recv_timeout)

    def send(self, data: bytes) -> None:
        self.sock.sendto(data, (self.ip, self.port))

    def receive(self, bufsize=4096) -> Optional[bytes]:
        try:
            data, _ = self.sock.recvfrom(bufsize)
            return data
        except socket.timeout:
            return None

    def close(self):
        self.sock.close()

# ---------------------------
# Device Protocol
# ---------------------------

# Placeholder protocol mapping (should be replaced with actual protocol definition)
# For demonstration, use simple message patterns

def build_heartbeat_packet() -> bytes:
    # Device-specific: replace with actual heartbeat packet structure
    # Example: header + command code = 0x01 for heartbeat
    return struct.pack('<BB', 0xAA, 0x01)

def build_state_packet() -> bytes:
    # Device-specific: replace with actual packet structure
    return struct.pack('<BB', 0xAA, 0x02)

def build_command_packet(cmd_name: str, params: Dict[str, Any]) -> bytes:
    # Device-specific: must be replaced with actual protocol
    # For example, map known commands to command codes and parameters
    cmd_map = {
        'heartbeat': 0x01,
        'stand': 0x03,
        'squat': 0x04,
        'soft_stop': 0x05,
        'zeroing': 0x06,
        # ... add more mappings as per device spec
    }
    code = cmd_map.get(cmd_name, 0x00)
    # For demonstration, pack code and one integer param if present
    param_val = params.get('value', 0)
    return struct.pack('<BBi', 0xAA, code, param_val)

def parse_state_response(data: bytes) -> Dict[str, Any]:
    # Device-specific: parse binary struct into JSON
    # Example: suppose state packet is AA 02 followed by 4 floats (battery, rpy[3])
    if len(data) < 18:
        return {"error": "Incomplete state packet"}
    hdr, cmd = struct.unpack('<BB', data[:2])
    if hdr != 0xAA or cmd != 0x02:
        return {"error": "Invalid state response"}
    # Example: battery(float32), rpy(float32*3)
    battery, rpy1, rpy2, rpy3 = struct.unpack('<ffff', data[2:18])
    return {
        "battery": battery,
        "rpy": [rpy1, rpy2, rpy3],
        # ... add more fields as per device spec
    }

def parse_heartbeat_response(data: bytes) -> Dict[str, Any]:
    # Device-specific: parse binary heartbeat response
    if len(data) < 3:
        return {"error": "Incomplete heartbeat"}
    hdr, cmd, status = struct.unpack('<BBB', data[:3])
    return {"heartbeat": bool(status)}

# ---------------------------
# FastAPI App
# ---------------------------

app = FastAPI()

# Maintain device status
device_phase = "Pending"

async def check_device_connectivity():
    global device_phase
    udp = UDPClient(DEVICE_UDP_IP, DEVICE_UDP_PORT)
    try:
        udp.send(build_heartbeat_packet())
        resp = udp.receive()
        if resp is None:
            device_phase = "Failed"
        else:
            device_phase = "Running"
    except Exception:
        device_phase = "Failed"
    finally:
        udp.close()
    edge_dev_status.update_phase(device_phase)

@app.on_event("startup")
async def startup_event():
    # On startup, check device connectivity
    await check_device_connectivity()

@app.on_event("shutdown")
async def shutdown_event():
    edge_dev_status.update_phase("Unknown")

@app.middleware("http")
async def update_status_middleware(request: Request, call_next):
    # Update device phase before each request
    await check_device_connectivity()
    response = await call_next(request)
    return response

@app.get("/heartbeat")
async def get_heartbeat():
    udp = UDPClient(DEVICE_UDP_IP, DEVICE_UDP_PORT)
    try:
        udp.send(build_heartbeat_packet())
        data = udp.receive()
        if data:
            result = parse_heartbeat_response(data)
            return JSONResponse(content=result)
        else:
            raise HTTPException(status_code=504, detail="No heartbeat from device")
    finally:
        udp.close()

@app.get("/state")
async def get_state(fields: Optional[str] = None):
    udp = UDPClient(DEVICE_UDP_IP, DEVICE_UDP_PORT)
    try:
        udp.send(build_state_packet())
        data = udp.receive()
        if data:
            result = parse_state_response(data)
            if fields:
                field_list = [f.strip() for f in fields.split(',')]
                result = {k: v for k, v in result.items() if k in field_list}
            return JSONResponse(content=result)
        else:
            raise HTTPException(status_code=504, detail="No state response from device")
    finally:
        udp.close()

@app.get("/telemetry")
async def get_telemetry(fields: Optional[str] = None, page: Optional[int] = None, limit: Optional[int] = None):
    # For simplicity, telemetry = state
    udp = UDPClient(DEVICE_UDP_IP, DEVICE_UDP_PORT)
    try:
        udp.send(build_state_packet())
        data = udp.receive()
        if data:
            result = parse_state_response(data)
            if fields:
                field_list = [f.strip() for f in fields.split(',')]
                result = {k: v for k, v in result.items() if k in field_list}
            # Pagination is not supported in this simple example
            return JSONResponse(content=result)
        else:
            raise HTTPException(status_code=504, detail="No telemetry from device")
    finally:
        udp.close()

class CommandRequest(BaseModel):
    command: str
    parameters: Optional[Dict[str, Any]] = {}

@app.post("/cmd")
async def post_cmd(cmd: CommandRequest):
    udp = UDPClient(DEVICE_UDP_IP, DEVICE_UDP_PORT)
    try:
        udp.send(build_command_packet(cmd.command, cmd.parameters or {}))
        data = udp.receive()
        if data:
            return JSONResponse(content={"success": True, "response": list(data)})
        else:
            raise HTTPException(status_code=504, detail="No response from device")
    finally:
        udp.close()

@app.post("/commands")
async def post_commands(cmd: CommandRequest):
    udp = UDPClient(DEVICE_UDP_IP, DEVICE_UDP_PORT)
    try:
        udp.send(build_command_packet(cmd.command, cmd.parameters or {}))
        data = udp.receive()
        if data:
            return JSONResponse(content={"success": True, "response": list(data)})
        else:
            raise HTTPException(status_code=504, detail="No response from device")
    finally:
        udp.close()

# ---------------------------
# HTTP Run Entrypoint
# ---------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=DEVICE_HTTP_HOST, port=DEVICE_HTTP_PORT)