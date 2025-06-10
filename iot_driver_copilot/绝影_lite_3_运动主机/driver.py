import os
import yaml
import socket
import struct
import json
import threading
from typing import Dict, Any, Optional, List
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from kubernetes import client, config
from kubernetes.client.rest import ApiException
from starlette.middleware.cors import CORSMiddleware
from starlette.status import HTTP_503_SERVICE_UNAVAILABLE

# --- CONFIG & ENV ---

EDGEDEVICE_NAME = os.environ['EDGEDEVICE_NAME']
EDGEDEVICE_NAMESPACE = os.environ['EDGEDEVICE_NAMESPACE']
UDP_DEVICE_IP = os.environ['DEVICE_UDP_IP']
UDP_DEVICE_PORT = int(os.environ['DEVICE_UDP_PORT'])
HTTP_SERVER_HOST = os.environ.get('HTTP_SERVER_HOST', '0.0.0.0')
HTTP_SERVER_PORT = int(os.environ.get('HTTP_SERVER_PORT', '8080'))

CONFIGMAP_CONFIG_DIR = '/etc/edgedevice/config/instructions'
INSTRUCTION_CONFIG_FILE = os.path.join(CONFIGMAP_CONFIG_DIR, 'instructions.yaml')

EDGEDEVICE_CRD_GROUP = "shifu.edgenesis.io"
EDGEDEVICE_CRD_VERSION = "v1alpha1"
EDGEDEVICE_CRD_PLURAL = "edgedevices"

# --- LOAD INSTRUCTION CONFIGMAP ---

def load_instruction_config(path: str) -> Dict[str, Any]:
    try:
        with open(path, 'r') as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {}

instruction_config = load_instruction_config(INSTRUCTION_CONFIG_FILE)

# --- KUBERNETES CLIENT SETUP ---

def k8s_client():
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()
    return client.CustomObjectsApi()

k8s_api = k8s_client()

def get_edgedevice():
    return k8s_api.get_namespaced_custom_object(
        group=EDGEDEVICE_CRD_GROUP,
        version=EDGEDEVICE_CRD_VERSION,
        namespace=EDGEDEVICE_NAMESPACE,
        plural=EDGEDEVICE_CRD_PLURAL,
        name=EDGEDEVICE_NAME
    )

def update_edgedevice_status(phase: str):
    body = {
        "status": {
            "edgeDevicePhase": phase
        }
    }
    try:
        k8s_api.patch_namespaced_custom_object_status(
            group=EDGEDEVICE_CRD_GROUP,
            version=EDGEDEVICE_CRD_VERSION,
            namespace=EDGEDEVICE_NAMESPACE,
            plural=EDGEDEVICE_CRD_PLURAL,
            name=EDGEDEVICE_NAME,
            body=body
        )
    except ApiException:
        pass

# --- UDP COMMUNICATION WRAPPER ---

class UDPDeviceClient:
    def __init__(self, device_ip: str, device_port: int):
        self.device_ip = device_ip
        self.device_port = device_port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(2.0)
        self.lock = threading.Lock()
        self.last_status = 'Unknown'

    def _send_and_receive(self, payload: bytes) -> Optional[bytes]:
        with self.lock:
            try:
                self.sock.sendto(payload, (self.device_ip, self.device_port))
                data, _ = self.sock.recvfrom(4096)
                self.last_status = 'Running'
                return data
            except socket.timeout:
                self.last_status = 'Pending'
                return None
            except Exception:
                self.last_status = 'Failed'
                return None

    def check_connection(self) -> str:
        # Try a dummy "heartbeat" command for status check
        payload = self.encode_command("heartbeat", {})
        resp = self._send_and_receive(payload)
        if resp:
            self.last_status = 'Running'
            return 'Running'
        elif self.last_status == 'Failed':
            return 'Failed'
        elif self.last_status == 'Pending':
            return 'Pending'
        else:
            return 'Unknown'

    def get_state(self, fields: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
        payload = self.encode_command("get_state", {"fields": fields} if fields else {})
        resp = self._send_and_receive(payload)
        if resp:
            return self.decode_state(resp, fields)
        else:
            return None

    def send_command(self, command: str, params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        payload = self.encode_command(command, params)
        resp = self._send_and_receive(payload)
        if resp:
            return self.decode_command_response(resp)
        else:
            return None

    # --- Device-specific protocol encoding/decoding (simplified for example) ---

    def encode_command(self, cmd: str, params: Dict[str, Any]) -> bytes:
        # Example: {"cmd": "stand", "params": {"speed": 1.2}} as JSON then bytes.
        message = {"cmd": cmd, "params": params}
        return json.dumps(message).encode('utf-8')

    def decode_state(self, data: bytes, fields: Optional[List[str]]) -> Dict[str, Any]:
        # Example: response as JSON over UDP, filter fields if specified
        try:
            state = json.loads(data.decode('utf-8'))
            if fields:
                state = {k: v for k, v in state.items() if k in fields}
            return state
        except Exception:
            return {"error": "Failed to decode device state"}

    def decode_command_response(self, data: bytes) -> Dict[str, Any]:
        try:
            return json.loads(data.decode('utf-8'))
        except Exception:
            return {"status": "error", "detail": "Failed to decode response"}

# --- K8S PHASE MAINTAINER THREAD ---

def phase_maintainer(client: UDPDeviceClient):
    prev_phase = None
    while True:
        phase = client.check_connection()
        if phase != prev_phase:
            update_edgedevice_status(phase)
            prev_phase = phase
        threading.Event().wait(5)

# --- FASTAPI APP ---

app = FastAPI(title="Deep Robotics Lite3 Robot Control DeviceShifu")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

udp_client = UDPDeviceClient(UDP_DEVICE_IP, UDP_DEVICE_PORT)

# --- PHASE MAINTAINER THREAD ---
threading.Thread(target=phase_maintainer, args=(udp_client,), daemon=True).start()

# --- API: GET /state ---

@app.get("/state")
def get_state(request: Request):
    fields_q = request.query_params.get("fields")
    fields = [f.strip() for f in fields_q.split(",")] if fields_q else None

    # Get settings from instruction_config if present
    protocol_settings = instruction_config.get("api1", {}).get("protocolPropertyList", {}) if "api1" in instruction_config else {}

    state = udp_client.get_state(fields)
    if state is None:
        raise HTTPException(status_code=HTTP_503_SERVICE_UNAVAILABLE, detail="Failed to retrieve state from device")
    return JSONResponse(content=state)

# --- API: POST /cmd ---

@app.post("/cmd")
async def post_cmd(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")
    command = body.get("command")
    params = body.get("params", {})

    if not command:
        raise HTTPException(status_code=400, detail="Missing 'command' in request body")

    # Get settings from instruction_config if present
    protocol_settings = instruction_config.get("api2", {}).get("protocolPropertyList", {}) if "api2" in instruction_config else {}

    resp = udp_client.send_command(command, params)
    if resp is None:
        raise HTTPException(status_code=HTTP_503_SERVICE_UNAVAILABLE, detail="Failed to send command to device")
    return JSONResponse(content=resp)

# --- HEALTH CHECK ---

@app.get("/healthz")
def healthz():
    return {"status": "ok"}

# --- MAIN (if standalone, for local testing) ---

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=HTTP_SERVER_HOST, port=HTTP_SERVER_PORT)