import os
import sys
import socket
import struct
import threading
import time
import yaml
import json
from typing import Any, Dict, Optional
from flask import Flask, request, Response, jsonify
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

# ========================== CONFIGURATION ==========================
EDGEDEVICE_NAME = os.environ["EDGEDEVICE_NAME"]
EDGEDEVICE_NAMESPACE = os.environ["EDGEDEVICE_NAMESPACE"]
K8S_IN_CLUSTER = os.environ.get("KUBERNETES_SERVICE_HOST") is not None

HTTP_HOST = os.environ.get("DEVICE_SHIFU_HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.environ.get("DEVICE_SHIFU_HTTP_PORT", "8080"))
UDP_DEVICE_IP = os.environ.get("DEVICE_SHIFU_DEVICE_IP")  # required
UDP_DEVICE_PORT = int(os.environ.get("DEVICE_SHIFU_DEVICE_UDP_PORT", "60000"))
UDP_LOCAL_BIND_PORT = int(os.environ.get("DEVICE_SHIFU_LOCAL_UDP_PORT", "55000"))

CONFIGMAP_INSTRUCTION_PATH = "/etc/edgedevice/config/instructions"

# ========================== K8S CLIENT UTILS ==========================
def k8s_load_config():
    if K8S_IN_CLUSTER:
        config.load_incluster_config()
    else:
        config.load_kube_config()

def get_edgedevice_api():
    k8s_load_config()
    api = client.CustomObjectsApi()
    return api

def get_edgedevice():
    api = get_edgedevice_api()
    return api.get_namespaced_custom_object(
        group="shifu.edgenesis.io",
        version="v1alpha1",
        namespace=EDGEDEVICE_NAMESPACE,
        plural="edgedevices",
        name=EDGEDEVICE_NAME
    )

def update_edgedevice_phase(phase: str):
    api = get_edgedevice_api()
    body = {
        "status": {
            "edgeDevicePhase": phase
        }
    }
    # Use patch to update status subresource
    try:
        api.patch_namespaced_custom_object_status(
            group="shifu.edgenesis.io",
            version="v1alpha1",
            namespace=EDGEDEVICE_NAMESPACE,
            plural="edgedevices",
            name=EDGEDEVICE_NAME,
            body=body
        )
    except ApiException as e:
        # Fallback: try patching the whole object (if status subresource not enabled)
        try:
            api.patch_namespaced_custom_object(
                group="shifu.edgenesis.io",
                version="v1alpha1",
                namespace=EDGEDEVICE_NAMESPACE,
                plural="edgedevices",
                name=EDGEDEVICE_NAME,
                body=body
            )
        except Exception:
            pass

# ========================== INSTRUCTION CONFIG ==========================
def load_instruction_config() -> Dict[str, Any]:
    config_path = os.path.join(CONFIGMAP_INSTRUCTION_PATH, "config.yaml")
    if not os.path.exists(config_path):
        return {}
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

INSTRUCTION_CONFIG = load_instruction_config()

# ========================== UDP CLIENT (DEVICE CONNECTION) ==========================
class RobotUDPClient:
    def __init__(self, ip: str, port: int, local_port: int):
        self.device_ip = ip
        self.device_port = port
        self.local_port = local_port
        self.sock = None
        self.lock = threading.Lock()
        self.alive = False
        self.last_recv_time = 0
        self.last_state = None
        self._start_udp_thread()

    def _start_udp_thread(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.settimeout(2.0)
        self.sock.bind(("", self.local_port))
        self.alive = True
        self.last_recv_time = 0
        self.last_state = None
        t = threading.Thread(target=self._udp_listener, daemon=True)
        t.start()

    def _udp_listener(self):
        while self.alive:
            try:
                data, _ = self.sock.recvfrom(4096)
                self.lock.acquire()
                self.last_recv_time = time.time()
                self.last_state = data
                self.lock.release()
            except socket.timeout:
                continue
            except Exception:
                continue

    def close(self):
        self.alive = False
        try:
            self.sock.close()
        except Exception:
            pass

    def send(self, payload: bytes):
        self.sock.sendto(payload, (self.device_ip, self.device_port))

    def get_last_state(self) -> Optional[bytes]:
        self.lock.acquire()
        data = self.last_state
        self.lock.release()
        return data

    def get_last_time(self) -> float:
        self.lock.acquire()
        t = self.last_recv_time
        self.lock.release()
        return t

    def is_connected(self) -> bool:
        # Considered 'connected' if we received something in the last 5 seconds
        return (time.time() - self.get_last_time()) < 5

# ========================== DEVICE PROTOCOL ENCODING ==========================
def encode_heartbeat(payload: Dict[str, Any]) -> bytes:
    # Example: Heartbeat packet, protocol is not specified, using a simple struct [cmd_id|timestamp]
    # Let's assign: cmd_id=0x01, timestamp=uint32_t (epoch seconds)
    cmd_id = 0x01
    timestamp = int(payload.get("timestamp", int(time.time())))
    return struct.pack("<BI", cmd_id, timestamp)

def encode_estop(payload: Dict[str, Any]) -> bytes:
    # Example: Soft emergency stop, cmd_id=0x02, optional reason str (max 32 bytes, utf-8, null-terminated)
    cmd_id = 0x02
    reason = payload.get("reason", "")
    reason_bytes = reason.encode("utf-8")[:31]
    reason_bytes = reason_bytes + b"\x00" * (32 - len(reason_bytes))
    return struct.pack("<B32s", cmd_id, reason_bytes)

def encode_action(payload: Dict[str, Any]) -> bytes:
    # Example: Action trigger, cmd_id=0x03, action_type:uint8, speed:float, angle:float, duration:uint16
    # Action type mapping
    actions = {
        "Twist": 1, "Roll": 2, "Moonwalk": 3, "Backflip": 4, "Wave": 5, "Jump": 6
    }
    cmd_id = 0x03
    action_type = actions.get(payload.get("type", "Twist"), 1)
    speed = float(payload.get("speed", 0.0))
    angle = float(payload.get("angle", 0.0))
    duration = int(payload.get("duration", 0))
    # [cmd_id|action_type|speed|angle|duration]
    return struct.pack("<BBffH", cmd_id, action_type, speed, angle, duration)

def encode_mode(payload: Dict[str, Any]) -> bytes:
    # Example: Mode switch, cmd_id=0x04, mode:uint8, param:uint8
    modes = {
        "motion": 1, "gait": 2, "control": 3
    }
    cmd_id = 0x04
    mode = modes.get(payload.get("mode", "motion"), 1)
    param = int(payload.get("param", 0))
    return struct.pack("<BBB", cmd_id, mode, param)

def encode_command(path: str, payload: Dict[str, Any]) -> bytes:
    if path == "/cmd/estop":
        return encode_estop(payload)
    elif path == "/cmd/action":
        return encode_action(payload)
    elif path == "/cmd/heartbeat":
        return encode_heartbeat(payload)
    elif path == "/cmd/mode":
        return encode_mode(payload)
    else:
        raise NotImplementedError("Unknown command path: %s" % path)

# ========================== DEVICE PROTOCOL DECODING ==========================
def parse_robot_state(data: bytes) -> Dict[str, Any]:
    # This is a stub: in practice, the binary struct format must match the device
    # We'll assume a fake format: battery_level:float32, joint_angles[12]:float32, joint_velocities[12]:float32
    # Total bytes: 4 + 4*12 + 4*12 = 4 + 48 + 48 = 100 bytes
    if len(data) < 100:
        return {"error": "insufficient data"}
    battery_level = struct.unpack_from("<f", data, 0)[0]
    joint_angles = list(struct.unpack_from("<12f", data, 4))
    joint_velocities = list(struct.unpack_from("<12f", data, 52))
    # Add more parsing as needed
    return {
        "battery_level": battery_level,
        "joint_angles": joint_angles,
        "joint_velocities": joint_velocities
    }

# ========================== DEVICE STATUS REPORTING ==========================
class DeviceStatusReporter(threading.Thread):
    def __init__(self, udp_client: RobotUDPClient):
        super().__init__(daemon=True)
        self.udp_client = udp_client
        self.running = True

    def run(self):
        while self.running:
            try:
                if self.udp_client.is_connected():
                    update_edgedevice_phase("Running")
                else:
                    update_edgedevice_phase("Pending")
            except Exception:
                update_edgedevice_phase("Unknown")
            time.sleep(3)

    def stop(self):
        self.running = False

# ========================== HTTP SERVER ==========================
app = Flask(__name__)
udp_client = RobotUDPClient(UDP_DEVICE_IP, UDP_DEVICE_PORT, UDP_LOCAL_BIND_PORT)
status_reporter = DeviceStatusReporter(udp_client)
status_reporter.start()

def get_protocol_property(path: str, key: str, default=None):
    conf = INSTRUCTION_CONFIG.get(path.lstrip("/"), {})
    return conf.get("protocolPropertyList", {}).get(key, default)

@app.route("/state", methods=["GET"])
def state():
    # Optionally, filter/paginate by query params
    data = udp_client.get_last_state()
    if data is None:
        return jsonify({"error": "No state received from device"}), 504
    state = parse_robot_state(data)
    return jsonify(state)

@app.route("/cmd/estop", methods=["POST"])
def cmd_estop():
    payload = request.get_json(force=True)
    packet = encode_estop(payload)
    udp_client.send(packet)
    return jsonify({"result": "Soft emergency stop sent"})

@app.route("/cmd/action", methods=["POST"])
def cmd_action():
    payload = request.get_json(force=True)
    packet = encode_action(payload)
    udp_client.send(packet)
    return jsonify({"result": "Action command sent"})

@app.route("/cmd/mode", methods=["POST"])
def cmd_mode():
    payload = request.get_json(force=True)
    packet = encode_mode(payload)
    udp_client.send(packet)
    return jsonify({"result": "Mode switch command sent"})

@app.route("/cmd/heartbeat", methods=["POST"])
def cmd_heartbeat():
    payload = request.get_json(force=True)
    packet = encode_heartbeat(payload)
    udp_client.send(packet)
    return jsonify({"result": "Heartbeat sent"})

@app.route("/healthz", methods=["GET"])
def healthz():
    return "ok", 200

@app.errorhandler(Exception)
def handle_exception(e):
    return jsonify({"error": str(e)}), 500

def shutdown():
    status_reporter.stop()
    udp_client.close()

import atexit
atexit.register(shutdown)

if __name__ == "__main__":
    try:
        app.run(host=HTTP_HOST, port=HTTP_PORT)
    except KeyboardInterrupt:
        pass