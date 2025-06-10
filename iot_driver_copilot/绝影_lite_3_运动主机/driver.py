import os
import socket
import yaml
import json
import struct
import threading
import logging
from flask import Flask, request, jsonify
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

# Environment Variables
EDGEDEVICE_NAME = os.environ['EDGEDEVICE_NAME']
EDGEDEVICE_NAMESPACE = os.environ['EDGEDEVICE_NAMESPACE']
HTTP_SERVER_HOST = os.environ.get('HTTP_SERVER_HOST', '0.0.0.0')
HTTP_SERVER_PORT = int(os.environ.get('HTTP_SERVER_PORT', '8080'))
UDP_DEVICE_IP = os.environ['UDP_DEVICE_IP']
UDP_DEVICE_PORT = int(os.environ['UDP_DEVICE_PORT'])

INSTRUCTION_CONFIG_PATH = '/etc/edgedevice/config/instructions'

PHASE_PENDING = "Pending"
PHASE_RUNNING = "Running"
PHASE_FAILED = "Failed"
PHASE_UNKNOWN = "Unknown"

# CRD constants
CRD_GROUP = "shifu.edgenesis.io"
CRD_VERSION = "v1alpha1"
CRD_PLURAL = "edgedevices"

# --- Kubernetes CRD Status Updater ---
class CRDStatusUpdater(threading.Thread):
    def __init__(self, device_name, namespace):
        super().__init__(daemon=True)
        config.load_incluster_config()
        self.api = client.CustomObjectsApi()
        self.device_name = device_name
        self.namespace = namespace
        self.last_phase = None
        self._phase_lock = threading.Lock()
        self._current_phase = PHASE_UNKNOWN

    def set_phase(self, phase):
        with self._phase_lock:
            if phase != self._current_phase:
                self._current_phase = phase

    def run(self):
        while True:
            with self._phase_lock:
                phase = self._current_phase
            if phase != self.last_phase:
                self._update_phase(phase)
                self.last_phase = phase
            threading.Event().wait(3)

    def _update_phase(self, phase):
        body = {"status": {"edgeDevicePhase": phase}}
        try:
            self.api.patch_namespaced_custom_object_status(
                group=CRD_GROUP,
                version=CRD_VERSION,
                namespace=self.namespace,
                plural=CRD_PLURAL,
                name=self.device_name,
                body=body
            )
            logging.info(f"EdgeDevice status updated: {phase}")
        except ApiException as e:
            logging.error(f"Failed to update EdgeDevice phase: {e}")

# --- UDP Client ---
class UdpDeviceClient:
    def __init__(self, udp_ip, udp_port):
        self.udp_ip = udp_ip
        self.udp_port = udp_port
        self.sock = None
        self.lock = threading.Lock()

    def connect(self):
        with self.lock:
            if self.sock is not None:
                self.sock.close()
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.settimeout(2.0)

    def close(self):
        with self.lock:
            if self.sock:
                self.sock.close()
            self.sock = None

    def send_recv(self, payload):
        with self.lock:
            if not self.sock:
                self.connect()
            try:
                self.sock.sendto(payload, (self.udp_ip, self.udp_port))
                data, _ = self.sock.recvfrom(4096)
                return data
            except Exception as e:
                logging.warning(f"UDP communication error: {e}")
                self.close()
                raise e

# --- Config Loader ---
def load_instruction_config():
    if not os.path.exists(INSTRUCTION_CONFIG_PATH):
        return {}
    with open(INSTRUCTION_CONFIG_PATH) as f:
        return yaml.safe_load(f) or {}

api_settings = load_instruction_config()

# --- DeviceShifu Logic ---
class RobotDriver:
    def __init__(self, udp_client):
        self.udp = udp_client

    # Example: UDP protocol mapping functions (to be customized per actual binary protocol)
    def build_state_query(self, fields=None):
        # For demonstration, a simple protocol: "STATE?fields=rpy,battery"
        if fields:
            field_str = ','.join(fields)
            msg = f'STATE?fields={field_str}'.encode('utf-8')
        else:
            msg = b'STATE'
        return msg

    def parse_state_response(self, data):
        # Real implementation: parse the binary struct to dict
        # Here, we try to decode as UTF-8, fallback to hex
        try:
            text = data.decode('utf-8')
            if text.startswith('{'):
                return json.loads(text)
            return {"raw": text}
        except Exception:
            return {"raw_hex": data.hex()}

    def build_command(self, cmd_name, params):
        # For demonstration, build as "CMD:heartbeat" or "CMD:stand:height=0.5"
        param_str = ''
        if params:
            param_str = ':' + ','.join(f"{k}={v}" for k, v in params.items())
        msg = f'CMD:{cmd_name}{param_str}'.encode('utf-8')
        return msg

    def parse_command_response(self, data):
        # Real implementation: parse status/acknowledgement
        try:
            text = data.decode('utf-8')
            return {"response": text}
        except Exception:
            return {"response_hex": data.hex()}

# --- Flask HTTP Server ---
app = Flask(__name__)

udp_client = UdpDeviceClient(UDP_DEVICE_IP, UDP_DEVICE_PORT)
robot_driver = RobotDriver(udp_client)
status_updater = CRDStatusUpdater(EDGEDEVICE_NAME, EDGEDEVICE_NAMESPACE)
status_updater.set_phase(PHASE_PENDING)
status_updater.start()

def maintain_device_status():
    # Background thread to probe device and update status
    def probe():
        while True:
            try:
                # Send a heartbeat or state query
                query = robot_driver.build_state_query(fields=["robot_basic_state"])
                udp_client.connect()
                udp_client.send_recv(query)
                status_updater.set_phase(PHASE_RUNNING)
            except socket.timeout:
                status_updater.set_phase(PHASE_PENDING)
            except Exception:
                status_updater.set_phase(PHASE_FAILED)
            threading.Event().wait(5)
    t = threading.Thread(target=probe, daemon=True)
    t.start()

maintain_device_status()

@app.route('/state', methods=['GET'])
def get_state():
    fields = request.args.get('fields')
    field_list = None
    if fields:
        field_list = [f.strip() for f in fields.split(',')]
    try:
        query = robot_driver.build_state_query(field_list)
        data = udp_client.send_recv(query)
        state = robot_driver.parse_state_response(data)
        if field_list:
            # Filter fields if possible
            filtered = {k: v for k, v in state.items() if k in field_list}
            return jsonify(filtered)
        return jsonify(state)
    except socket.timeout:
        status_updater.set_phase(PHASE_PENDING)
        return jsonify({"error": "Device timeout"}), 504
    except Exception as e:
        status_updater.set_phase(PHASE_FAILED)
        return jsonify({"error": str(e)}), 500

@app.route('/cmd', methods=['POST'])
def post_cmd():
    try:
        payload = request.get_json(force=True)
        cmd_name = payload.get('command')
        params = payload.get('params', {})
        if not cmd_name:
            return jsonify({"error": "Missing 'command' in JSON"}), 400
        cmd = robot_driver.build_command(cmd_name, params)
        data = udp_client.send_recv(cmd)
        resp = robot_driver.parse_command_response(data)
        return jsonify(resp)
    except socket.timeout:
        status_updater.set_phase(PHASE_PENDING)
        return jsonify({"error": "Device timeout"}), 504
    except Exception as e:
        status_updater.set_phase(PHASE_FAILED)
        return jsonify({"error": str(e)}), 500

@app.route('/healthz', methods=['GET'])
def healthz():
    return "ok"

if __name__ == '__main__':
    app.run(host=HTTP_SERVER_HOST, port=HTTP_SERVER_PORT)