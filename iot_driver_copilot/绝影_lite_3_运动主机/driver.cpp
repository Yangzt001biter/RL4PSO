#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <map>
#include <thread>
#include <mutex>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <yaml-cpp/yaml.h>
#include <nlohmann/json.hpp>
#include <curl/curl.h>
#include "mqtt/async_client.h"

// ----------------------
// Helper Macros/Consts
// ----------------------

#define DEFAULT_MQTT_QOS 1
#define CONFIG_PATH "/etc/edgedevice/config/instructions"
#define DEVICE_STATUS_UPDATE_INTERVAL 5        // seconds
#define DEVICE_TELEMETRY_POLL_INTERVAL 1       // seconds

using json = nlohmann::json;

// ----------------------
// Global Variables
// ----------------------

std::atomic<bool> running{true};
std::mutex device_status_mutex;
std::string device_phase = "Unknown";    // Pending, Running, Failed, Unknown

// ----------------------
// Utility Functions
// ----------------------

std::string getenvOrFail(const char* var) {
    const char* val = std::getenv(var);
    if (!val) {
        std::cerr << "Missing required environment variable: " << var << std::endl;
        exit(1);
    }
    return std::string(val);
}

size_t curl_write_cb(void* contents, size_t size, size_t nmemb, void* userp) {
    std::string* s = (std::string*)userp;
    s->append((char*)contents, size * nmemb);
    return size * nmemb;
}

std::string http_request(const std::string& url, const std::string& method, const std::string& data = "", const std::map<std::string, std::string>& headers = {}) {
    CURL* curl = curl_easy_init();
    if (!curl) return "";

    struct curl_slist* chunk = NULL;
    for (const auto& h : headers) {
        std::string header = h.first + ": " + h.second;
        chunk = curl_slist_append(chunk, header.c_str());
    }

    std::string response;
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 3L);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    if (!headers.empty())
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);

    if (method == "POST") {
        curl_easy_setopt(curl, CURLOPT_POST, 1L);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data.c_str());
    }
    else if (method == "GET") {
        // default
    }
    else if (method == "PUT") {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data.c_str());
    }
    else if (method == "PATCH") {
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PATCH");
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data.c_str());
    }

    CURLcode res = curl_easy_perform(curl);
    curl_easy_cleanup(curl);
    if (chunk) curl_slist_free_all(chunk);
    if (res != CURLE_OK) {
        return "";
    }
    return response;
}

// ------------------------------------
// Kubernetes CRD Status Update Section
// ------------------------------------

void update_edgedevice_status(const std::string& name, const std::string& ns, const std::string& phase) {
    // Use in-cluster service account token
    std::ifstream tokenf("/var/run/secrets/kubernetes.io/serviceaccount/token");
    std::string token((std::istreambuf_iterator<char>(tokenf)), std::istreambuf_iterator<char>());
    tokenf.close();

    std::ifstream caf("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt");
    bool use_ca = caf.good();
    caf.close();

    std::string k8s_host = "https://kubernetes.default.svc";
    std::string url = k8s_host + "/apis/shifu.edgenesis.io/v1alpha1/namespaces/" + ns + "/edgedevices/" + name + "/status";

    json status_patch = {
        {"status", {{"edgeDevicePhase", phase}}}
    };

    std::map<std::string, std::string> headers = {
        {"Authorization", "Bearer " + token},
        {"Content-Type", "application/merge-patch+json"}
    };

    CURL* curl = curl_easy_init();
    if (!curl) return;
    struct curl_slist* chunk = NULL;
    for (const auto& h : headers) {
        std::string header = h.first + ": " + h.second;
        chunk = curl_slist_append(chunk, header.c_str());
    }
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PATCH");
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, status_patch.dump().c_str());
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, chunk);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 3L);
    if (use_ca) {
        curl_easy_setopt(curl, CURLOPT_CAINFO, "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt");
    } else {
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
    }
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
    std::string dummy;
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &dummy);
    curl_easy_perform(curl);
    curl_easy_cleanup(curl);
    if (chunk) curl_slist_free_all(chunk);
}

void device_status_updater(const std::string& name, const std::string& ns) {
    std::string prev_phase = "";
    while (running) {
        device_status_mutex.lock();
        std::string phase = device_phase;
        device_status_mutex.unlock();

        if (phase != prev_phase) {
            update_edgedevice_status(name, ns, phase);
            prev_phase = phase;
        }
        std::this_thread::sleep_for(std::chrono::seconds(DEVICE_STATUS_UPDATE_INTERVAL));
    }
}

// -----------------------------
// ConfigMap Loader
// -----------------------------

struct APIInstructionConfig {
    std::map<std::string, std::string> protocolPropertyList;
};
using InstructionConfigMap = std::map<std::string, APIInstructionConfig>;

InstructionConfigMap load_instruction_config(const std::string& path) {
    InstructionConfigMap result;
    try {
        YAML::Node root = YAML::LoadFile(path);
        for (auto it = root.begin(); it != root.end(); ++it) {
            std::string api = it->first.as<std::string>();
            APIInstructionConfig config;
            if (it->second["protocolPropertyList"]) {
                auto ppl = it->second["protocolPropertyList"];
                for (auto pit = ppl.begin(); pit != ppl.end(); ++pit) {
                    config.protocolPropertyList[pit->first.as<std::string>()] = pit->second.as<std::string>();
                }
            }
            result[api] = config;
        }
    } catch (...) {}
    return result;
}

// -----------------------------
// MQTT <-> HTTP Protocol Bridge
// -----------------------------

class MqttDeviceBridge : public virtual mqtt::callback, public virtual mqtt::iaction_listener {
public:
    MqttDeviceBridge(
        const std::string& broker,
        const std::string& client_id,
        const std::string& user_cmd_topic,
        const std::string& device_telemetry_topic,
        const std::string& device_cmd_topic,
        const std::string& device_http_endpoint,
        InstructionConfigMap& instruction_config,
        int qos = DEFAULT_MQTT_QOS
    ) :
        cli_(broker, client_id),
        user_cmd_topic_(user_cmd_topic),
        device_telemetry_topic_(device_telemetry_topic),
        device_cmd_topic_(device_cmd_topic),
        device_http_endpoint_(device_http_endpoint),
        qos_(qos),
        instruction_config_(instruction_config)
    {}

    void connect() {
        mqtt::connect_options connOpts;
        connOpts.set_clean_session(true);
        cli_.set_callback(*this);

        try {
            cli_.connect(connOpts)->wait();
            cli_.subscribe(user_cmd_topic_, qos_)->wait();
            cli_.subscribe(device_telemetry_topic_, qos_)->wait();
            device_status_mutex.lock();
            device_phase = "Running";
            device_status_mutex.unlock();
        } catch (const mqtt::exception& e) {
            device_status_mutex.lock();
            device_phase = "Failed";
            device_status_mutex.unlock();
            throw;
        }
    }

    void disconnect() {
        try { cli_.disconnect()->wait(); }
        catch (...) {}
        device_status_mutex.lock();
        device_phase = "Unknown";
        device_status_mutex.unlock();
    }

    void stop() {
        running = false;
        disconnect();
    }

    void publish(const std::string& topic, const std::string& payload) {
        mqtt::message_ptr pubmsg = mqtt::make_message(topic, payload);
        pubmsg->set_qos(qos_);
        cli_.publish(pubmsg);
    }

    // ----------------
    // MQTT Callback(s)
    // ----------------
    void message_arrived(mqtt::const_message_ptr msg) override {
        std::string topic = msg->get_topic();
        std::string payload = msg->to_string();

        if (topic == user_cmd_topic_) {
            // User wants to send device command (PUBLISH)
            handle_user_command(payload);
        } else if (topic == device_telemetry_topic_) {
            // Telemetry from device (SUBSCRIBE)
            // Optionally forward to user (or process internally)
            handle_device_telemetry(payload);
        }
    }

    void connection_lost(const std::string& cause) override {
        device_status_mutex.lock();
        device_phase = "Failed";
        device_status_mutex.unlock();
    }

    void delivery_complete(mqtt::delivery_token_ptr) override {}
    void on_failure(const mqtt::token&) override {}
    void on_success(const mqtt::token&) override {}

    // ---------------
    // Device Handlers
    // ---------------

    void handle_user_command(const std::string& payload) {
        // Parse JSON, forward as HTTP POST to device endpoint
        try {
            json j = json::parse(payload);
            std::string cmd = j.value("command", "");
            std::string url = device_http_endpoint_ + "/commands";

            std::string http_resp = http_request(url, "POST", payload, {{"Content-Type", "application/json"}});
            if (!http_resp.empty()) {
                publish(device_cmd_topic_, http_resp);
            } else {
                publish(device_cmd_topic_, R"({"error":"Device HTTP request failed"})");
            }
        } catch (...) {
            publish(device_cmd_topic_, R"({"error":"Invalid command payload"})");
        }
    }

    void handle_device_telemetry(const std::string& payload) {
        // Forward telemetry to user on a topic (or process as needed)
        publish(device_telemetry_topic_ + "/user", payload);
    }

    // ---------------
    // Telemetry Poll
    // ---------------

    void telemetry_poller() {
        while (running) {
            std::string url = device_http_endpoint_ + "/telemetry";
            std::string resp = http_request(url, "GET");
            if (!resp.empty()) {
                publish(device_telemetry_topic_, resp);
            }
            std::this_thread::sleep_for(std::chrono::seconds(DEVICE_TELEMETRY_POLL_INTERVAL));
        }
    }

private:
    mqtt::async_client cli_;
    std::string user_cmd_topic_;
    std::string device_telemetry_topic_;
    std::string device_cmd_topic_;
    std::string device_http_endpoint_;
    int qos_;
    InstructionConfigMap& instruction_config_;
};

// -----------------------------
// Signal Handler
// -----------------------------

void sig_handler(int) {
    running = false;
}

// -----------------------------
// Main Entrypoint
// -----------------------------

int main() {
    signal(SIGTERM, sig_handler);
    signal(SIGINT, sig_handler);

    // Env vars for Kubernetes/EdgeDevice
    std::string EDGEDEVICE_NAME = getenvOrFail("EDGEDEVICE_NAME");
    std::string EDGEDEVICE_NAMESPACE = getenvOrFail("EDGEDEVICE_NAMESPACE");

    // MQTT config from env
    std::string MQTT_BROKER = getenvOrFail("MQTT_BROKER");
    std::string MQTT_CLIENT_ID = getenvOrFail("MQTT_CLIENT_ID");
    std::string MQTT_USER_CMD_TOPIC = getenvOrFail("MQTT_USER_CMD_TOPIC");            // e.g., device/commands/control
    std::string MQTT_DEVICE_TELEMETRY_TOPIC = getenvOrFail("MQTT_DEVICE_TELEMETRY_TOPIC"); // e.g., device/telemetry
    std::string MQTT_DEVICE_CMD_TOPIC = getenvOrFail("MQTT_DEVICE_CMD_TOPIC");        // e.g., device/commands/result

    // HTTP endpoint config
    std::string DEVICE_HTTP_ENDPOINT = getenvOrFail("DEVICE_HTTP_ENDPOINT"); // e.g., http://127.0.0.1:8080

    // Instruction config
    InstructionConfigMap instruction_config = load_instruction_config(CONFIG_PATH);

    // Read device address from EdgeDevice CRD (optional, if needed)
    // For this implementation, DEVICE_HTTP_ENDPOINT is passed via env vars.

    // Initialize curl
    curl_global_init(CURL_GLOBAL_DEFAULT);

    // Start EdgeDevice status update thread
    std::thread status_updater(device_status_updater, EDGEDEVICE_NAME, EDGEDEVICE_NAMESPACE);

    // Start MQTT<->HTTP Bridge
    MqttDeviceBridge bridge(
        MQTT_BROKER, MQTT_CLIENT_ID,
        MQTT_USER_CMD_TOPIC,
        MQTT_DEVICE_TELEMETRY_TOPIC,
        MQTT_DEVICE_CMD_TOPIC,
        DEVICE_HTTP_ENDPOINT,
        instruction_config
    );

    try {
        bridge.connect();
    } catch (...) {
        device_status_mutex.lock();
        device_phase = "Failed";
        device_status_mutex.unlock();
        running = false;
    }

    // Start telemetry poller
    std::thread telemetry_thread(&MqttDeviceBridge::telemetry_poller, &bridge);

    // Main loop
    while (running) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    // Cleanup
    bridge.stop();
    telemetry_thread.join();
    status_updater.join();
    curl_global_cleanup();
    return 0;
}