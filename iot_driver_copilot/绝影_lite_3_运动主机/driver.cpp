#include <iostream>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <chrono>
#include <atomic>
#include <fstream>
#include <sstream>
#include <map>
#include <mutex>

// MQTT
#include <mqtt/async_client.h>

// HTTP
#include <curl/curl.h>

// YAML parsing
#include <yaml-cpp/yaml.h>

// K8s API
#include <nlohmann/json.hpp>
#include <curl/curl.h>

using json = nlohmann::json;

// --------- ENV VAR UTILS ----------
std::string getenv_or_throw(const std::string& var) {
    const char* val = std::getenv(var.c_str());
    if (!val) {
        throw std::runtime_error("Environment variable " + var + " is not set");
    }
    return std::string(val);
}

std::string getenv_or_default(const std::string& var, const std::string& def) {
    const char* val = std::getenv(var.c_str());
    return val ? std::string(val) : def;
}

// --------- YAML CONFIG ------------
struct ApiSettings {
    std::map<std::string, std::string> protocolPropertyList;
};

class InstructionConfig {
public:
    std::map<std::string, ApiSettings> apis;

    static InstructionConfig Load(const std::string& config_path) {
        InstructionConfig config;
        YAML::Node node = YAML::LoadFile(config_path);
        for (const auto& it : node) {
            ApiSettings api;
            if (it.second["protocolPropertyList"]) {
                for (const auto& p : it.second["protocolPropertyList"]) {
                    api.protocolPropertyList[p.first.as<std::string>()] = p.second.as<std::string>();
                }
            }
            config.apis[it.first.as<std::string>()] = api;
        }
        return config;
    }
};

// --------- K8S CLIENT -------------
class K8sClient {
    std::string token;
    std::string api_server;
public:
    K8sClient() {
        std::ifstream tokenFile("/var/run/secrets/kubernetes.io/serviceaccount/token");
        token = std::string((std::istreambuf_iterator<char>(tokenFile)), std::istreambuf_iterator<char>());
        api_server = "https://" + getenv_or_default("KUBERNETES_SERVICE_HOST", "kubernetes.default.svc") 
                        + ":" + getenv_or_default("KUBERNETES_SERVICE_PORT", "443");
    }

    bool patch_status(const std::string& ns, const std::string& name, const std::string& phase) {
        std::string url = api_server + "/apis/shifu.edgenesis.io/v1alpha1/namespaces/" + ns + "/edgedevices/" + name + "/status";
        json patch_json;
        patch_json["status"]["edgeDevicePhase"] = phase;

        CURL* curl = curl_easy_init();
        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/merge-patch+json");
        std::string auth_header = "Authorization: Bearer " + token;
        headers = curl_slist_append(headers, auth_header.c_str());

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PATCH");
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, patch_json.dump().c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, patch_json.dump().size());
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

        long http_code = 0;
        CURLcode res = curl_easy_perform(curl);
        curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

        curl_easy_cleanup(curl);
        curl_slist_free_all(headers);

        return (res == CURLE_OK && (http_code == 200 || http_code == 201));
    }

    // Fetch EdgeDevice CR (for .spec.address)
    std::string get_device_address(const std::string& ns, const std::string& name) {
        std::string url = api_server + "/apis/shifu.edgenesis.io/v1alpha1/namespaces/" + ns + "/edgedevices/" + name;
        CURL* curl = curl_easy_init();
        struct curl_slist* headers = nullptr;
        std::string auth_header = "Authorization: Bearer " + token;
        headers = curl_slist_append(headers, auth_header.c_str());

        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

        std::string response;
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, +[](char* ptr, size_t size, size_t nmemb, void* userdata) {
            std::string* s = static_cast<std::string*>(userdata);
            s->append(ptr, size*nmemb);
            return size*nmemb;
        });
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

        CURLcode res = curl_easy_perform(curl);
        curl_easy_cleanup(curl);
        curl_slist_free_all(headers);

        if (res != CURLE_OK) return "";

        auto j = json::parse(response, nullptr, false);
        if (j.is_discarded() || !j.contains("spec") || !j["spec"].contains("address")) return "";
        return j["spec"]["address"];
    }
};

// --------- HTTP CLIENT ------------
class HttpClient {
public:
    HttpClient() { curl_global_init(CURL_GLOBAL_ALL); }
    ~HttpClient() { curl_global_cleanup(); }

    bool post(const std::string& url, const std::string& data, std::string& result) {
        CURL* curl = curl_easy_init();
        struct curl_slist* headers = nullptr;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data.c_str());
        curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

        // No verify for self-signed
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, +[](char* ptr, size_t size, size_t nmemb, void* userdata) {
            std::string* s = static_cast<std::string*>(userdata);
            s->append(ptr, size*nmemb);
            return size*nmemb;
        });
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result);

        CURLcode res = curl_easy_perform(curl);
        curl_easy_cleanup(curl);
        curl_slist_free_all(headers);
        return (res == CURLE_OK);
    }

    bool get(const std::string& url, std::string& result) {
        CURL* curl = curl_easy_init();
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());

        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(curl, CURLOPT_SSL_VERIFYHOST, 0L);

        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, +[](char* ptr, size_t size, size_t nmemb, void* userdata) {
            std::string* s = static_cast<std::string*>(userdata);
            s->append(ptr, size*nmemb);
            return size*nmemb;
        });
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result);

        CURLcode res = curl_easy_perform(curl);
        curl_easy_cleanup(curl);
        return (res == CURLE_OK);
    }
};

// --------- MQTT CLIENT ------------
class MqttBridge {
    std::string broker_address;
    std::string client_id;
    std::string user_topic_telemetry;
    std::string user_topic_commands;
    std::string device_http_endpoint;
    std::string telemetry_http_url;
    std::string command_http_url;

    std::string username, password;
    int qos;
    mqtt::async_client client;
    mqtt::connect_options connOpts;
    InstructionConfig config;

    std::atomic<bool> connected;
    std::atomic<bool> device_online;
    std::atomic<bool> stop_telemetry;
    std::mutex status_mutex;

    K8sClient k8s;
    HttpClient http;

    std::string edgeDeviceName, edgeDeviceNamespace;
    std::thread telemetry_thread;

public:
    MqttBridge(const std::string& broker, const std::string& clientId,
        const std::string& telem_topic, const std::string& cmd_topic,
        const std::string& device_http, int qos_,
        const InstructionConfig& cfg,
        const std::string& edgeName, const std::string& edgeNs)
        : broker_address(broker),
          client_id(clientId),
          user_topic_telemetry(telem_topic), user_topic_commands(cmd_topic),
          device_http_endpoint(device_http),
          connOpts(),
          config(cfg),
          connected(false), device_online(false), stop_telemetry(false),
          edgeDeviceName(edgeName), edgeDeviceNamespace(edgeNs),
          client(broker, clientId), qos(qos_), http()
    {
        username = getenv_or_default("MQTT_USERNAME", "");
        password = getenv_or_default("MQTT_PASSWORD", "");
        if (!username.empty())
            connOpts.set_user_name(username);
        if (!password.empty())
            connOpts.set_password(password);

        telemetry_http_url = device_http_endpoint + "/telemetry";
        command_http_url = device_http_endpoint + "/commands/control";
    }

    void update_status(const std::string& phase) {
        std::lock_guard<std::mutex> lk(status_mutex);
        k8s.patch_status(edgeDeviceNamespace, edgeDeviceName, phase);
    }

    void start() {
        try {
            client.set_connected_handler([this](const std::string&) {
                connected = true;
                update_status("Running");
                subscribe_topics();
            });
            client.set_connection_lost_handler([this](const std::string&) {
                connected = false;
                update_status("Pending");
            });
            client.set_message_callback([this](mqtt::const_message_ptr msg) {
                this->on_message(msg);
            });

            mqtt::token_ptr conntok = client.connect(connOpts);
            conntok->wait();
            connected = true;
            update_status("Running");
        } catch (const mqtt::exception& exc) {
            update_status("Failed");
            std::cerr << "MQTT Connect failed: " << exc.what() << std::endl;
            return;
        }

        // Start telemetry thread
        telemetry_thread = std::thread([this]() { telemetry_loop(); });
    }

    void stop() {
        stop_telemetry = true;
        if (telemetry_thread.joinable()) {
            telemetry_thread.join();
        }
        try {
            client.disconnect()->wait();
            update_status("Pending");
        } catch (...) {}
    }

    void subscribe_topics() {
        client.subscribe(user_topic_commands, qos);
    }

    void telemetry_loop() {
        while (!stop_telemetry) {
            if (!connected) {
                update_status("Pending");
                std::this_thread::sleep_for(std::chrono::seconds(2));
                continue;
            }
            std::string response;
            bool http_ok = http.get(telemetry_http_url, response);
            if (http_ok) {
                device_online = true;
                update_status("Running");
                client.publish(user_topic_telemetry, response, qos, false);
            } else {
                device_online = false;
                update_status("Failed");
            }
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
    }

    void on_message(mqtt::const_message_ptr msg) {
        // Commands from users; forward to device via HTTP
        std::string payload = msg->to_string();
        std::string http_resp;
        bool ok = http.post(command_http_url, payload, http_resp);
        std::string result_topic = user_topic_commands + "/response";
        if (ok) {
            client.publish(result_topic, http_resp, qos, false);
        } else {
            json err;
            err["error"] = "Failed to send command to device";
            client.publish(result_topic, err.dump(), qos, false);
        }
    }
};

// --------- MAIN -------------------
int main() {
    try {
        std::string edgeDeviceName = getenv_or_throw("EDGEDEVICE_NAME");
        std::string edgeDeviceNamespace = getenv_or_throw("EDGEDEVICE_NAMESPACE");

        std::string mqtt_broker = getenv_or_throw("MQTT_BROKER_ADDRESS");
        std::string mqtt_client_id = getenv_or_default("MQTT_CLIENT_ID", "device-shifu-lite3");
        std::string mqtt_cmd_topic = getenv_or_default("MQTT_COMMAND_TOPIC", "device/commands/control");
        std::string mqtt_telem_topic = getenv_or_default("MQTT_TELEMETRY_TOPIC", "device/telemetry");
        int mqtt_qos = std::stoi(getenv_or_default("MQTT_QOS", "1"));

        std::string config_path = "/etc/edgedevice/config/instructions";
        InstructionConfig config = InstructionConfig::Load(config_path);

        // K8s client to get device HTTP endpoint (from .spec.address)
        K8sClient k8s;
        std::string device_http_endpoint = k8s.get_device_address(edgeDeviceNamespace, edgeDeviceName);
        if (device_http_endpoint.empty()) {
            throw std::runtime_error("Device HTTP endpoint address missing in EdgeDevice.spec.address");
        }

        MqttBridge bridge(
            mqtt_broker, mqtt_client_id,
            mqtt_telem_topic, mqtt_cmd_topic,
            device_http_endpoint,
            mqtt_qos,
            config,
            edgeDeviceName, edgeDeviceNamespace
        );

        bridge.update_status("Pending");
        bridge.start();

        // Wait for SIGTERM/SIGINT
        std::atomic<bool> running(true);
        std::signal(SIGINT, [](int){ std::exit(0); });
        std::signal(SIGTERM, [](int){ std::exit(0); });
        while (running) std::this_thread::sleep_for(std::chrono::seconds(1));

        bridge.stop();

    } catch (const std::exception& ex) {
        std::cerr << "DeviceShifu driver failed: " << ex.what() << std::endl;
        return 1;
    }
    return 0;
}