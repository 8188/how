#include <chrono>
#include <cstddef>
#include <ctime>
#include <iostream>
#include <memory>
#include <optional>
#include <stdexcept>
#include <string_view>

#include "csv.hpp"
#include "dotenv.h"
#include "nlohmann/json.hpp"
#include "taskflow/taskflow.hpp"
#include <mqtt/async_client.h>
#include <sw/redis++/redis++.h>

using json = nlohmann::json;

const std::string PROJECT_NAME { "HOW" };
constexpr const long long INTERVAL { 5000000 };
constexpr const int QOS { 1 };
constexpr const auto TIMEOUT { std::chrono::seconds(10) };
constexpr const char* DATE_FORMAT { "%Y-%m-%d %H:%M:%S" };
constexpr const double H2_PRESSURE_LOW { 0.44 };
constexpr const double H2_PURITY_LOW { 95 };
constexpr const double H2_PURITY_LOWLOW { 92 };
constexpr const double H2_LEAKAGE_LOW { 4 };
constexpr const double OIL_H2_PRESSURE_DIFF_LOW { 36 };
constexpr const double OIL_H2_PRESSURE_DIFF_HIGH { 76 };
constexpr const double WATER_FLOW_LOW { 22 };
constexpr const double WATER_CONDUCTIVITY_HIGH { 0.5 };
constexpr const double WATER_CONDUCTIVITY_HIGHHIGH { 2 };

bool fileExists(const std::string& filename)
{
    std::ifstream file(filename);
    return file.good();
}

std::string generate_random_string_with_hyphens()
{
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(0, 15);

    std::stringstream ss;
    const char* hex_chars = "0123456789abcdef";
    for (int i = 0; i < 32; ++i) {
        if (i == 8 || i == 12 || i == 16 || i == 20) {
            ss << "-";
        }
        int index = dis(gen);
        ss << hex_chars[index];
    }

    return ss.str();
}

const std::string get_now()
{
    constexpr int BUFFER_SIZE { 20 };

    auto now { std::chrono::system_clock::now() };
    auto now_time { std::chrono::system_clock::to_time_t(now) };
    char buffer[BUFFER_SIZE];
    std::strftime(buffer, BUFFER_SIZE, DATE_FORMAT, std::localtime(&now_time));
    return std::string(buffer);
}

std::time_t string2time(const std::string& timeStr)
{
    std::tm tm = {};
    strptime(timeStr.c_str(), DATE_FORMAT, &tm);
    return std::mktime(&tm);
}

class MyRedis {
private:
    sw::redis::Redis m_redis;
    
    sw::redis::ConnectionOptions makeConnectionOptions(const std::string& ip, int port, int db, const std::string& user, const std::string& password)
    {
        sw::redis::ConnectionOptions opts;
        opts.host = ip;
        opts.port = port;
        opts.db = db;
        if (!user.empty()) {
            opts.user = user;
        }
        if (!password.empty()) {
            opts.password = password;
        }
        opts.socket_timeout = std::chrono::milliseconds(50);
        return opts;
    }

    sw::redis::ConnectionPoolOptions makePoolOptions()
    {
        sw::redis::ConnectionPoolOptions pool_opts;
        pool_opts.size = 3;
        pool_opts.wait_timeout = std::chrono::milliseconds(50);
        return pool_opts;
    }

public:
    MyRedis(const std::string& ip, int port, int db, const std::string& user, const std::string& password)
        : m_redis(makeConnectionOptions(ip, port, db, user, password), makePoolOptions())
    {
        m_redis.ping();
        std::cout << "Connected to Redis.\n";
    }

    MyRedis(const std::string& unixSocket)
        : m_redis(unixSocket)
    {
        m_redis.ping();
        std::cout << "Connected to Redis by unix socket.\n";
    }

    std::string m_hget(const std::string& key, const std::string& field)
    {
        std::string res;
        try {
            const auto optional_str = m_redis.hget(key, field);
            res = optional_str.value_or("0");
        } catch (const std::exception& e) {
            std::cerr << "Exception: " << e.what() << std::endl;
        }
        return res;
    }

    void m_hset(const std::string_view& hash, const std::string_view& key, const std::string_view& value)
    {
        try {
            m_redis.hset(hash, key, value);
        } catch (const std::exception& e) {
            std::cerr << "Exception: " << e.what() << std::endl;
        }
    }
};

class MyMQTT {
private:
    mqtt::async_client client;
    mqtt::connect_options connOpts;

    mqtt::connect_options buildConnectOptions(const std::string& username, const std::string& password,
        const std::string& caCerts, const std::string& certfile,
        const std::string& keyFile, const std::string& keyFilePassword) const
    {
        // mqtt::connect_options_builder()对应mqtt:/ip:port, ::ws()对应ws:/ip:port
        auto connBuilder = mqtt::connect_options_builder()
                               .user_name(username)
                               .password(password)
                               .keep_alive_interval(std::chrono::seconds(45));

        if (!caCerts.empty()) {
            mqtt::ssl_options ssl;
            ssl.set_trust_store(caCerts);
            ssl.set_key_store(certfile);
            ssl.set_private_key(keyFile);
            ssl.set_private_key_password(keyFilePassword);

            connBuilder.ssl(ssl);
        }

        return connBuilder.finalize();
    }

    void disconnect()
    {
        if (client.is_connected()) {
            client.disconnect()->wait();
            std::cout << "Disconnected from MQTT broker.\n";
        }
    }

public:
    MyMQTT(const std::string& address, const std::string& clientId,
        const std::string& username, const std::string& password,
        const std::string& caCerts, const std::string& certfile,
        const std::string& keyFile, const std::string& keyFilePassword)
        : client(address, clientId)
        , connOpts { buildConnectOptions(username, password, caCerts, certfile, keyFile, keyFilePassword) }
    {
        connect();
        if (!client.is_connected()) {
            throw std::runtime_error("MQTT connection is not established.");
        }
    }

    MyMQTT(const MyMQTT&) = delete;
    MyMQTT& operator=(const MyMQTT&) = delete;
    MyMQTT(MyMQTT&&) = default;
    MyMQTT& operator=(MyMQTT&&) = default;

    ~MyMQTT() noexcept
    {
        disconnect();
    }

    void connect()
    {
        try {
            client.connect(connOpts)->wait_for(TIMEOUT); // 断线重连
            std::cout << "Connected to MQTT broker.\n";
        } catch (const mqtt::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
        }
    }

    void publish(const std::string& topic, const std::string& payload, int qos, bool retained = false)
    {
        auto msg = mqtt::make_message(topic, payload, qos, retained);
        try {
            bool ok = client.publish(msg)->wait_for(TIMEOUT);
            if (!ok) {
                std::cerr << "Error: Publishing message timed out." << std::endl;
            }
        } catch (const mqtt::exception& e) {
            std::cerr << "Error: " << e.what() << std::endl;
            connect();
        }
    }
};

class MechanismBase {
private:
    struct Alarm {
        std::string_view code;
        std::string desc;
        std::string_view advice;
        std::string startTime;
    };

protected:
    const std::string m_unit;
    std::shared_ptr<MyRedis> m_redis;
    std::shared_ptr<MyMQTT> m_MQTTCli;
    std::unordered_map<std::string_view, std::unordered_map<std::string_view, std::vector<Alarm>>> alerts {};
    csv::CSVRow& m_c_df;

    const std::vector<std::string> m_H2Leakage;
    const std::vector<std::string> m_H2Leakage_switch;
    const std::vector<std::string> others;
    std::vector<std::string> m_all_targets {};

    MechanismBase(const std::string& unit, std::shared_ptr<MyRedis> redis, std::shared_ptr<MyMQTT> MQTTCli, csv::CSVReader::iterator& it)
        : m_unit { unit }
        , m_redis { redis }
        , m_MQTTCli { MQTTCli }
        , m_c_df { *it }
        , m_H2Leakage {
            "发电机内冷水箱氢气泄漏",
            "发电机密封油励侧回油氢气泄漏",
            "发电机密封油汽侧回油氢气泄漏",
            "发电机封闭母线A相氢气泄漏",
            "发电机封闭母线B相氢气泄漏",
            "发电机封闭母线C相氢气泄漏",
            "发电机封闭母线中性点1氢气泄漏",
            "发电机封闭母线中性点2氢气泄漏",
        }
        , m_H2Leakage_switch {
            "发电机内冷水箱氢气泄漏高报警",
            "发电机密封油励侧回油氢气泄漏高报警",
            "发电机密封油汽侧回油氢气泄漏高",
            "发电机封闭母线A相氢气泄漏高",
            "发电机封闭母线B相氢气泄漏高",
            "发电机封闭母线C相氢气泄漏高",
            "发电机封闭母线中性点1氢气泄漏高",
            "发电机封闭母线中性点2氢气泄漏高",
        }
        , others { "进氢压力", "发电机内氢气纯度", "发电机密封油-氢气差压", "发电机定子冷却水电导率", "发电机定子冷却水流量1差压" }
    {
        if (unit < "1" || unit > "9") {
            throw std::invalid_argument("unit must be in the range from '1' to '9'");
        }

        const std::vector<std::vector<std::string>> allVectors {
            m_H2Leakage,
            m_H2Leakage_switch,
            others
        };

        std::size_t estimatedTotalSize { 100 };
        std::size_t totalSize { 0 };
        m_all_targets.reserve(estimatedTotalSize);
        for (const auto& vec : allVectors) {
            totalSize += vec.size();
            if (totalSize > estimatedTotalSize) {
                m_all_targets.reserve(m_all_targets.capacity() * 2);
                estimatedTotalSize *= 2;

                std::cerr << "Warning: Total size exceeded estimated size. Doubling capacity.\n";
            }
            m_all_targets.insert(m_all_targets.end(), vec.begin(), vec.end());
        }

        m_all_targets.shrink_to_fit();
    }

    virtual int logic() = 0;

    void trigger(const std::string_view& key, const std::string_view& field, const std::string_view& tag,
        const std::string_view& content, const std::string_view& st, const std::string_view& now)
    {
        Alarm newAlarm;
        newAlarm.code = tag;
        newAlarm.desc = content;
        newAlarm.advice = "";
        newAlarm.startTime = st;

        if (st == "0") {
            m_redis->m_hset(key, field, now);
            newAlarm.startTime = now;
        }

        alerts[m_unit]["alarms"].emplace_back(newAlarm);
    }

    void revert(const std::string_view& key, const std::string_view& field, const std::string_view& st) const
    {
        if (!st.empty()) {
            m_redis->m_hset(key, field, "0");
        }
    }

    template <typename T>
    std::optional<T> get_value_from_CSVRow(csv::CSVRow& row, const std::string& colName) const
    {
        T value {};
        try {
            return row[colName].get<T>();
        } catch (const std::exception& e) {
            std::cerr << e.what() << '\n';
            return std::nullopt;
        }
    }

public:
    void send_message(const std::string& topic)
    {
        json j;
        for (const auto& pair : alerts[m_unit]) {
            json alarms;
            for (const auto& alarm : pair.second) {
                json alarmJson;
                alarmJson["code"] = alarm.code;
                alarmJson["desc"] = alarm.desc;
                alarmJson["advice"] = alarm.advice;
                alarmJson["startTime"] = alarm.startTime;
                alarms.push_back(alarmJson);
                // std::cout << "Code: " << alarmJson["code"] << ", Desc: " << alarmJson["desc"] << ", Advice: " << alarmJson["advice"] << ", Start Time: " << alarmJson["startTime"] << '\n';
            }
            j[std::string(pair.first)] = alarms;
        }

        const std::string jsonString = j.dump();
        m_MQTTCli->publish(topic, jsonString, QOS);
        alerts.clear();
    }
};

class H2Pressure : public MechanismBase {
private:
    const std::vector<std::string> m_H2Pressure;
    int iUnit;

public:
    H2Pressure(const std::string& unit, std::shared_ptr<MyRedis> redis, std::shared_ptr<MyMQTT> MQTTCli, csv::CSVReader::iterator& it)
        : MechanismBase(unit, redis, MQTTCli, it)
        , m_H2Pressure { "进氢压力" }
        , iUnit { std::stoi(m_unit) - 1 }
    {
    }

    int logic() override
    {
        int flag { 0 };
        const std::string key { PROJECT_NAME + m_unit + ":Mechanism:H2_pressure" };
        const std::string content { "发电机进氢压力低" };
        const std::string now { get_now() };

        for (const std::string& tag : m_H2Pressure) {
            const std::string st = m_redis->m_hget(key, tag);

            auto H2_pressure_opt = get_value_from_CSVRow<double>(m_c_df, tag);
            if (!H2_pressure_opt.has_value()) {
                continue;
            }
            // std::cout << "H2_pressure: " << H2_pressure_opt.value() << '\n';
            if (H2_pressure_opt.value() < H2_PRESSURE_LOW) {
                trigger(key, tag, tag, content, st, now);
                flag = 1;
            } else {
                revert(key, tag, st);
            }
        }
        return flag;
    }
};

class WaterFlow : public MechanismBase {
private:
    const std::vector<std::string> m_WaterFlow;
    int iUnit;

public:
    WaterFlow(const std::string& unit, std::shared_ptr<MyRedis> redis, std::shared_ptr<MyMQTT> MQTTCli, csv::CSVReader::iterator& it)
        : MechanismBase(unit, redis, MQTTCli, it)
        , m_WaterFlow { "发电机定子冷却水流量1差压" }
        , iUnit { std::stoi(m_unit) - 1 }
    {
    }

    int logic() override
    {
        int flag { 0 };
        const std::string key { PROJECT_NAME + m_unit + ":Mechanism:water_flow" };
        const std::string content { "发电机流量低" };
        const std::string now { get_now() };

        for (const std::string& tag : m_WaterFlow) {
            const std::string st = m_redis->m_hget(key, tag);

            auto water_flow_opt = get_value_from_CSVRow<double>(m_c_df, tag);
            if (!water_flow_opt.has_value()) {
                continue;
            }
            // std::cout << "water flow: " << water_flow_opt.value()  << '\n';
            if (water_flow_opt.value() < WATER_FLOW_LOW) {
                trigger(key, tag, tag, content, st, now);
                flag = 1;
            } else {
                revert(key, tag, st);
            }
        }
        return flag;
    }
};

class DiffPressure : public MechanismBase {
private:
    const std::vector<std::string> m_DiffPressure;
    int iUnit;

public:
    DiffPressure(const std::string& unit, std::shared_ptr<MyRedis> redis, std::shared_ptr<MyMQTT> MQTTCli, csv::CSVReader::iterator& it)
        : MechanismBase(unit, redis, MQTTCli, it)
        , m_DiffPressure { "发电机密封油-氢气差压" }
        , iUnit { std::stoi(m_unit) - 1 }
    {
    }

    int logic() override
    {
        int flag { 0 };
        const std::string key { PROJECT_NAME + m_unit + ":Mechanism:diff_pressure" };
        const std::string content { "油氢差压" };
        const std::string now { get_now() };

        for (const std::string& tag : m_DiffPressure) {
            const std::string st1 = m_redis->m_hget(key, tag + "_1");
            const std::string st2 = m_redis->m_hget(key, tag + "_2");

            auto diff_pressure_opt = get_value_from_CSVRow<double>(m_c_df, tag);
            if (!diff_pressure_opt.has_value()) {
                continue;
            }
            // std::cout << "diff pressure: " << diff_pressure_opt.value()  << '\n';
            if (diff_pressure_opt.value() > OIL_H2_PRESSURE_DIFF_HIGH) {
                trigger(key, tag + "_1", tag, content + "高", st1, now);
                flag = 1;
            } else {
                revert(key, tag + "_1", st1);
            }

            if (diff_pressure_opt.value() < OIL_H2_PRESSURE_DIFF_LOW) {
                trigger(key, tag + "_2", tag, content + "低", st2, now);
                flag = 1;
            } else {
                revert(key, tag + "_2", st2);
            }
        }
        return flag;
    }
};

class H2Purity : public MechanismBase {
private:
    const std::vector<std::string> m_H2Purity;
    int iUnit;

public:
    H2Purity(const std::string& unit, std::shared_ptr<MyRedis> redis, std::shared_ptr<MyMQTT> MQTTCli, csv::CSVReader::iterator& it)
        : MechanismBase(unit, redis, MQTTCli, it)
        , m_H2Purity { "发电机内氢气纯度" }
        , iUnit { std::stoi(m_unit) - 1 }
    {
    }

    int logic() override
    {
        int flag { 0 };
        const std::string key { PROJECT_NAME + m_unit + ":Mechanism:H2_purity" };
        const std::string content { "氢气纯度" };
        const std::string now { get_now() };

        for (const std::string& tag : m_H2Purity) {
            const std::string st1 = m_redis->m_hget(key, tag + "_1");
            const std::string st2 = m_redis->m_hget(key, tag + "_2");

            auto H2_purity_opt = get_value_from_CSVRow<double>(m_c_df, tag);
            if (!H2_purity_opt.has_value()) {
                continue;
            }
            // std::cout << "H2_purity: " << H2_purity_opt.value()  << '\n';
            if (H2_purity_opt.value() < H2_PURITY_LOW) {
                trigger(key, tag + "_1", tag, content + "低", st1, now);
                flag = 1;
            } else {
                revert(key, tag + "_1", st1);
            }

            if (H2_purity_opt.value() < H2_PURITY_LOWLOW) {
                trigger(key, tag + "_2", tag, content + "低低", st2, now);
                flag = 1;
            } else {
                revert(key, tag + "_2", st2);
            }
        }
        return flag;
    }
};

class Conductivity : public MechanismBase {
private:
    const std::vector<std::string> m_Conductivity;
    int iUnit;

public:
    Conductivity(const std::string& unit, std::shared_ptr<MyRedis> redis, std::shared_ptr<MyMQTT> MQTTCli, csv::CSVReader::iterator& it)
        : MechanismBase(unit, redis, MQTTCli, it)
        , m_Conductivity { "发电机定子冷却水电导率" }
        , iUnit { std::stoi(m_unit) - 1 }
    {
    }

    int logic() override
    {
        int flag { 0 };
        const std::string key { PROJECT_NAME + m_unit + ":Mechanism:conductivity" };
        const std::string content { "定冷水电导率" };
        const std::string now { get_now() };

        for (const std::string& tag : m_Conductivity) {
            const std::string st1 = m_redis->m_hget(key, tag + "_1");
            const std::string st2 = m_redis->m_hget(key, tag + "_2");

            auto conductivity_opt = get_value_from_CSVRow<double>(m_c_df, tag);
            if (!conductivity_opt.has_value()) {
                continue;
            }
            // std::cout << "conductivity: " << conductivity_opt.value()  << '\n';
            if (conductivity_opt.value() > WATER_CONDUCTIVITY_HIGH) {
                trigger(key, tag + "_1", tag, content + "高", st1, now);
                flag = 1;
            } else {
                revert(key, tag + "_1", st1);
            }

            if (conductivity_opt.value() > WATER_CONDUCTIVITY_HIGHHIGH) {
                trigger(key, tag + "_2", tag, content + "高高", st2, now);
                flag = 1;
            } else {
                revert(key, tag + "_2", st2);
            }
        }
        return flag;
    }
};

class H2Leakage : public MechanismBase {
private:
    int iUnit;

public:
    H2Leakage(const std::string& unit, std::shared_ptr<MyRedis> redis, std::shared_ptr<MyMQTT> MQTTCli, csv::CSVReader::iterator& it)
        : MechanismBase(unit, redis, MQTTCli, it)
        , iUnit { std::stoi(m_unit) - 1 }
    {
    }

    int logic() override
    {
        int flag { 0 };
        const std::string key { "FJS" + m_unit + ":Mechanism:H2_leakage" };
        const std::string content { "发电机漏氢" };
        const std::string now { get_now() };

        size_t m_H2Leakage_size { m_H2Leakage.size() };
        for (size_t i { 0 }; i < m_H2Leakage_size; ++i) {
            std::string tag = m_H2Leakage[i];
            const std::string st = m_redis->m_hget(key, tag);
            auto H2_leakage_opt = get_value_from_CSVRow<double>(m_c_df, tag);

            std::string tagSwitch = m_H2Leakage_switch[i];
            auto H2_leakage_switch_opt = get_value_from_CSVRow<double>(m_c_df, tagSwitch);

            bool has_switch_opt = H2_leakage_switch_opt.has_value();
            bool has_leakage_opt = H2_leakage_opt.has_value();
            
            if (!has_switch_opt && !has_leakage_opt) {
                continue;
            }

            // std::cout << "H2 leakage: " << H2_leakage_opt.value() << '\n';
            // std::cout << "H2 leakage switch: " << H2_leakage_switch_opt.value() << '\n';
            if ((has_switch_opt && H2_leakage_switch_opt.value() > 0) || (has_leakage_opt && H2_leakage_opt.value() > H2_LEAKAGE_LOW)) {
                trigger(key, tag, tag, content, st, now);
                flag = 1;
            } else {
                revert(key, tag, st);
            }
        }
        return flag;
    }
};

class GeneratorH2Leakage {
private:
    const double V; // generatorVolume m3
    const double gasFactor;
    const double ratedPressure;

public:
    GeneratorH2Leakage()
        : V { 73 }
        , gasFactor { (273 + 20) * 24 / 0.1 } // 20℃，0.1MPa
        , ratedPressure { 0.056 }
    {
    }

    double calculate()
    {
        double H = 12.0; // duration hours
        double P1 = 0.3015; // innerPressureStart Mpa
        double P2 = 0.30088; // innerPressureEnd
        double t1 = 35.2; // temperatureStart ℃
        double t2 = 35.8; // temperatureEnd
        double B1 = 0.086; // outerPressureStart Mpa
        double B2 = 0.085; // outerPressureEnd

        double leakage24 = gasFactor * V / H * ((P1 + B1) / (273 + t1) - (P2 + B2) / (273 + t2));
        return leakage24; // m3/d
    }

    int criteria(double leakage24, int medium)
    {
        double factor { 1 }; // air
        if (medium == 1) {
            factor = 3.75; // H2
        }
        int result;

        if (0.1 > ratedPressure) {
            if (leakage24 < 0.8 * factor)
                result = 3;
            else if (leakage24 < 0.9 * factor)
                result = 2;
            else if (leakage24 < 1.1 * factor)
                result = 1;
            else
                result = 0;
        }
        return result;
    }
};

class Task {
private:
    const std::string m_unit;
    std::shared_ptr<MyMQTT> m_MQTTCli;
    csv::CSVReader::iterator& m_it;

    const std::string H2_pressure_topic;
    const std::string water_flow_topic;
    const std::string diff_pressure_topic;
    const std::string H2_purity_topic;
    const std::string conductivity_topic;
    const std::string H2_leakage_topic;

    H2Pressure H2_pressure;
    WaterFlow water_flow;
    DiffPressure diff_pressure;
    H2Purity H2_purity;
    Conductivity conductivity;
    H2Leakage H2_leakage;
    GeneratorH2Leakage Generator_H2_leakage;

    template <typename T>
    void test(T& mechanism, const std::string& topic) const
    {
        int flag = mechanism.logic();
        std::cout << flag << '\n';
        if (flag == 1) {
            mechanism.send_message(topic);
        }
    }

    void show_points() const
    {
        csv::CSVRow& c_df { *m_it };
        const std::string jsonString = c_df.to_json();
        m_MQTTCli->publish(PROJECT_NAME + m_unit + "/Points", jsonString, QOS);
    }

    void calculate_leakage()
    {
        // speed = 100;
        double leakage24 = Generator_H2_leakage.calculate();
        int judgeCode = Generator_H2_leakage.criteria(leakage24, 1);

        std::string judgeStatus;
        switch (judgeCode) {
        case 0:
            judgeStatus = "不合格";
            break;
        case 1:
            judgeStatus = "合格";
            break;
        case 2:
            judgeStatus = "良好";
            break;
        case 3:
            judgeStatus = "优秀";
            break;
        default:
            judgeStatus = "未知";
            break;
        }

        json j;
        j["leakage24"] = leakage24;
        j["judge"] = judgeStatus;
        const std::string jsonString = j.dump();

        m_MQTTCli->publish(PROJECT_NAME + m_unit + "/Leakage", jsonString, QOS);
    }

public:
    Task(const std::string& unit, std::shared_ptr<MyRedis> redisCli, std::shared_ptr<MyMQTT> MQTTCli, csv::CSVReader::iterator& it)
        : m_unit { unit }
        , m_MQTTCli { MQTTCli }
        , m_it { it }
        , H2_pressure_topic { PROJECT_NAME + unit + "/Mechanism/H2Pressure" }
        , water_flow_topic { PROJECT_NAME + unit + "/Mechanism/WaterFlow" }
        , diff_pressure_topic { PROJECT_NAME + unit + "/Mechanism/DiffPressure" }
        , H2_purity_topic { PROJECT_NAME + unit + "/Mechanism/H2Purity" }
        , conductivity_topic { PROJECT_NAME + unit + "/Mechanism/Conductivity" }
        , H2_leakage_topic { PROJECT_NAME + unit + "/Mechanism/H2Leakage" }
        , H2_pressure { unit, redisCli, MQTTCli, it }
        , water_flow { unit, redisCli, MQTTCli, it }
        , diff_pressure { unit, redisCli, MQTTCli, it }
        , H2_purity { unit, redisCli, MQTTCli, it }
        , conductivity { unit, redisCli, MQTTCli, it }
        , H2_leakage { unit, redisCli, MQTTCli, it }
        , Generator_H2_leakage {}
    {
    }

    tf::Taskflow flow()
    {
        tf::Taskflow f1("F1");

        tf::Task f1A = f1.emplace([&]() {
                             test(H2_pressure, H2_pressure_topic);
                         }).name("test_H2_pressure");

        tf::Task f1B = f1.emplace([&]() {
                             test(water_flow, water_flow_topic);
                         }).name("test_water_flow");

        tf::Task f1C = f1.emplace([&]() {
                             test(diff_pressure, diff_pressure_topic);
                         }).name("test_diff_pressure");

        tf::Task f1D = f1.emplace([&]() {
                             test(H2_purity, H2_purity_topic);
                         }).name("test_H2_purity");

        tf::Task f1E = f1.emplace([&]() {
                             test(conductivity, conductivity_topic);
                         }).name("test_conductivity");

        tf::Task f1F = f1.emplace([&]() {
                             test(H2_leakage, H2_leakage_topic);
                         }).name("test_H2_leakage");
        
        tf::Task f1G = f1.emplace([&]() {
                             show_points();
                         }).name("show_points");

        tf::Task f1H = f1.emplace([&]() {
                             calculate_leakage();
                         }).name("calculate_leakage");

        return f1;
    }
};

int main()
{
    if (!fileExists(".env")) {
        throw std::runtime_error("File .env does not exist!");
    }

    dotenv::init();
    const std::string MQTT_ADDRESS { std::getenv("MQTT_ADDRESS") };
    const std::string MQTT_USERNAME { std::getenv("MQTT_USERNAME") };
    const std::string MQTT_PASSWORD { std::getenv("MQTT_PASSWORD") };
    const std::string MQTT_CA_CERTS { std::getenv("MQTT_CA_CERTS") };
    const std::string MQTT_CERTFILE { std::getenv("MQTT_CERTFILE") };
    const std::string MQTT_KEYFILE { std::getenv("MQTT_KEYFILE") };
    const std::string MQTT_KEYFILE_PASSWORD { std::getenv("MQTT_KEYFILE_PASSWORD") };
    const std::string CLIENT_ID { generate_random_string_with_hyphens() };

    const std::string REDIS_IP { std::getenv("REDIS_IP") };
    const int REDIS_PORT = std::atoi(std::getenv("REDIS_PORT"));
    const int REDIS_DB = std::atoi(std::getenv("REDIS_DB"));
    const std::string REDIS_USER { std::getenv("REDIS_USER") };
    const std::string REDIS_PASSWORD { std::getenv("REDIS_PASSWORD") };

    auto MQTTCli = std::make_shared<MyMQTT>(MQTT_ADDRESS, CLIENT_ID, MQTT_USERNAME, MQTT_PASSWORD,
        MQTT_CA_CERTS, MQTT_CERTFILE, MQTT_KEYFILE, MQTT_KEYFILE_PASSWORD);

    auto redisCli = std::make_shared<MyRedis>(REDIS_IP, REDIS_PORT, REDIS_DB, REDIS_USER, REDIS_PASSWORD);

    const std::string unit1 { "1" };
    const int iUnit1 { std::stoi(unit1) - 1 };
    csv::CSVReader reader("test.csv");
    std::array<csv::CSVReader::iterator, 2> it {};
    for (it[iUnit1] = reader.begin(); it[iUnit1] != reader.end(); ++it[iUnit1]) {
    }
    
    Task task1(unit1, redisCli, MQTTCli, it[iUnit1]);

    tf::Executor executor;
    long long count { 0 };
    tf::Taskflow f { task1.flow() };

    while (1) {
        auto start = std::chrono::steady_clock::now();

        executor.run(f).wait();
        // executor.run(task2.flow()).wait();

        auto end = std::chrono::steady_clock::now();
        auto elapsed_time = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        std::cout << "Loop " << ++count << " time used: " << elapsed_time.count() << " microseconds\n";
        std::this_thread::sleep_for(std::chrono::microseconds(INTERVAL - elapsed_time.count()));
    }

    return 0;
}