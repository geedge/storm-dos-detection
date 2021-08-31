package com.zdjizhi.common;

import com.zdjizhi.utils.CommonConfigurations;

public class CommonConfig {

    public static final String MODEL = "remote";

    public static final int KAFKA_INPUT_PARALLELISM = CommonConfigurations.getIntProperty("kafka.input.parallelism");
    public static final String KAFKA_INPUT_TOPIC_NAME = CommonConfigurations.getStringProperty("kafka.input.topic.name");
    public static final String KAFKA_INPUT_BOOTSTRAP_SERVERS = CommonConfigurations.getStringProperty("kafka.input.bootstrap.servers");
    public static final String KAFKA_SCAN_STARTUP_MODE = CommonConfigurations.getStringProperty("kafka.input.scan.startup.mode");
    public static final String KAFKA_GROUP_ID = CommonConfigurations.getStringProperty("kafka.input.group.id");

    public static final int KAFKA_OUTPUT_METRIC_PARALLELISM = CommonConfigurations.getIntProperty("kafka.output.metric.parallelism");
    public static final String KAFKA_OUTPUT_METRIC_TOPIC_NAME = CommonConfigurations.getStringProperty("kafka.output.metric.topic.name");
    public static final int KAFKA_OUTPUT_EVENT_PARALLELISM = CommonConfigurations.getIntProperty("kafka.output.event.parallelism");
    public static final String KAFKA_OUTPUT_EVENT_TOPIC_NAME = CommonConfigurations.getStringProperty("kafka.output.event.topic.name");
    public static final String KAFKA_OUTPUT_BOOTSTRAP_SERVERS = CommonConfigurations.getStringProperty("kafka.output.bootstrap.servers");

    public static final int KAFKA_OUTPUT_METRIC_BATCH_NUM = CommonConfigurations.getIntProperty("kafka.output.metric.batch.num");
    public static final int KAFKA_OUTPUT_EVENT_BATCH_NUM = CommonConfigurations.getIntProperty("kafka.output.event.batch.num");

    public static final String HBASE_ZOOKEEPER_QUORUM = CommonConfigurations.getStringProperty("hbase.zookeeper.quorum");
    public static final int HBASE_CLIENT_OPERATION_TIMEOUT = CommonConfigurations.getIntProperty("hbase.client.operation.timeout");
    public static final int HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD = CommonConfigurations.getIntProperty("hbase.client.scanner.timeout.period");

    public static final String HBASE_BASELINE_TABLE_NAME = CommonConfigurations.getStringProperty("hbase.baseline.table.name");
    public static final int HBASE_BASELINE_TOTAL_NUM = CommonConfigurations.getIntProperty("hbase.baseline.total.num");

    public static final int STORM_WINDOW_MAX_TIME = CommonConfigurations.getIntProperty("storm.window.max.time");

    public static final int SOURCE_IP_LIST_LIMIT = CommonConfigurations.getIntProperty("source.ip.list.limit");
    public static final int DATA_CENTER_ID_NUM = CommonConfigurations.getIntProperty("data.center.id.num");

    public static final String IP_MMDB_PATH = CommonConfigurations.getStringProperty("ip.mmdb.path");

    public static final double BASELINE_SESSIONS_MINOR_THRESHOLD = CommonConfigurations.getDoubleProperty("baseline.sessions.minor.threshold");
    public static final double BASELINE_SESSIONS_WARNING_THRESHOLD = CommonConfigurations.getDoubleProperty("baseline.sessions.warning.threshold");
    public static final double BASELINE_SESSIONS_MAJOR_THRESHOLD = CommonConfigurations.getDoubleProperty("baseline.sessions.major.threshold");
    public static final double BASELINE_SESSIONS_SEVERE_THRESHOLD = CommonConfigurations.getDoubleProperty("baseline.sessions.severe.threshold");
    public static final double BASELINE_SESSIONS_CRITICAL_THRESHOLD = CommonConfigurations.getDoubleProperty("baseline.sessions.critical.threshold");

    public static final int BASELINE_THRESHOLD_SCHEDULE_DAYS = CommonConfigurations.getIntProperty("baseline.threshold.schedule.days");

    public static final int DESTINATION_IP_PARTITION_NUM = CommonConfigurations.getIntProperty("destination.ip.partition.num");

    public static final int TOPOLOGY_SPOUT_SLEEP_TIME = CommonConfigurations.getIntProperty("topology.spout.sleep.time");

    public static final Integer TOPOLOGY_WORKERS = CommonConfigurations.getIntProperty("topology.workers");

    public static final String RETRIES = CommonConfigurations.getStringProperty( "retries");
    public static final String LINGER_MS = CommonConfigurations.getStringProperty( "linger.ms");
    public static final Integer REQUEST_TIMEOUT_MS = CommonConfigurations.getIntProperty( "request.timeout.ms");
    public static final Integer BATCH_SIZE = CommonConfigurations.getIntProperty("batch.size");
    public static final Integer BUFFER_MEMORY = CommonConfigurations.getIntProperty("buffer.memory");
    public static final Integer MAX_REQUEST_SIZE = CommonConfigurations.getIntProperty("max.request.size");
    public static final Integer TRANSFER_BUFFER_SIZE = CommonConfigurations.getIntProperty("transfer_buffer_size");
    public static final Integer EXECUTOR_RECEIVE_BUFFER_SIZE = CommonConfigurations.getIntProperty( "executor_receive_buffer_size");
    public static final Integer EXECUTOR_SEND_BUFFER_SIZE = CommonConfigurations.getIntProperty("executor_send_buffer_size");

    public static final Integer TOPOLOGY_CONFIG_MAX_SPOUT_PENDING = CommonConfigurations.getIntProperty("topology.config.max.spout.pending");
    public static final Integer TOPOLOGY_NUM_ACKS = CommonConfigurations.getIntProperty("topology.num.acks");
    public static final String PRODUCER_ACK = CommonConfigurations.getStringProperty("producer.ack");

    public static final String BIFANG_SERVER_URI = CommonConfigurations.getStringProperty("bifang.server.uri");
    public static final String BIFANG_SERVER_USER = CommonConfigurations.getStringProperty("bifang.server.user");
    public static final String BIFANG_SERVER_PASSWORD = CommonConfigurations.getStringProperty("bifang.server.password");
    public static final String BIFANG_SERVER_ENCRYPTPWD_PATH = CommonConfigurations.getStringProperty("bifang.server.encryptpwd.path");
    public static final String BIFANG_SERVER_LOGIN_PATH = CommonConfigurations.getStringProperty("bifang.server.login.path");
    public static final String BIFANG_SERVER_POLICY_THRESHOLD_PATH = CommonConfigurations.getStringProperty("bifang.server.policy.threshold.path");

    public static final int HTTP_POOL_MAX_CONNECTION = CommonConfigurations.getIntProperty("http.pool.max.connection");
    public static final int HTTP_POOL_MAX_PER_ROUTE = CommonConfigurations.getIntProperty("http.pool.max.per.route");
    public static final int HTTP_POOL_REQUEST_TIMEOUT = CommonConfigurations.getIntProperty("http.pool.request.timeout");
    public static final int HTTP_POOL_CONNECT_TIMEOUT = CommonConfigurations.getIntProperty("http.pool.connect.timeout");
    public static final int HTTP_POOL_RESPONSE_TIMEOUT = CommonConfigurations.getIntProperty("http.pool.response.timeout");


}
