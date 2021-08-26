package com.zdjizhi.topology;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.bolt.*;
import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.spout.CustomizedKafkaSpout;
import org.apache.storm.Config;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Storm程序主类
 *
 * @author wlh
 */
public class LogFlowWriteTopology {
    private static final Log logger = LogFactory.get();
    private final String topologyName;
    private final Config topologyConfig;
    private TopologyBuilder builder;

    private LogFlowWriteTopology() {
        this(LogFlowWriteTopology.class.getSimpleName());
    }

    private LogFlowWriteTopology(String topologyName) {
        this.topologyName = topologyName;
        topologyConfig = createTopologyConfig();
    }

    private Config createTopologyConfig() {
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMessageTimeoutSecs(60);
        conf.setMaxSpoutPending(CommonConfig.TOPOLOGY_CONFIG_MAX_SPOUT_PENDING);
        conf.setNumAckers(CommonConfig.TOPOLOGY_NUM_ACKS);
        return conf;
    }

    private void runLocally() throws InterruptedException {
        topologyConfig.setMaxTaskParallelism(1);
        topologyConfig.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3);
        StormRunner.runTopologyLocally(builder, topologyName, topologyConfig, 600);
    }

    private void runRemotely() throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        topologyConfig.setNumWorkers(CommonConfig.TOPOLOGY_WORKERS);
        topologyConfig.put(Config.TOPOLOGY_TRANSFER_BUFFER_SIZE, CommonConfig.TRANSFER_BUFFER_SIZE);
        topologyConfig.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, CommonConfig.EXECUTOR_RECEIVE_BUFFER_SIZE);
        topologyConfig.put(Config.TOPOLOGY_EXECUTOR_SEND_BUFFER_SIZE, CommonConfig.EXECUTOR_SEND_BUFFER_SIZE);
        topologyConfig.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3);
        StormRunner.runTopologyRemotely(builder, topologyName, topologyConfig);
    }

    private void buildTopology() {
        builder = new TopologyBuilder();
        builder.setSpout("SketchLogSpout", new CustomizedKafkaSpout(), CommonConfig.KAFKA_INPUT_PARALLELISM);
        builder.setBolt("ParseSketchLogBolt", new ParseSketchLogBolt(), CommonConfig.KAFKA_INPUT_PARALLELISM)
                .localOrShuffleGrouping("SketchLogSpout");
        builder.setBolt("MiddleStreamBolt", new MiddleStreamBolt())
                .fieldsGrouping("ParseSketchLogBolt", new Fields("attack_type", "destination_ip"));
        builder.setBolt("ServerIpMetricsBolt", new ServerIpMetricsBolt(), CommonConfig.KAFKA_OUTPUT_METRIC_PARALLELISM)
                .localOrShuffleGrouping("MiddleStreamBolt");
        builder.setBolt("DosDetectionBolt", new DosDetectionBolt(), CommonConfig.KAFKA_OUTPUT_EVENT_PARALLELISM)
                .localOrShuffleGrouping("MiddleStreamBolt");
    }

    public static void main(String[] args) {
        LogFlowWriteTopology flowWriteTopology;
        try {
            boolean runLocally = true;
            int size = 2;
            if (args.length >= size && CommonConfig.MODEL.equalsIgnoreCase(args[1])) {
                runLocally = false;
                flowWriteTopology = new LogFlowWriteTopology(args[0]);
            } else {
                flowWriteTopology = new LogFlowWriteTopology();
            }

            flowWriteTopology.buildTopology();

            if (runLocally) {
                logger.info("执行本地模式...");
                flowWriteTopology.runLocally();
            } else {
                logger.info("执行远程部署模式...");
                flowWriteTopology.runRemotely();
            }
        } catch (RuntimeException | InterruptedException | InvalidTopologyException | AlreadyAliveException | AuthorizationException e) {
            logger.error("Topology Start ERROR! message is:" + e);
        }
    }
}
