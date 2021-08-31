package com.zdjizhi.bolt;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosMetricsLog;
import com.zdjizhi.common.DosSketchLog;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.KafkaUtils;
import com.zdjizhi.utils.StringUtil;
import com.zdjizhi.utils.TupleUtils;
import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author wlh
 */
public class ServerIpMetricsBolt extends BaseBasicBolt {

    private static final Log logger = LogFactory.get();
    private KafkaUtils kafkaLogSend;
    private List<String> list = new LinkedList<>();

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        kafkaLogSend = KafkaUtils.getInstance();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            if (TupleUtils.isTick(input)) {
                if (list.size() != 0) {
                    kafkaLogSend.sendMessage(list, CommonConfig.KAFKA_OUTPUT_METRIC_TOPIC_NAME);
                    logger.info("发送 {} {}条成功",CommonConfig.KAFKA_OUTPUT_METRIC_TOPIC_NAME,list.size());
                    list.clear();
                }
            } else {
                DosSketchLog tmpSketchLog = (DosSketchLog) input.getValueByField("middle_sketch_log");
                String message = JsonMapper.toJsonString(transSketchToMetric(tmpSketchLog));
                if (StringUtil.isNotBlank(message)) {
                    list.add(message);
                }
                if (list.size() >= CommonConfig.KAFKA_OUTPUT_METRIC_BATCH_NUM) {
                    kafkaLogSend.sendMessage(list,CommonConfig.KAFKA_OUTPUT_METRIC_TOPIC_NAME);
                    logger.info("发送 {} {}条成功",CommonConfig.KAFKA_OUTPUT_METRIC_TOPIC_NAME,list.size());
                    list.clear();
                }
            }
        } catch (RuntimeException e) {
            logger.error("dos metrics日志发送Kafka过程出现异常 ",e);
        }
    }

    private DosMetricsLog transSketchToMetric(DosSketchLog tmpSketchLog){
        DosMetricsLog dosMetricsLog = new DosMetricsLog();
        try {
            dosMetricsLog.setSketch_start_time(tmpSketchLog.getSketch_start_time());
            dosMetricsLog.setDestination_ip(tmpSketchLog.getDestination_ip());
            dosMetricsLog.setAttack_type(tmpSketchLog.getAttack_type());
            dosMetricsLog.setSession_rate(tmpSketchLog.getSketch_sessions());
            dosMetricsLog.setPacket_rate(tmpSketchLog.getSketch_packets());
            dosMetricsLog.setBit_rate(tmpSketchLog.getSketch_bytes());
            dosMetricsLog.setPartition_num(getPartitionNumByIp(tmpSketchLog.getDestination_ip()));
            logger.debug("生成dos metrics日志成功 {}",dosMetricsLog);
        }catch (Exception e){
            logger.error("生成dos metrics日志异常 ",e);
        }
        return dosMetricsLog;
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3);
        return conf;
    }


    private static int getPartitionNumByIp(String destinationIp){
        return Math.abs(destinationIp.hashCode()) % CommonConfig.DESTINATION_IP_PARTITION_NUM;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
