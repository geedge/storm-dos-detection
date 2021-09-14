package com.zdjizhi.bolt;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosSketchLog;
import com.zdjizhi.utils.TupleUtils;
import io.vavr.Tuple2;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wlh
 */
public class MiddleStreamBolt extends BaseBasicBolt {
    private static final Log logger = LogFactory.get();
    private Map<Tuple2<String, String>, DosSketchLog> middleStreamResult;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        middleStreamResult = new HashMap<>(4);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            if (TupleUtils.isTick(input)) {
                for (Tuple2<String, String> keys : middleStreamResult.keySet()) {
                    DosSketchLog sketchLog = middleStreamResult.get(keys);
                    sketchLog.setSketch_sessions(sketchLog.getSketch_sessions()/CommonConfig.STORM_WINDOW_MAX_TIME);
                    sketchLog.setSketch_packets(sketchLog.getSketch_packets()/CommonConfig.STORM_WINDOW_MAX_TIME);
                    sketchLog.setSketch_bytes(sketchLog.getSketch_bytes()*8/CommonConfig.STORM_WINDOW_MAX_TIME);
                    sketchLog.setSource_ip(StringUtils.join(sketchLog.getSource_ip_list(), ","));
                    collector.emit(new Values(sketchLog));
                }
                logger.debug("中间结果计算完毕,共计: {}条", middleStreamResult.size());
                middleStreamResult.clear();
            } else {
                sketchAggregate(input);
            }
        } catch (Exception e) {
            logger.error("处理中间结果异常 {}", e);
        }
    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, CommonConfig.STORM_WINDOW_MAX_TIME);
        return conf;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("middle_sketch_log"));
    }

    private void sketchAggregate(Tuple tuple) {
        try {
            DosSketchLog tmpSketchLog = (DosSketchLog) tuple.getValueByField("sketch_source");
            Tuple2<String, String> keys = io.vavr.Tuple.of(
                    tuple.getStringByField("attack_type"),
                    tuple.getStringByField("destination_ip")
            );
            if (middleStreamResult.containsKey(keys)) {
                DosSketchLog sketchLog = middleStreamResult.get(keys);
                StreamAggregationHandler.mergeSketch(sketchLog, tmpSketchLog);
                middleStreamResult.replace(keys, sketchLog);
            } else {
                StreamAggregationHandler.initSketch(tmpSketchLog);
                middleStreamResult.put(keys, tmpSketchLog);
            }
            logger.debug("处理当前keys: {},数据: {},结果集大小: {}", keys, tmpSketchLog, middleStreamResult.size());

        } catch (Exception e) {
            logger.error("中间结果聚合失败 {}", e);
        }
    }

}
