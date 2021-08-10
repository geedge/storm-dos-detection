package com.zdjizhi.bolt;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosSketchLog;
import io.vavr.Tuple4;
import org.apache.storm.Config;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;
import java.util.Map;

/**
 * @author wlh
 */
public class MiddleStreamBolt extends BaseWindowedBolt {
    private static final Log logger = LogFactory.get();
    private OutputCollector collector;
    private Map<Tuple4<String, String, String, String>,DosSketchLog> middleStreamResult;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        middleStreamResult = new HashMap<>(4);
        this.collector = collector;
    }

    @Override
    public void execute(TupleWindow inputWindow) {
        try {
            sketchAggregate(inputWindow);
            for (Tuple4<String, String, String, String> keys:middleStreamResult.keySet()){
                DosSketchLog sketchLog = middleStreamResult.get(keys);
                collector.emit(new Values(keys._3,keys._4,sketchLog));
            }
            logger.debug("中间结果计算完毕,共计: {}条",middleStreamResult.size());
            middleStreamResult.clear();
        }catch (Exception e){
            logger.error("处理中间结果异常 {}",e);
        }
    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS,(CommonConfig.STORM_WINDOW_MAX_TIME+60)*1000*2);
        conf.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT,Integer.MAX_VALUE);
        conf.put(Config.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS,CommonConfig.STORM_WINDOW_MAX_TIME*1000);
        return conf;
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("attack_type","destination_ip","middle_sketch_log"));
    }

    private void sketchAggregate(TupleWindow inputWindow){
        try {
            for (Tuple tuple:inputWindow.get()){
                DosSketchLog tmpSketchLog = (DosSketchLog) tuple.getValueByField("sketch_source");
                Tuple4<String, String, String, String> keys = io.vavr.Tuple.of(
                        tuple.getStringByField("common_sled_ip"),
                        tuple.getStringByField("common_data_center"),
                        tuple.getStringByField("attack_type"),
                        tuple.getStringByField("destination_ip")
                );
                if (middleStreamResult.containsKey(keys)){
                    DosSketchLog sketchLog = middleStreamResult.get(keys);
                    StreamAggregationHandler.mergeSketch(sketchLog,tmpSketchLog);
                    middleStreamResult.replace(keys,sketchLog);
                }else {
                    StreamAggregationHandler.initSketch(tmpSketchLog);
                    middleStreamResult.put(keys,tmpSketchLog);
                }
                logger.debug("处理当前keys: {},数据: {},结果集大小: {}",keys,tmpSketchLog,middleStreamResult.size());
            }
        }catch (Exception e){
            logger.error("中间结果聚合失败 {}",e);
        }
    }

}
