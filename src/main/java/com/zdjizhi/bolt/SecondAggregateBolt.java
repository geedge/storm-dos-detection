package com.zdjizhi.bolt;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosSketchLog;
import com.zdjizhi.utils.TupleUtils;
import io.vavr.Tuple2;
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
import java.util.Set;

public class SecondAggregateBolt extends BaseBasicBolt {
    private static final Log logger = LogFactory.get();
    private Map<Tuple2<String, String>, DosSketchLog> streamAggregationSketch;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        streamAggregationSketch = new HashMap<>();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            if (TupleUtils.isTick(input)){
                if (!streamAggregationSketch.isEmpty()){
                    Set<Tuple2<String, String>> keySet = streamAggregationSketch.keySet();
                    for (Tuple2<String, String> key:keySet){
                        collector.emit(new Values(streamAggregationSketch.get(key)));
                    }
                    logger.debug("完成二次聚合,共处理{}条",streamAggregationSketch.size());
                    streamAggregationSketch.clear();
                }
            }else {
                secondAggregate(input);
            }
        }catch (Exception e){
            logger.error("二次聚合过程存在异常 ",e);
        }
    }

    private void secondAggregate(Tuple input){
        try {
            DosSketchLog tmpSketchLog = (DosSketchLog) input.getValueByField("middle_sketch_log");
            Tuple2<String, String> keys = io.vavr.Tuple.of(input.getStringByField("attack_type"), input.getStringByField("destination_ip"));
            if (streamAggregationSketch.containsKey(keys)){
                DosSketchLog sketchLog = streamAggregationSketch.get(keys);
                StreamAggregationHandler.mergeSketch(sketchLog,tmpSketchLog);
            }else {
                streamAggregationSketch.put(keys,tmpSketchLog);
            }
            logger.debug("处理当前keys: {},数据: {},结果集大小: {}",keys,tmpSketchLog,streamAggregationSketch.size());
        }catch (Exception e){
            logger.error("二次聚合失败 ",e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("second_sketch_log"));
    }
}
