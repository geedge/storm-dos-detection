package com.zdjizhi.bolt;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.fasterxml.jackson.databind.JavaType;
import com.zdjizhi.common.DosSketchLog;
import com.zdjizhi.utils.JsonMapper;
import com.zdjizhi.utils.StringUtil;
import com.zdjizhi.utils.TupleUtils;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author wlh
 */
public class ParseSketchLogBolt extends BaseBasicBolt {
    private static final long serialVersionUID = 9006119186526123734L;
    private static final Log logger = LogFactory.get();
    private static JsonMapper jsonMapperInstance = JsonMapper.getInstance();
    private static JavaType hashmapJsonType = jsonMapperInstance.createCollectionType(HashMap.class, String.class, Object.class);
    private static JavaType listType = jsonMapperInstance.createCollectionType(ArrayList.class, HashMap.class);


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        try {
            if (!TupleUtils.isTick(input)){
                String message = input.getString(0);
                if (StringUtil.isNotBlank(message)) {
                    flatSketchLog(message,collector);
                }
            }
        } catch (Exception e) {
            logger.error("原始日志下发过程异常,异常信息：{}",e);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("attack_type","destination_ip","sketch_source"));
    }

    private void flatSketchLog(String message, BasicOutputCollector collector){
        try {
            HashMap<String, Object> sketchSource = jsonMapperInstance.fromJson(message, hashmapJsonType);
            String commonSledIp = sketchSource.get("common_sled_ip").toString();
            String commonDataCenter = sketchSource.get("common_data_center").toString();
            long sketchStartTime = Long.parseLong(sketchSource.get("sketch_start_time").toString());
            long sketchDuration = Long.parseLong(sketchSource.get("sketch_duration").toString());
            String attackType = sketchSource.get("attack_type").toString();
            ArrayList<HashMap<String, Object>> reportIpList = jsonMapperInstance.fromJson(jsonMapperInstance.toJson(sketchSource.get("report_ip_list")), listType);
            for (HashMap<String, Object> obj : reportIpList) {
                DosSketchLog dosSketchLog = new DosSketchLog();
                dosSketchLog.setCommon_sled_ip(commonSledIp);
                dosSketchLog.setCommon_data_center(commonDataCenter);
                dosSketchLog.setSketch_start_time(sketchStartTime);
                dosSketchLog.setSketch_duration(sketchDuration);
                dosSketchLog.setAttack_type(attackType);
                String sourceIp = obj.get("source_ip").toString();
                String destinationIp = obj.get("destination_ip").toString();
                long sketchSessions = Long.parseLong(obj.get("sketch_sessions").toString());
                long sketchPackets = Long.parseLong(obj.get("sketch_packets").toString());
                long sketchBytes = Long.parseLong(obj.get("sketch_bytes").toString());
                dosSketchLog.setSource_ip(sourceIp);
                dosSketchLog.setDestination_ip(destinationIp);
                dosSketchLog.setSketch_sessions(sketchSessions / sketchDuration);
                dosSketchLog.setSketch_packets(sketchPackets / sketchDuration);
                dosSketchLog.setSketch_bytes(sketchBytes * 8 / sketchDuration);
                collector.emit(new Values(attackType,destinationIp,dosSketchLog));
                logger.debug("数据解析成功：{}", dosSketchLog.toString());
            }
        }catch (Exception e){
            logger.error("数据解析错误：{} \n{}",message,e);
        }
    }

}
