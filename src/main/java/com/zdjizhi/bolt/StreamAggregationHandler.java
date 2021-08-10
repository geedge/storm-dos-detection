package com.zdjizhi.bolt;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosSketchLog;

import java.util.HashSet;

class StreamAggregationHandler {
    private static final Log logger = LogFactory.get();

    static void mergeSketch(DosSketchLog sketchLog, DosSketchLog tmpSketchLog) throws Exception{
        setAverage(sketchLog,tmpSketchLog);
        updateDistinctSourceIp(sketchLog,tmpSketchLog);
    }

    static void initSketch(DosSketchLog tmpSketchLog) throws Exception{
        initDistinctSourceIp(tmpSketchLog);
    }

    private static void setAverage(DosSketchLog sketchLog, DosSketchLog tmpSketchLog) throws Exception{
        sketchLog.setSketch_sessions((sketchLog.getSketch_sessions()+tmpSketchLog.getSketch_sessions())/2);
        sketchLog.setSketch_packets((sketchLog.getSketch_packets()+tmpSketchLog.getSketch_packets())/2);
        sketchLog.setSketch_bytes((sketchLog.getSketch_bytes()+tmpSketchLog.getSketch_bytes())/2);
    }

    private static void initDistinctSourceIp(DosSketchLog tmpSketchLog){
        try {
            HashSet<String> sourceIpList = new HashSet<>();
            sourceIpList.add(tmpSketchLog.getSource_ip());
            tmpSketchLog.setSource_ip_list(sourceIpList);
            logger.debug("{} 初始化distinct source IP成功",tmpSketchLog);
        }catch (Exception e){
            logger.error("初始化distinct source IP失败: {}\n",tmpSketchLog,e);
        }
    }

    private static void updateDistinctSourceIp(DosSketchLog sketchLog, DosSketchLog tmpSketchLog){
        try {
            HashSet<String> sourceIpList = sketchLog.getSource_ip_list();
            if (sourceIpList.size() < CommonConfig.SOURCE_IP_LIST_LIMIT){
                sourceIpList.add(tmpSketchLog.getSource_ip());
                sketchLog.setSource_ip_list(sourceIpList);
            }
            logger.debug("更新distinct source IP成功,新数据: {}",sketchLog);
        }catch (Exception e){
            logger.error("更新distinct source IP失败,历史数据 :{}\n新数据: {}\n",sketchLog,tmpSketchLog,e);
        }
    }
}
