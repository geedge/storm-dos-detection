package com.zdjizhi.bolt;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.google.common.collect.TreeRangeMap;
import com.zdjizhi.common.CommonConfig;
import com.zdjizhi.common.DosDetectionThreshold;
import com.zdjizhi.common.DosEventLog;
import com.zdjizhi.common.DosSketchLog;
import com.zdjizhi.utils.*;
import inet.ipaddr.IPAddress;
import inet.ipaddr.IPAddressString;
import io.vavr.Tuple2;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.math.BigDecimal;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author wlh
 */
public class DosDetectionBolt extends BaseBasicBolt {

    private static final Log logger = LogFactory.get();
    private final static int BASELINE_SIZE = 144;
    private KafkaUtils kafkaLogSend;
    private final static NumberFormat PERCENT_INSTANCE = NumberFormat.getPercentInstance();
    private static Map<String, Map<String, Tuple2<ArrayList<Integer>, Integer>>> baselineMap = new HashMap<>();
    private TreeRangeMap<IPAddress, Map<String, DosDetectionThreshold>> thresholdRangeMap;
    private List<String> list = new LinkedList<>();

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(2,
                new BasicThreadFactory.Builder().namingPattern("Dos-Detection-%d").daemon(true).build());
        try {
            executorService.scheduleAtFixedRate(() -> thresholdRangeMap = ParseStaticThreshold.createStaticThreshold(),
                    0, CommonConfig.STATIC_THRESHOLD_SCHEDULE_MINUTES, TimeUnit.MINUTES);

            executorService.scheduleAtFixedRate(() -> baselineMap = HbaseUtils.readFromHbase(),
                    0, CommonConfig.BASELINE_THRESHOLD_SCHEDULE_DAYS, TimeUnit.DAYS);
        } catch (Exception e) {
            logger.error("定时器任务执行失败", e);
        }
        PERCENT_INSTANCE.setMinimumFractionDigits(2);
        kafkaLogSend = KafkaUtils.getInstance();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3);
        return conf;
    }


    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if (TupleUtils.isTick(input)) {
            if (list.size() != 0) {
                kafkaLogSend.sendMessage(list, CommonConfig.KAFKA_OUTPUT_EVENT_TOPIC_NAME);
                logger.info("发送 {} {}条成功", CommonConfig.KAFKA_OUTPUT_EVENT_TOPIC_NAME, list.size());
                list.clear();
            }
        } else {
            DosSketchLog sketchLog = (DosSketchLog) input.getValueByField("middle_sketch_log");
            DosEventLog dosEventLog = dosDetection(sketchLog);
            String message = JsonMapper.toJsonString(dosEventLog);
            if (dosEventLog != null && StringUtil.isNotBlank(message)) {
                list.add(message);
                logger.info("dos detection log:{} \n{}",message,dosEventLog);
            }
            if (list.size() >= CommonConfig.KAFKA_OUTPUT_EVENT_BATCH_NUM) {
                kafkaLogSend.sendMessage(list, CommonConfig.KAFKA_OUTPUT_EVENT_TOPIC_NAME);
                logger.info("发送 {} {}条成功", CommonConfig.KAFKA_OUTPUT_EVENT_TOPIC_NAME, list.size());
                list.clear();
            }
        }

    }

    private DosEventLog dosDetection(DosSketchLog value) {
        DosEventLog finalResult = null;
        try {
            String destinationIp = value.getDestination_ip();
            String attackType = value.getAttack_type();
            long sketchSessions = value.getSketch_sessions();
            IPAddress destinationIpAddress = new IPAddressString(destinationIp).getAddress();
            Map<String, DosDetectionThreshold> thresholdMap = thresholdRangeMap.get(destinationIpAddress);
            logger.debug("当前判断IP：{}, 类型: {}", destinationIp, attackType);
            if (sketchSessions > CommonConfig.SENSITIVITY_THRESHOLD){
                if (baselineMap != null && baselineMap.containsKey(destinationIp) && thresholdMap == null) {
                    finalResult = getDosEventLogByBaseline(value, destinationIp, attackType)._2;
                } else if (baselineMap != null && !baselineMap.containsKey(destinationIp) && thresholdMap != null) {
                    finalResult = getDosEventLogByStaticThreshold(value, thresholdMap)._2;
                } else if (baselineMap != null && baselineMap.containsKey(destinationIp) && thresholdMap != null) {
                    Tuple2<Severity, DosEventLog> eventLogByBaseline = getDosEventLogByBaseline(value, destinationIp, attackType);
                    Tuple2<Severity, DosEventLog> eventLogByStaticThreshold = getDosEventLogByStaticThreshold(value, thresholdMap);
                    finalResult = mergeFinalResult(eventLogByBaseline, eventLogByStaticThreshold);
                } else {
                    logger.debug("未获取到当前server IP：{} 类型 {} 静态阈值 和 baseline", destinationIp, attackType);
                }
            }
        } catch (Exception e) {
            logger.error("判定失败\n {} \n{}", value, e);
        }
        return finalResult;
    }

    private DosEventLog mergeFinalResult(Tuple2<Severity, DosEventLog> eventLogByBaseline, Tuple2<Severity, DosEventLog> eventLogByStaticThreshold) {
        if (eventLogByBaseline._1.score > eventLogByStaticThreshold._1.score) {
            mergeCondition(eventLogByBaseline._2, eventLogByStaticThreshold._2);
            logger.info("merge eventLogByBaseline {} \neventLogByStaticThreshold {}",eventLogByBaseline,eventLogByStaticThreshold);
            return eventLogByBaseline._2;
        } else {
            mergeCondition(eventLogByStaticThreshold._2, eventLogByBaseline._2);
            logger.info("merge eventLogByStaticThreshold {} \neventLogByBaseline {}",eventLogByStaticThreshold,eventLogByBaseline);
            return eventLogByStaticThreshold._2;
        }
    }

    private void mergeCondition(DosEventLog log1, DosEventLog log2) {
        if (log1 != null && log2 != null) {
            String conditions1 = log1.getConditions();
            String conditions2 = log2.getConditions();
            log1.setConditions(conditions1 + " and " + conditions2);
        }
    }

    private Tuple2<Severity, DosEventLog> getDosEventLogByBaseline(DosSketchLog value, String destinationIp, String attackType) {
        Tuple2<ArrayList<Integer>, Integer> floodTypeTup = baselineMap.get(destinationIp).get(attackType);
        Integer base = getBaseValue(floodTypeTup, value);
        long diff = value.getSketch_sessions() - base;
        return getDosEventLog(value, base, diff, "baseline");
    }

    private Tuple2<Severity, DosEventLog> getDosEventLogByStaticThreshold(DosSketchLog value, Map<String, DosDetectionThreshold> thresholdMap) {
        Tuple2<Severity, DosEventLog> result = io.vavr.Tuple.of(Severity.NORMAL, null);
        String attackType = value.getAttack_type();
        if (thresholdMap.containsKey(attackType)) {
            DosDetectionThreshold threshold = thresholdMap.get(attackType);
            long base = threshold.getSessionsPerSec();
            long diff = value.getSketch_sessions() - base;
            result = getDosEventLog(value, base, diff, "static");
        }
        return result;
    }

    private Tuple2<Severity, DosEventLog> getDosEventLog(DosSketchLog value, long base, long diff, String tag) {
        DosEventLog result = null;
        String destinationIp = value.getDestination_ip();
        String attackType = value.getAttack_type();
        Severity severity = Severity.NORMAL;
        if (diff > 0 && base != 0) {
            double percent = getDiffPercent(diff, base);
            severity = judgeSeverity(percent);
            if (severity != Severity.NORMAL) {
                result = getResult(value, severity, percent, tag);
                logger.info("检测到当前server IP {} 存在 {} 异常,超出基线{} {}倍,日志详情\n {}", destinationIp,attackType,base,percent,result);
            } else {
                logger.debug("当前server IP：{} 未出现 {} 异常,日志详情 {}", destinationIp, attackType, value.toString());
            }
        }
        return io.vavr.Tuple.of(severity, result);
    }

    private DosEventLog getResult(DosSketchLog value, Severity severity, double percent, String tag) {
        DosEventLog dosEventLog = new DosEventLog();
        dosEventLog.setLog_id(SnowflakeId.generateId());
        dosEventLog.setStart_time(value.getSketch_start_time());
        dosEventLog.setEnd_time(value.getSketch_start_time() + CommonConfig.STORM_WINDOW_MAX_TIME);
        dosEventLog.setAttack_type(value.getAttack_type());
        dosEventLog.setSeverity(severity.severity);
        dosEventLog.setConditions(getConditions(PERCENT_INSTANCE.format(percent), value.getSketch_sessions(), tag));
        dosEventLog.setDestination_ip(value.getDestination_ip());
        dosEventLog.setDestination_country(IpUtils.ipLookup.countryLookup(value.getDestination_ip()));
        String ipList = value.getSource_ip();
        dosEventLog.setSource_ip_list(ipList);
        dosEventLog.setSource_country_list(getSourceCountryList(ipList));
        dosEventLog.setSession_rate(value.getSketch_sessions());
        dosEventLog.setPacket_rate(value.getSketch_packets());
        dosEventLog.setBit_rate(value.getSketch_bytes());
        return dosEventLog;
    }

    private Integer getBaseValue(Tuple2<ArrayList<Integer>, Integer> floodTypeTup, DosSketchLog value) {
        Integer base = 0;
        try {
            if (floodTypeTup != null) {
                ArrayList<Integer> baselines = floodTypeTup._1;
                Integer defaultVaule = floodTypeTup._2;
                if (baselines != null && baselines.size() == BASELINE_SIZE) {
                    int timeIndex = getCurrentTimeIndex(value.getSketch_start_time());
                    base = baselines.get(timeIndex);
                    if (base == 0) {
                        logger.debug("获取到当前IP: {},类型: {} baseline值为0,替换为P95观测值{}", value.getDestination_ip(), value.getAttack_type(), defaultVaule);
                        base = defaultVaule;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("解析baseline数据失败,返回默认值0", e);
        }
        return base;
    }

    private String getConditions(String percent, long sessions, String tag) {
        switch (tag) {
            case "baseline":
                return "sessions > " + percent + " of baseline";
            case "static":
                return "sessions > " + sessions + " sessions/s";
            default:
                return null;
        }
    }

    private String getSourceCountryList(String sourceIpList) {
        String[] ipArr = sourceIpList.split(",");
        HashSet<String> countrySet = new HashSet<>();
        for (String ip : ipArr) {
            countrySet.add(IpUtils.ipLookup.countryLookup(ip));
        }
        return StringUtils.join(countrySet, ",");
    }

    private int getCurrentTimeIndex(long sketchStartTime) {
        long currentDayTime = sketchStartTime / (60 * 60 * 24) * 60 * 60 * 24;
        long indexLong = (sketchStartTime - currentDayTime) / 600;
        return Integer.parseInt(Long.toString(indexLong));
    }

    private Double getDiffPercent(long diff, long base) {
        return BigDecimal.valueOf((float)diff/base).setScale(4, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    private Severity judgeSeverity(double diffPercent) {
        if (diffPercent >= CommonConfig.BASELINE_SESSIONS_MINOR_THRESHOLD && diffPercent < CommonConfig.BASELINE_SESSIONS_WARNING_THRESHOLD) {
            return Severity.MINOR;
        } else if (diffPercent >= CommonConfig.BASELINE_SESSIONS_WARNING_THRESHOLD && diffPercent < CommonConfig.BASELINE_SESSIONS_MAJOR_THRESHOLD) {
            return Severity.WARNING;
        } else if (diffPercent >= CommonConfig.BASELINE_SESSIONS_MAJOR_THRESHOLD && diffPercent < CommonConfig.BASELINE_SESSIONS_SEVERE_THRESHOLD) {
            return Severity.MAJOR;
        } else if (diffPercent >= CommonConfig.BASELINE_SESSIONS_SEVERE_THRESHOLD && diffPercent < CommonConfig.BASELINE_SESSIONS_CRITICAL_THRESHOLD) {
            return Severity.SEVERE;
        } else if (diffPercent >= CommonConfig.BASELINE_SESSIONS_CRITICAL_THRESHOLD) {
            return Severity.CRITICAL;
        } else {
            return Severity.NORMAL;
        }
    }

    private enum Severity {
        /**
         * 判断严重程度枚举类型
         */
        CRITICAL("Critical", 5),
        SEVERE("Severe", 4),
        MAJOR("Major", 3),
        WARNING("Warning", 2),
        MINOR("Minor", 1),
        NORMAL("Normal", 0);

        private final String severity;
        private final int score;

        @Override
        public String toString() {
            return this.severity;
        }

        Severity(String severity, int score) {
            this.severity = severity;
            this.score = score;
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
