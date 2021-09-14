package com.zdjizhi.utils;

import com.zdjizhi.common.CommonConfig;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author wlh
 */
public class HbaseUtils {
    private static final Logger logger = LoggerFactory.getLogger(HbaseUtils.class);
    private static Table table = null;
    private static Scan scan = null;
    private static ArrayList<String> floodTypeList = new ArrayList<>();

    static {
        floodTypeList.add("TCP SYN Flood");
        floodTypeList.add("UDP Flood");
        floodTypeList.add("ICMP Flood");
        floodTypeList.add("DNS Flood");
    }

    private static void prepareHbaseEnv() throws IOException {
        org.apache.hadoop.conf.Configuration config = HBaseConfiguration.create();

        config.set("hbase.zookeeper.quorum", CommonConfig.HBASE_ZOOKEEPER_QUORUM);
        config.set("hbase.client.retries.number", "3");
        config.set("hbase.bulkload.retries.number", "3");
        config.set("zookeeper.recovery.retry", "3");
        config.setInt(HConstants.HBASE_CLIENT_OPERATION_TIMEOUT, CommonConfig.HBASE_CLIENT_OPERATION_TIMEOUT);
        config.setInt(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, CommonConfig.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD);

        TableName tableName = TableName.valueOf(CommonConfig.HBASE_BASELINE_TABLE_NAME);
        Connection conn = ConnectionFactory.createConnection(config);
        table = conn.getTable(tableName);
        scan = new Scan().setAllowPartialResults(true).setLimit(CommonConfig.HBASE_BASELINE_TOTAL_NUM);
        logger.info("连接hbase成功，正在读取baseline数据");
    }

    public static void main(String[] args) {
        Map<String, Map<String, Tuple2<ArrayList<Integer>, Integer>>> baselineMap = readFromHbase();
        Set<String> keySet = baselineMap.keySet();
        for (String key : keySet) {
            Map<String, Tuple2<ArrayList<Integer>, Integer>> stringTuple2Map = baselineMap.get(key);
            Set<String> strings = stringTuple2Map.keySet();
            for (String s:strings){
                Tuple2<ArrayList<Integer>, Integer> arrayListIntegerTuple2 = stringTuple2Map.get(s);
                System.out.println(key+"---"+s+"---"+arrayListIntegerTuple2._1+"---"+arrayListIntegerTuple2._2);
            }
        }
        System.out.println(baselineMap.size());
    }

    public static Map<String, Map<String, Tuple2<ArrayList<Integer>, Integer>>> readFromHbase() {
        Map<String, Map<String, Tuple2<ArrayList<Integer>, Integer>>> baselineMap = new HashMap<>();
        try {
            prepareHbaseEnv();
            logger.info("开始读取baseline数据");
            ResultScanner rs = table.getScanner(scan);
            for (Result result : rs) {
                Map<String, Tuple2<ArrayList<Integer>, Integer>> floodTypeMap = new HashMap<>();
                String rowkey = Bytes.toString(result.getRow());
                for (String type:floodTypeList){
                    ArrayList<Integer> sessionRate = getArraylist(result, type, "session_rate");
                    if (sessionRate != null && !sessionRate.isEmpty()){
                        Integer defaultValue = getDefaultValue(result, type, "session_rate_default_value");
                        floodTypeMap.put(type, Tuple.of(sessionRate, defaultValue));
                    }
                }
                baselineMap.put(rowkey, floodTypeMap);
            }
            logger.info("格式化baseline数据成功，读取IP共：{}", baselineMap.size());
        } catch (Exception e) {
            logger.error("读取hbase数据失败", e);
        }
        return baselineMap;
    }

    private static Integer getDefaultValue(Result result, String family, String qualifier) {
        byte[] value = result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        if (value != null){
            return Bytes.toInt(value);
        }
        return 1;
    }

    private static ArrayList<Integer> getArraylist(Result result, String family, String qualifier) throws IOException {
        if (containsColumn(result, family, qualifier)) {
            ArrayWritable w = new ArrayWritable(IntWritable.class);
            w.readFields(new DataInputStream(new ByteArrayInputStream(result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)))));
            return fromWritable(w);
        }
        return null;
    }

    private static ArrayList<Integer> fromWritable(ArrayWritable writable) {
        Writable[] writables = writable.get();
        ArrayList<Integer> list = new ArrayList<>(writables.length);
        for (Writable wrt : writables) {
            list.add(((IntWritable) wrt).get());
        }
        return list;
    }

    private static boolean containsColumn(Result result, String family, String qualifier) {
        return result.containsColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
    }

}
