package com.zdjizhi.utils;

import cn.hutool.log.Log;
import cn.hutool.log.LogFactory;
import com.zdjizhi.common.CommonConfig;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * HBase 工具类
 *
 * @author wanglihui
 */

public class HBaseUtils {
    private static final Log logger = LogFactory.get();
    private static Map<String, Map<String, List<Integer>>> baselineMap = new ConcurrentHashMap<>();
    private static Connection connection;

    private static String zookeeperIp;
    private static String hBaseTable;

    private static HBaseUtils hBaseUtils;

    private static void getInstance() {
        hBaseUtils = new HBaseUtils();
    }


    /**
     * 构造函数-新
     */
    private HBaseUtils() {
        zookeeperIp = CommonConfig.HBASE_ZOOKEEPER_QUORUM;
        hBaseTable = CommonConfig.HBASE_BASELINE_TABLE_NAME;
        //获取连接
        getConnection();
        //拉取所有,定时更新
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(2,
                new BasicThreadFactory.Builder().namingPattern("Dos-Detection-%d").daemon(true).build());
        try {
            executorService.scheduleAtFixedRate(HBaseUtils::getAll, 0, CommonConfig.BASELINE_THRESHOLD_SCHEDULE_DAYS, TimeUnit.DAYS);
        }catch (Exception e){
            logger.error("定时器任务执行失败", e);
        }
    }

    private static void getConnection() {
        try {
            // 管理Hbase的配置信息
            Configuration configuration = HBaseConfiguration.create();
            // 设置zookeeper节点
            configuration.set("hbase.zookeeper.quorum", zookeeperIp);
            configuration.set("hbase.client.retries.number", "3");
            configuration.set("hbase.bulkload.retries.number", "3");
            configuration.set("zookeeper.recovery.retry", "3");
            connection = ConnectionFactory.createConnection(configuration);
            logger.warn("HBaseUtils get HBase connection,now to getAll().");
        } catch (IOException ioe) {
            logger.error("HBaseUtils getHbaseConn() IOException===>{" + ioe + "}<===");
        } catch (RuntimeException e) {
            logger.error("HBaseUtils getHbaseConn() Exception===>{" + e + "}<===");
        }
    }

    /**
     * 获取所有的 key value
     */
    private static void getAll() {
        long begin = System.currentTimeMillis();
        try {
            Table table = connection.getTable(TableName.valueOf(hBaseTable));
            Scan scan = new Scan().setAllowPartialResults(true).setLimit(CommonConfig.HBASE_BASELINE_TOTAL_NUM);
            ResultScanner rs = table.getScanner(scan);
            for (Result result : rs) {
                Map<String, List<Integer>> floodTypeMap = new HashMap<>();
                String rowkey = Bytes.toString(result.getRow());
                ArrayList<Integer> tcp = getArraylist(result,"TCP SYN Flood", "session_num");
                ArrayList<Integer> udp = getArraylist(result,"UDP Flood", "session_num");
                ArrayList<Integer> icmp = getArraylist(result,"ICMP Flood", "session_num");
                ArrayList<Integer> dns = getArraylist(result,"DNS Amplification", "session_num");
                floodTypeMap.put("TCP SYN Flood",tcp);
                floodTypeMap.put("UDP Flood",udp);
                floodTypeMap.put("ICMP Flood",icmp);
                floodTypeMap.put("DNS Amplification",dns);
                baselineMap.put(rowkey,floodTypeMap);
            }

            logger.warn("HBaseUtils Get fullAmount List size->subIdMap.size(): " + baselineMap.size());
            logger.warn("HBaseUtils Get fullAmount List size->subIdMap.size() timeConsuming is: " + (System.currentTimeMillis() - begin));
            rs.close();
        } catch (IOException ioe) {
            logger.error("HBaseUtils getAll() is IOException===>{" + ioe + "}<===");
        } catch (RuntimeException e) {
            logger.error("HBaseUtils getAll() is Exception===>{" + e + "}<===");
        }
    }

    private static ArrayList<Integer> getArraylist(Result result, String family, String qualifier) throws IOException {
        if (!result.containsColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier))){
            return null;
        }
        ArrayWritable w = new ArrayWritable(IntWritable.class);
        w.readFields(new DataInputStream(new ByteArrayInputStream(result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)))));
        return fromWritable(w);
    }

    private static ArrayList<Integer> fromWritable(ArrayWritable writable) {
        Writable[] writables = writable.get();
        ArrayList<Integer> list = new ArrayList<>(writables.length);
        for (Writable wrt : writables) {
            list.add(((IntWritable)wrt).get());
        }
        return list;
    }

    /**
     * 获取 account
     *
     * @param destinationIp destinationIp
     * @param attackType attackType
     * @return baseline
     */
    public static List<Integer> getAccount(String destinationIp,String attackType) {

        if (hBaseUtils == null) {
            getInstance();
        }
        if (baselineMap.containsKey(destinationIp)){
            return baselineMap.get(destinationIp).getOrDefault(attackType,null);
        }
        return null;
    }

    public static void main(String[] args) {
        List<Integer> tcpSynFlood = getAccount("192.168.10.8", "ICMP Flood");
        if (tcpSynFlood != null){
            System.out.println(Arrays.toString(tcpSynFlood.toArray()));
            System.out.println(tcpSynFlood.size());
        }
    }

}
