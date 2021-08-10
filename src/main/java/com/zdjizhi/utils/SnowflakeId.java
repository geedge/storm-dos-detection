package com.zdjizhi.utils;

import com.zdjizhi.common.CommonConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeId {
    private static final Logger logger = LoggerFactory.getLogger(SnowflakeId.class);

    /**
     * 共64位 第一位为符号位 默认0
     * 时间戳 39位(17 year), centerId:(关联每个环境或任务数) :7位(0-127),
     * workerId(关联进程):6(0-63) ,序列号：11位(2047/ms)
     *
     * 序列号 /ms = (-1L ^ (-1L << 11))
     * 最大使用年 = (1L << 39) / (1000L * 60 * 60 * 24 * 365)
     */
    /**
     * 开始时间截 (2020-11-14 00:00:00) max 17years
     */
    private final long twepoch = 1605283200000L;

    /**
     * 机器id所占的位数
     */
    private final long workerIdBits = 6L;

    /**
     * 数据标识id所占的位数
     */
    private final long dataCenterIdBits = 7L;

    /**
     * 支持的最大机器id，结果是63 (这个移位算法可以很快的计算出几位二进制数所能表示的最大十进制数)
     * M << n  = M * 2^n
     */
    private final long maxWorkerId = -1L ^ (-1L << workerIdBits);

    /**
     * 支持的最大数据标识id，结果是127
     */
    private final long maxDataCenterId = -1L ^ (-1L << dataCenterIdBits);

    /**
     * 序列在id中占的位数
     */
    private final long sequenceBits = 11L;

    /**
     * 机器ID向左移12位
     */
    private final long workerIdShift = sequenceBits;

    /**
     * 数据标识id向左移17位(14+6)
     */
    private final long dataCenterIdShift = sequenceBits + workerIdBits;

    /**
     * 时间截向左移22位(4+6+14)
     */
    private final long timestampLeftShift = sequenceBits + workerIdBits + dataCenterIdBits;

    /**
     * 生成序列的掩码，这里为2047
     */
    private final long sequenceMask = -1L ^ (-1L << sequenceBits);

    /**
     * 工作机器ID(0~63)
     */
    private long workerId;

    /**
     * 数据中心ID(0~127)
     */
    private long dataCenterId;

    /**
     * 毫秒内序列(0~2047)
     */
    private long sequence = 0L;

    /**
     * 上次生成ID的时间截
     */
    private long lastTimestamp = -1L;


    /**
     * 设置允许时间回拨的最大限制10s
     */
    private static final long ROLL_BACK_TIME = 10000L;


    private static SnowflakeId idWorker;

    private static ZookeeperUtils zookeeperUtils = new ZookeeperUtils();

    static {
        idWorker = new SnowflakeId(CommonConfig.HBASE_ZOOKEEPER_QUORUM, CommonConfig.DATA_CENTER_ID_NUM);
    }

    //==============================Constructors=====================================

    /**
     * 构造函数
     */
    private SnowflakeId(String zookeeperIp, long dataCenterIdNum) {
        DistributedLock lock = new DistributedLock(CommonConfig.HBASE_ZOOKEEPER_QUORUM, "disLocks1");
        try {
            lock.lock();
            int tmpWorkerId = zookeeperUtils.modifyNode("/Snowflake/" + "worker" + dataCenterIdNum, zookeeperIp);
            if (tmpWorkerId > maxWorkerId || tmpWorkerId < 0) {
                throw new IllegalArgumentException(String.format("worker Id can't be greater than %d or less than 0", maxWorkerId));
            }
            if (dataCenterIdNum > maxDataCenterId || dataCenterIdNum < 0) {
                throw new IllegalArgumentException(String.format("datacenter Id can't be greater than %d or less than ", maxDataCenterId));
            }
            this.workerId = tmpWorkerId;
            this.dataCenterId = dataCenterIdNum;
        } catch (RuntimeException e) {
            logger.error("This is not usual error!!!===>>>" + e + "<<<===");
        }finally {
            lock.unlock();
        }
    }

    // ==============================Methods==========================================

    /**
     * 获得下一个ID (该方法是线程安全的)
     *
     * @return SnowflakeId
     */
    private synchronized long nextId() {
        long timestamp = timeGen();
        //设置一个允许回拨限制时间，系统时间回拨范围在rollBackTime内可以等待校准
        if (lastTimestamp - timestamp > 0 && lastTimestamp - timestamp < ROLL_BACK_TIME) {
            timestamp = tilNextMillis(lastTimestamp);
        }
        //如果当前时间小于上一次ID生成的时间戳，说明系统时钟回退过这个时候应当抛出异常
        if (timestamp < lastTimestamp) {
            throw new RuntimeException(
                    String.format("Clock moved backwards.  Refusing to generate id for %d milliseconds", lastTimestamp - timestamp));
        }

        //如果是同一时间生成的，则进行毫秒内序列
        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & sequenceMask;
            //毫秒内序列溢出
            if (sequence == 0) {
                //阻塞到下一个毫秒,获得新的时间戳
                timestamp = tilNextMillis(lastTimestamp);
            }
        }
        //时间戳改变，毫秒内序列重置
        else {
            sequence = 0L;
        }

        //上次生成ID的时间截
        lastTimestamp = timestamp;

        //移位并通过或运算拼到一起组成64位的ID
        return ((timestamp - twepoch) << timestampLeftShift)
                | (dataCenterId << dataCenterIdShift)
                | (workerId << workerIdShift)
                | sequence;
    }

    /**
     * 阻塞到下一个毫秒，直到获得新的时间戳
     *
     * @param lastTimestamp 上次生成ID的时间截
     * @return 当前时间戳
     */
    private long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    /**
     * 返回以毫秒为单位的当前时间
     *
     * @return 当前时间(毫秒)
     */
    private long timeGen() {
        return System.currentTimeMillis();
    }


    /**
     * 静态工具类
     */
    public static Long generateId() {
        return idWorker.nextId();
    }


}
