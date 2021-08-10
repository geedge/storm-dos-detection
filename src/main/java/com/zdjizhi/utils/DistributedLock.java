package com.zdjizhi.utils;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;


public class DistributedLock implements Lock, Watcher {
    private static final Logger logger = LoggerFactory.getLogger(DistributedLock.class);

    private ZooKeeper zk = null;
    /**
     * 根节点
     */
    private final String ROOT_LOCK = "/locks";
    /**
     * 竞争的资源
     */
    private String lockName;
    /**
     * 等待的前一个锁
     */
    private String waitLock;
    /**
     * 当前锁
     */
    private String currentLock;
    /**
     * 计数器
     */
    private CountDownLatch countDownLatch;

    private int sessionTimeout = 2000;

    private List<Exception> exceptionList = new ArrayList<Exception>();

    /**
     * 配置分布式锁
     *
     * @param config   连接的url
     * @param lockName 竞争资源
     */
    public DistributedLock(String config, String lockName) {
        this.lockName = lockName;
        try {
            // 连接zookeeper
            zk = new ZooKeeper(config, sessionTimeout, this);
            Stat stat = zk.exists(ROOT_LOCK, false);
            if (stat == null) {
                // 如果根节点不存在，则创建根节点
                zk.create(ROOT_LOCK, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (IOException | InterruptedException | KeeperException e) {
            logger.error("Node already exists!");
        }
    }

    /**
     * 节点监视器
     */
    @Override
    public void process(WatchedEvent event) {
        if (this.countDownLatch != null) {
            this.countDownLatch.countDown();
        }
    }

    @Override
    public void lock() {
        if (exceptionList.size() > 0) {
            throw new LockException(exceptionList.get(0));
        }
        try {
            if (this.tryLock()) {
                logger.info(Thread.currentThread().getName() + " " + lockName + "获得了锁");
            } else {
                // 等待锁
                waitForLock(waitLock, sessionTimeout);
            }
        } catch (InterruptedException | KeeperException e) {
            logger.error("获取锁异常" + e);
        }
    }

    @Override
    public boolean tryLock() {
        try {
            String splitStr = "_lock_";
            if (lockName.contains(splitStr)) {
                throw new LockException("锁名有误");
            }
            // 创建临时有序节点
            currentLock = zk.create(ROOT_LOCK + "/" + lockName + splitStr, new byte[0],
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            // 取所有子节点
            List<String> subNodes = zk.getChildren(ROOT_LOCK, false);
            // 取出所有lockName的锁
            List<String> lockObjects = new ArrayList<String>();
            for (String node : subNodes) {
                String tmpNode = node.split(splitStr)[0];
                if (tmpNode.equals(lockName)) {
                    lockObjects.add(node);
                }
            }
            Collections.sort(lockObjects);
            // 若当前节点为最小节点，则获取锁成功
            if (currentLock.equals(ROOT_LOCK + "/" + lockObjects.get(0))) {
                return true;
            }
            // 若不是最小节点，则找到自己的前一个节点
            String prevNode = currentLock.substring(currentLock.lastIndexOf("/") + 1);
            waitLock = lockObjects.get(Collections.binarySearch(lockObjects, prevNode) - 1);
        } catch (InterruptedException | KeeperException e) {
            logger.error("获取锁过程异常" + e);
        }
        return false;
    }


    @Override
    public boolean tryLock(long timeout, TimeUnit unit) {
        try {
            if (this.tryLock()) {
                return true;
            }
            return waitForLock(waitLock, timeout);
        } catch (KeeperException | InterruptedException | RuntimeException e) {
            logger.error("判断是否锁定异常" + e);
        }
        return false;
    }

    /**
     * 等待锁
     *
     * @param prev 锁名称
     * @param waitTime 等待时间
     * @return
     * @throws KeeperException
     * @throws InterruptedException
     */
    private boolean waitForLock(String prev, long waitTime) throws KeeperException, InterruptedException {
        Stat stat = zk.exists(ROOT_LOCK + "/" + prev, true);

        if (stat != null) {
            this.countDownLatch = new CountDownLatch(1);
            // 计数等待，若等到前一个节点消失，则precess中进行countDown，停止等待，获取锁
            this.countDownLatch.await(waitTime, TimeUnit.MILLISECONDS);
            this.countDownLatch = null;
        }
        return true;
    }

    @Override
    public void unlock() {
        try {
            zk.delete(currentLock, -1);
            currentLock = null;
            zk.close();
        } catch (InterruptedException | KeeperException e) {
            logger.error("关闭锁异常" + e);
        }
    }

    @Override
    public Condition newCondition() {
        return null;
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        this.lock();
    }


    public class LockException extends RuntimeException {
        private static final long serialVersionUID = 1L;

        public LockException(String e) {
            super(e);
        }

        public LockException(Exception e) {
            super(e);
        }
    }

}
