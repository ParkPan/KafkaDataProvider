package com.yunshang.kafkahatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Tuple;

import javax.management.MBeanServerConnection;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.*;

/**
 * Created by PanPan on 2016/7/7.
 */
public class KafkaDataUtil {

    private static MBeanServerConnection mbsc = null;
    private static KafkaDataUtil utilInstance = null;
    private static JedisPool jedisPools;
    private final static int rows = 1440;
    private static Logger logger = LoggerFactory.getLogger("tooLog");

    private KafkaDataUtil(){
    }

    public static KafkaDataUtil getInitUtilInstance(String redisUrl, int redisPort) {
        if(utilInstance == null) {
            JedisPoolConfig jConfig = new JedisPoolConfig();
            jConfig.setMaxTotal(100);
            jConfig.setMaxWaitMillis(1000);
            jConfig.setTestOnBorrow(true);
            jedisPools = new JedisPool(jConfig, redisUrl, redisPort);
            utilInstance = new KafkaDataUtil();
        }
        return utilInstance;
    }

    public MBeanServerConnection getMBeanServerConnection(String jmxUrl) throws IOException {
        if(mbsc == null) {
            JMXServiceURL url = new JMXServiceURL(jmxUrl);
            JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
            mbsc = jmxc.getMBeanServerConnection();
        }
        return mbsc;
    }

    public boolean saveMonitorData(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPools.getResource();
            Long timestamp = new Date().getTime();
            String tmpValue = value + Long.toString(timestamp);
            jedis.zadd(key, timestamp, tmpValue);
            if(jedis.zcard(key) > rows) {
                jedis.zremrangeByScore(key, 0, timestamp-6*60*60*1000);
            }
        } catch (Exception ex) {
            logger.error("Redis connection pool exception when save data.");
            return false;
        } finally {
            if(jedis != null)
                jedis.close();
        }
        return true;
    }

//    public Set<Tuple> getMonitorData(String key, long beginTime) {
//        Jedis jedis = null;
//        Set<Tuple> retTuple = null;
//        try {
//            jedis = jedisPools.getResource();
//            retTuple = jedis.zrangeByScoreWithScores(key, beginTime, -1);
//        } catch (Exception ex) {
//            return null;
//        } finally {
//            if(jedis != null)
//                jedis.close();
//        }
//        return retTuple;
//    }
}
