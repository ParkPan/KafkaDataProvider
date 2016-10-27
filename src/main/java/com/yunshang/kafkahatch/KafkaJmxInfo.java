package com.yunshang.kafkahatch;

import javafx.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.*;

/**
 * Created by PanPan on 2016/7/11.
 */

public class KafkaJmxInfo {

    private static KafkaJmxInfo jmxInstance = new KafkaJmxInfo();
    private HashMap<String, Pair<String, List<String>>> jmxInfoMap;
    private static Logger logger = LoggerFactory.getLogger("tooLog");

    private KafkaJmxInfo(){
        ArrayList<String> monitorTopic = new ArrayList<String>(), monitorJmxInfo = new ArrayList<String>();
        ArrayList<List<String>> topicAttribute = new ArrayList<List<String>>();

        Properties prop = new Properties();
        try {
            FileInputStream in = new FileInputStream("./conf/monitorTopic.properties");
            prop.load(in);
            Iterator<String> it = prop.stringPropertyNames().iterator();
            while (it.hasNext()) {
                String key = it.next();
                String value = prop.getProperty(key);
                monitorTopic.add(key);
                monitorJmxInfo.add(value.split("\\|")[0]);
                topicAttribute.add(Arrays.asList(value.split("\\|")[1].split(";")));
            }
            in.close();
        } catch (Exception e) {
            logger.error("init KafkaJmxInfo failed, get monitorTopic.properties file info error.");
            return;
        }
        if(monitorTopic.size() == 0) {
            logger.warn("no monitor topic found in monitorTopic.properties file.");
        }
        jmxInfoMap = new HashMap<String, Pair<String, List<String>>>();
        for(int i=0; i<monitorTopic.size(); i++) {
            Pair<String, List<String>> tmpValue = new Pair<String, List<String>>(monitorJmxInfo.get(i), topicAttribute.get(i));
            jmxInfoMap.put(monitorTopic.get(i), tmpValue);
        }
    }

    public static KafkaJmxInfo getKafkaJmxInfoInstance() {
        return jmxInstance;
    }

    public HashMap<String, Pair<String, List<String>>> getJmxInfoMap() {
        return jmxInfoMap;
    }
}
