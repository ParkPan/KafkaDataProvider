/**
 * Created by PanPan on 2016/7/7.
 */

import javax.management.ObjectName;
import javax.management.MBeanServerConnection;
import java.io.FileInputStream;
import java.util.*;

import com.yunshang.kafkahatch.KafkaDataUtil;
import com.yunshang.kafkahatch.KafkaJmxInfo;
import javafx.util.Pair;
import org.apache.log4j.PropertyConfigurator;
import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;

import static org.quartz.JobBuilder.*;
import static org.quartz.TriggerBuilder.*;
import static org.quartz.SimpleScheduleBuilder.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaDataProvider implements Job {
    private static Logger logger = LoggerFactory.getLogger("tooLog");

    private static String redisUrl;
    private static int redisPort;
    private static String jmxServerUrl;
    private static int interval;
    public KafkaDataProvider() {
    }

    public void execute(JobExecutionContext jobContext) throws JobExecutionException {
        new KafkaDataProvider().extractMonitorData();
    }

    public void extractMonitorData() {
        try {
            MBeanServerConnection jmxConnection = KafkaDataUtil.getInitUtilInstance(redisUrl, redisPort).getMBeanServerConnection(jmxServerUrl);
            HashMap<String, Pair<String, List<String>>> jmxInfoMap = KafkaJmxInfo.getKafkaJmxInfoInstance().getJmxInfoMap();
            for (String key : jmxInfoMap.keySet()) {
                Pair<String, List<String>> entry = jmxInfoMap.get(key);
                ObjectName tmpObjName = new ObjectName(entry.getKey());
                String monitorValue = "";
                for (String monitorAtt : entry.getValue()) {
                    monitorValue += jmxConnection.getAttribute(tmpObjName, monitorAtt) + ":";
                }
                if (!KafkaDataUtil.getInitUtilInstance(redisUrl, redisPort).saveMonitorData(key, monitorValue)) {
                    logger.error("Save {} key data failed", key);
                }
            }
        } catch (Exception e) {
            logger.error("executing exception in method extractMonitorData.");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        PropertyConfigurator.configure("./conf/log4j.properties");
        try {
            Properties prop = new Properties();
            FileInputStream fin = new FileInputStream("./conf/configInfo.properties");
            prop.load(fin);
            redisUrl = prop.getProperty("redisUrl");
            redisPort = Integer.parseInt(prop.getProperty("redisPort"));
            jmxServerUrl = prop.getProperty("jmxServerUrl");
            interval = Integer.parseInt(prop.getProperty("interval"));
            fin.close();
        }catch (Exception e) {
            logger.error("Start provider tool failed, get configInfo.properties file info error.");
            e.printStackTrace();
            return;
        }
        try {
            Scheduler scheduler = StdSchedulerFactory.getDefaultScheduler();
            JobDetail job = newJob(KafkaDataProvider.class).withIdentity("job1", "group1").build();
            Trigger trigger = newTrigger().withIdentity("trigger1", "group1").startNow()
                    .withSchedule(simpleSchedule().withIntervalInSeconds(interval).repeatForever()).build();
            scheduler.scheduleJob(job, trigger);
            logger.info("Provider tool scheduler start working...");
            scheduler.start();
        } catch (SchedulerException se) {
           logger.error("Scheduler exception, provider tool exit.");
           se.printStackTrace();
        }
    }
}