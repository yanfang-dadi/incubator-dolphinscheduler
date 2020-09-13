package org.apache.dolphinscheduler.alert.plugin;

import org.apache.dolphinscheduler.alert.utils.Constants;
import org.apache.dolphinscheduler.alert.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.DaoFactory;
import org.apache.dolphinscheduler.dao.ProcessInstanceDao;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.plugin.api.AlertPlugin;
import org.apache.dolphinscheduler.plugin.model.AlertInfo;
import org.apache.dolphinscheduler.plugin.model.PluginName;
import org.apache.parquet.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RabbitMqAlertPlugin implements AlertPlugin {

    private AlertContext alertContext;
    private RabbitMqPublisher publisher;
    private String exchange;
    private String rkey;
    private ProcessInstanceDao processInstanceDao = DaoFactory.getDaoInstance(ProcessInstanceDao.class);

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqAlertPlugin.class);

    public RabbitMqAlertPlugin(AlertContext alertContext) {
        this.alertContext = alertContext;
        String host = this.alertContext.getParam(Constants.ALERT_SERVER_CONTEXT_KEY_MQ_HOST);
        int port = Integer.valueOf(
                this.alertContext.getParam(Constants.ALERT_SERVER_CONTEXT_KEY_MQ_PORT));
        String user = this.alertContext.getParam(Constants.ALERT_SERVER_CONTEXT_KEY_MQ_USER);
        String pwd = this.alertContext.getParam(Constants.ALERT_SERVER_CONTEXT_KEY_MQ_PWD);
        String vhost = this.alertContext.getParam(Constants.ALERT_SERVER_CONTEXT_KEY_MQ_V_HOST);
        String exchange = this.alertContext.getParam(Constants.ALERT_SERVER_CONTEXT_KEY_MQ_EXCHANGE);
        String rkey = this.alertContext.getParam(Constants.ALERT_SERVER_CONTEXT_KEY_MQ_ROUTING_KEY);

        this.publisher = new RabbitMqPublisher(host,port,vhost,user,pwd);
        this.exchange = exchange;
        this.rkey = rkey;
    }

    @Override
    public String getId() {
        return "rabbit.mq.alert";
    }

    @Override
    public PluginName getName() {
        return new PluginName(){{setChinese("Rabbit消息通知插件");setEnglish("RabbitMqAlert");}};
    }

    @Override
    public Map<String, Object> process(AlertInfo info) {
        Map<String,Object> result = new HashMap<>();

        try {
            //content加入processDefinitionId
            String content = info.getAlertData().getContent();
            if (content == null) {
                logger.warn("Content is null. Alert Info:{}", JSONUtils.toJsonString(info));
                return null;
            }


            logger.info("Parse content: {}", content);

            Map<String, String> contentMap = parseContent(content);
            if (contentMap.size() > 1) {
                int instanceId = Integer.valueOf(contentMap.get("id"));
                ProcessInstance processInstance = processInstanceDao.queryProcessInstanceById(instanceId);
                if (processInstance == null) {
                    logger.warn("ProcessInstance is null. instanceId:{}, Alert Info:{}",
                            instanceId, JSONUtils.toJsonString(info));
                }

                int pdId = processInstance.getProcessDefinitionId();
                contentMap.put("processDefinitionId", String.valueOf(pdId));

                info.addProp("alertType","processStatus");
                info.getAlertData().setContent(JSONUtils.toJsonString(contentMap));
            }

//        processInstanceDao.queryProcessInstanceById(info.getAlertData());
            try {
                this.publisher.sendDirect(this.exchange, this.rkey, JSONUtils.toJsonString(info));
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("Send AlertInfo error. {}", e.getMessage());
                result.put(Constants.STATUS, "false");
                result.put(Constants.MESSAGE, e.getMessage());
            }
            result.put(Constants.STATUS, "true");
        }catch (Exception e) {
            return null;
        }
        return result;
    }

    private Map<String, String> parseContent(String content) {
        Map<String,String> cts = new HashMap<>();

        List<String> contentArr = JSONUtils.toList(content,String.class);
        for (String segment : contentArr) {
            String[] segmentArr = segment.split(":");
            cts.put(segmentArr[0],segmentArr[1]);
        }
        return cts;
    }

    public static void main(String[] args) {
        String a = "[\"id:107\",\"name:user_member_clean-0-1599602401164\",\"job type: scheduler\",\"state: SUCCESS\",\"recovery:NO\",\"run time: 1\",\"start time: 2020-09-09 06:00:01\",\"end time: 2020-09-09 06:50:57\",\"host: 10.110.0.34:12346\"]";
        List<String> l = JSONUtils.toList(a,String.class);
        System.out.println(l.size());
    }
}
