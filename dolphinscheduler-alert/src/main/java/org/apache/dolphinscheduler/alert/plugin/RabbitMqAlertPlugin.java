package org.apache.dolphinscheduler.alert.plugin;

import com.alibaba.fastjson.JSON;
import org.apache.dolphinscheduler.alert.utils.Constants;
import org.apache.dolphinscheduler.alert.utils.JSONUtils;
import org.apache.dolphinscheduler.dao.DaoFactory;
import org.apache.dolphinscheduler.dao.ProcessInstanceDao;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.plugin.api.AlertPlugin;
import org.apache.dolphinscheduler.plugin.model.AlertInfo;
import org.apache.dolphinscheduler.plugin.model.PluginName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RabbitMqAlertPlugin implements AlertPlugin {

    private AlertContext alertContext;
    private RabbitMqPublisher publisher;
    private String exchange;
    private String rkey;
    private ProcessInstanceDao processInstanceDao = DaoFactory.getDaoInstance(ProcessInstanceDao.class);

    private static final String PROCESS_STATE_TITLE_FAILED = "failed";
    private static final String PROCESS_STATE_TITLE_SUCCESS = "success";
    private static final String PROCESS_STATE_FAILED = "failed";
    private static final String PROCESS_STATE_SUCCESS = "success";

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

            String state = null;
            if (info.getAlertData().getTitle().contains(PROCESS_STATE_TITLE_FAILED)){
                state = PROCESS_STATE_FAILED;
            }else if (info.getAlertData().getTitle().contains(PROCESS_STATE_TITLE_SUCCESS)){
                state = PROCESS_STATE_SUCCESS;
            }else{
                this.publisher.sendDirect(this.exchange, this.rkey, JSONUtils.toJsonString(info));
                result.put(Constants.STATUS, "true");
            }
            Map<String, Object> contentMap = parseContent(content,state);
            if (contentMap.size() > 1) {
                Object processInstanceId = contentMap.get("id") == null? contentMap.get("process instance id"): contentMap.get("id");
                if (processInstanceId == null){
                    logger.error("Illegal alert content");
                    return null;
                }
                int instanceId = Integer.parseInt(processInstanceId.toString());
                ProcessInstance processInstance = processInstanceDao.queryProcessInstanceById(instanceId);
                if (processInstance == null) {
                    logger.warn("ProcessInstance is null. instanceId:{}, Alert Info:{}",
                            instanceId, JSONUtils.toJsonString(info));
                    return null;
                }

//                int pdId = processInstance.getProcessDefinitionId();
//
//                info.addProp("processDefinitionId",String.valueOf(pdId));
//                info.addProp("alertType","processStatus");
                try {
                    info.addProp("instance",JSONUtils.toJsonString(processInstance));

                    this.publisher.sendDirect(this.exchange, this.rkey, JSONUtils.toJsonString(info));
                    result.put(Constants.STATUS, "true");
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("Send AlertInfo error. {}", e.getMessage());
                    result.put(Constants.STATUS, "false");
                    result.put(Constants.MESSAGE, e.getMessage());
                }
            }else {
                return null;
            }

//        processInstanceDao.queryProcessInstanceById(info.getAlertData());
        }catch (Exception e) {
            return null;
        }
        return result;
    }

    private Map<String, Object> parseContent(String content,String state) {
        Map<String, Object> cts = new HashMap<>();

        List<String> contentArr = null;
        if (PROCESS_STATE_SUCCESS.equals(state)){
            contentArr = JSONUtils.toList(content, String.class);
            for (String segment : contentArr) {
                String[] segmentArr = segment.split(":");
                cts.put(segmentArr[0],segmentArr[1]);
            }
            return cts;
        }else if (PROCESS_STATE_FAILED.equals(state)){
            List<Map> contentTaskMapList = JSONUtils.toList(content, Map.class);
            if (contentTaskMapList == null){
                logger.error("Illegal alert content. {}",content);
            }
            Map contentTaskMap = contentTaskMapList.get(0);
            return contentTaskMap;
        }
        return cts;
    }

    public static void main(String[] args) {
        String a = "{\"cmdTypeIfComplement\":\"START_PROCESS\",\"commandParam\":\"{}\",\"commandStartTime\":1600145511000,\"commandType\":\"START_PROCESS\",\"complementData\":false,\"connects\":\"[]\",\"endTime\":1600145515000,\"executorId\":4,\"failureStrategy\":\"CONTINUE\",\"historyCmd\":\"START_PROCESS\",\"host\":\"10.110.0.34:12346\",\"id\":272,\"isSubProcess\":\"NO\",\"locations\":\"{\\\"tasks-23460\\\":{\\\"name\\\":\\\"test\\\",\\\"targetarr\\\":\\\"\\\",\\\"nodenumber\\\":\\\"0\\\",\\\"x\\\":233,\\\"y\\\":147}}\",\"maxTryTimes\":0,\"name\":\"test-0-1600145511051\",\"processDefinitionId\":75,\"processInstanceJson\":\"{\\\"globalParams\\\":[],\\\"tasks\\\":[{\\\"type\\\":\\\"SHELL\\\",\\\"id\\\":\\\"tasks-23460\\\",\\\"name\\\":\\\"test\\\",\\\"params\\\":{\\\"resourceList\\\":[],\\\"localParams\\\":[],\\\"rawScript\\\":\\\"1213123\\\"},\\\"description\\\":\\\"sad\\\",\\\"runFlag\\\":\\\"NORMAL\\\",\\\"conditionResult\\\":{\\\"successNode\\\":[\\\"\\\"],\\\"failedNode\\\":[\\\"\\\"]},\\\"dependence\\\":{},\\\"maxRetryTimes\\\":\\\"0\\\",\\\"retryInterval\\\":\\\"1\\\",\\\"timeout\\\":{\\\"strategy\\\":\\\"\\\",\\\"interval\\\":null,\\\"enable\\\":false},\\\"taskInstancePriority\\\":\\\"MEDIUM\\\",\\\"workerGroup\\\":\\\"default\\\",\\\"preTasks\\\":[]}],\\\"tenantId\\\":21,\\\"timeout\\\":0}\",\"processInstancePriority\":\"MEDIUM\",\"processInstanceStop\":true,\"recovery\":\"NO\",\"runTimes\":1,\"startTime\":1600145511000,\"state\":\"FAILURE\",\"taskDependType\":\"TASK_POST\",\"tenantId\":21,\"timeout\":0,\"warningGroupId\":0,\"warningType\":\"ALL\",\"workerGroup\":\"default\"}";
        ProcessInstance instance = JSON.parseObject(a, ProcessInstance.class);
        System.out.println(instance);
    }
}
