package org.apache.dolphinscheduler.alert.plugin;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.dolphinscheduler.alert.utils.Constants;
import org.apache.dolphinscheduler.alert.utils.JSONUtils;

public class AlertContext {
    private Map<String,String> params;

    public AlertContext(String[] args) {
        verify(args);
        params = new ConcurrentHashMap<>();
        params.put(Constants.ALERT_SERVER_CONTEXT_KEY_MQ_HOST,args[0]);
        params.put(Constants.ALERT_SERVER_CONTEXT_KEY_MQ_PORT,args[1]);
        params.put(Constants.ALERT_SERVER_CONTEXT_KEY_MQ_USER,args[2]);
        params.put(Constants.ALERT_SERVER_CONTEXT_KEY_MQ_PWD,args[3]);
        params.put(Constants.ALERT_SERVER_CONTEXT_KEY_MQ_V_HOST,args[4]);
        params.put(Constants.ALERT_SERVER_CONTEXT_KEY_MQ_EXCHANGE,args[5]);
        params.put(Constants.ALERT_SERVER_CONTEXT_KEY_MQ_ROUTING_KEY,args[6]);
    }

    private void verify(String[] args) {
        if (args.length !=7){
            throw new RuntimeException(String.format("AlertContext params lose, need 7 ordered params:" +
                    " host, port, user, pwd, vhost, exchange, routingKey. Params: %s", JSONUtils.toJsonString(args)));
        }
    }

    public void addParam(String key, String val){
        params.put(key,val);
    }

    public String getParam(String key){
        return params.get(key);
    }
}
