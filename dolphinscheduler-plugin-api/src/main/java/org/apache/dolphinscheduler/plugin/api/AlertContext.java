package org.apache.dolphinscheduler.plugin.api;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Description 告警通知上下文环境
 * @Author fang
 * @Date 2020-09-09 22:00
 **/
public class AlertContext {
    private Map<String,String> params;

    public AlertContext(String[] args) {
        if (args.length)
    }

    public void addParam(String key, String val){
        if (params == null){
            params = new ConcurrentHashMap<>();
        }
        params.put(key,val);
    }

    public String getParam(String key){
        if (params == null || key == null){
            return null;
        }
        return params.get(key);
    }
}
