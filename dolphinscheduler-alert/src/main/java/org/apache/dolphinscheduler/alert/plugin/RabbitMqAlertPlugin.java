package org.apache.dolphinscheduler.alert.plugin;

import org.apache.dolphinscheduler.plugin.api.AlertContext;
import org.apache.dolphinscheduler.plugin.api.AlertPlugin;
import org.apache.dolphinscheduler.plugin.model.AlertInfo;
import org.apache.dolphinscheduler.plugin.model.PluginName;

import java.util.Map;

/**
 * @Description TODO
 * @Author fang
 * @Date 2020-09-09 22:05
 **/
public class RabbitMqAlertPlugin implements AlertPlugin {

    private AlertContext alertContext;

    public RabbitMqAlertPlugin(AlertContext alertContext) {
        this.alertContext = alertContext;
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
    public Map<String, Object> process(AlertInfo info, AlertContext alertContext) {
        alertContext.getParam("");
        return null;
    }
}
