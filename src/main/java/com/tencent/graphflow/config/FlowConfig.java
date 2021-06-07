package com.tencent.graphflow.config;

import com.tencent.graphflow.utils.DataTypeUtils;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class FlowConfig extends ConcurrentHashMap<String, String> {

    private static final Log LOG = LogFactory.getLog(FlowConfig.class);
    private static volatile FlowConfig INSTANCE = null;

    public FlowConfig() {
        final Properties properties = new Properties();
        try {
            properties.load(this.getClass().getClassLoader().getResourceAsStream("flow.properties"));
            for (Object property : properties.keySet()) {
                put(property.toString(), properties.get(property).toString());
            }
        } catch (Throwable e) {
            LOG.warn("load config from data flow failed!", e);
        }
    }

    public static FlowConfig getInstance() {
        if (INSTANCE == null) {
            synchronized (FlowConfig.class) {
                if (INSTANCE == null) {
                    INSTANCE = new FlowConfig();
                }
            }
        }
        return INSTANCE;
    }

    public <T> T get(String key, T defaultValue) {
        String value = get(key);
        if (value == null) {
            return defaultValue;
        }
        Class clazz = defaultValue.getClass();
        T ret = DataTypeUtils.getValue(value, clazz);
        return ret;
    }
}
