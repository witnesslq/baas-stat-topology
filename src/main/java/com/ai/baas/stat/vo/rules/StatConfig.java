package com.ai.baas.stat.vo.rules;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StatConfig {
    //key :statID
    private Map<String, ServiceStatConfig> serviceStatConfigs;

    public StatConfig() {
        serviceStatConfigs = new HashMap<String, ServiceStatConfig>();
    }


    public void addServiceStatConfig(ServiceStatConfig serviceStatConfig) {
        serviceStatConfigs.put(serviceStatConfig.getStatID(), serviceStatConfig);
    }

    public Map<String, ServiceStatConfig> getServiceStatConfigs() {
        return serviceStatConfigs;
    }

    @Override
    public String toString() {
        return "StatConfig{" +
                "serviceStatConfigs=" + serviceStatConfigs +
                '}';
    }
}
