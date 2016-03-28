package com.ai.baas.stat.vo.rules;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StatConfig {
    private String tenantId;
    private String serviceType;
    //key :statID
    private Map<String, ServiceStatConfig> serviceStatConfigs;

    public StatConfig(String tenantId, String serviceType) {
        this.tenantId = tenantId;
        this.serviceType = serviceType;
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

    public String getServiceType() {
        return serviceType;
    }

    public String getTenantId() {
        return tenantId;
    }
}
