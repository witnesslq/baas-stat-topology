package com.ai.baas.stat.vo;

import java.util.HashMap;
import java.util.Map;

import com.ai.baas.stat.constants.Constants;
import com.ai.baas.stat.util.DBUtils;
import com.ai.baas.stat.vo.rules.ServiceStatConfig;

public class ServiceStatResult {

    private Map<String, StatResultItem> statResults = new HashMap<String, StatResultItem>();

    public void stat(ServiceStatConfig config, Map<String, String> tupleData) throws Exception {
        String groupKey = generateGroupKey(config, tupleData);
        StatResultItem statResultItem = statResults.get(groupKey);
        if (statResultItem == null) {
            statResultItem = loadStatResult(config, tupleData);
            statResults.put(groupKey, statResultItem);
        }
        statResultItem.stat(tupleData);
    }

    private static String generateGroupKey(ServiceStatConfig config, Map<String, String> tupleData) {
        StringBuilder key = new StringBuilder();
        for (String groupField : config.getGroupFields()) {
            key.append(tupleData.get(groupField) + "@");
        }
        return key.toString();
    }


    private StatResultItem loadStatResult(ServiceStatConfig config, Map<String, String> tupleData) throws Exception {
        Map<String, String> groupFieldValueMapping = new HashMap<String, String>();
        for (String groupField : config.getGroupFields()) {
            String groupFieldValue = tupleData.get(groupField);
            if (groupFieldValue == null || groupFieldValue.length() == 0) {
            }
            groupFieldValueMapping.put(groupField, tupleData.get(groupField));
        }
        return DBUtils.loadStatResult(config.getTableName(tupleData), config.getStatFields(), groupFieldValueMapping);
    }

    public void saveStatResult() throws Exception {
        for (Map.Entry<String, StatResultItem> entry : statResults.entrySet()) {
            entry.getValue().saveStatResult();
        }
    }

    public static ServiceStatResult load(ServiceStatConfig config, Map<String, String> tupleData) throws Exception {
        ServiceStatResult serviceStatResult = new ServiceStatResult();
        serviceStatResult.statResults.put(
                generateGroupKey(config, tupleData),
                DBUtils.loadStatResult(config.getTableName(tupleData), config.getStatFields(),
                        generateGroupFieldValueMapping(config, tupleData))
        );
        return serviceStatResult;
    }

    private static Map<String, String> generateGroupFieldValueMapping(ServiceStatConfig config,
                                                                      Map<String, String> tupleData) {
        HashMap<String, String> groupFiledValueMapping = new HashMap<String, String>();
        for (String groupField : config.getGroupFields()) {
            groupFiledValueMapping.put(groupField, tupleData.get(groupField));
        }
        return groupFiledValueMapping;
    }
}
