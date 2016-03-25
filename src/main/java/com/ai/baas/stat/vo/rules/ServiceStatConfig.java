package com.ai.baas.stat.vo.rules;


import com.ai.baas.stat.constants.Constants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class ServiceStatConfig {
    private Logger logger = LogManager.getLogger(ServiceStatConfig.class);
    private String tableName;
    private String statID;
    private TableRuleType tableRuleType;
    private List<String> groupFields;
    private List<String> statFields;

    public ServiceStatConfig(String tableName, String statID, String groupFields, String statFields, String tableRuleType) {
        this.tableName = tableName;
        this.statID = statID;
        this.groupFields = buildGroupFields(groupFields);
        this.statFields = buildStatFields(statFields);
        this.tableRuleType = TableRuleType.convert(tableRuleType);
    }

    private List<String> buildStatFields(String statFields) {
        try {
            return Arrays.asList(statFields.split(Constants.StatTopology.STAT_RULE_FIELDS_SPILT));
        } catch (Exception e) {
            logger.error("Failed to split stat fields. Fields[{}].", statFields);
            throw new RuntimeException("Failed to split stat fields. ");
        }
    }

    private List<String> buildGroupFields(String groupFields) {
        try {
            return Arrays.asList(groupFields.split(Constants.StatTopology.STAT_RULE_FIELDS_SPILT));
        } catch (Exception e) {
            logger.error("Failed to split group fields. Fields[{}].", groupFields);
            throw new RuntimeException("Failed to split group fields. ");
        }
    }


    public String getStatID() {
        return statID;
    }

    public List<String> getGroupFields() {
        return groupFields;
    }

    public List<String> getStatFields() {
        return statFields;
    }

    public String getTableName(Map<String, String> tupleData) {
        return tableRuleType.getTableName(tableName, tupleData);
    }
}
