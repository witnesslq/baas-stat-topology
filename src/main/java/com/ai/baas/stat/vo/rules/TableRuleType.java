package com.ai.baas.stat.vo.rules;

import com.ai.baas.storm.util.BaseConstants;

import java.util.Map;

public enum TableRuleType {
    FULL(0), ACC_MONTH(1), START_TIME(2);

    private int ruleTypeIndex;

    TableRuleType(int ruleTypeIndex) {
        this.ruleTypeIndex = ruleTypeIndex;
    }

    public String getTableName(String tableName, Map<String, String> tuple) {
        String result = null;
        switch (ruleTypeIndex) {
            case 1: {
                result = tableName + "_" + tuple.get(BaseConstants.ACC_MONTH);
                break;
            }
            case 2: {
                result = tableName + "_" + tuple.get(BaseConstants.START_TIME).substring(0, 6);
                break;
            }
            default:
                result = tableName;
        }
        return result;
    }

    public static TableRuleType convert(String tableRuleType) {
        TableRuleType result = null;
        switch (tableRuleType) {
            case "ACC_MONTH": {
                result = ACC_MONTH;
                break;
            }
            case "START_TIME": {
                result = START_TIME;
                break;
            }
            default: {
                result = FULL;
            }
        }

        return result;
    }
}
