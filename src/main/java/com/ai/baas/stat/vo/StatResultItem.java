package com.ai.baas.stat.vo;


import com.ai.baas.storm.jdbc.JdbcProxy;
import com.ai.baas.storm.util.BaseConstants;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by xin on 16-3-21.
 */
public class StatResultItem {
    private String tableName;
    private Map<String, Double> statResultMap;
    private Map<String, String> groupFieldValueMapping;
    private boolean isNew;

    public StatResultItem(String tableName, Map<String, Double> statResultMap, Map<String, String> groupFieldValueMapping) {
        this.tableName = tableName;
        this.groupFieldValueMapping = groupFieldValueMapping;
        this.statResultMap = statResultMap;
        this.isNew = false;
    }

    public StatResultItem(String tableName, List<String> statFields, Map<String, String> groupFieldValueMapping) {
        this.tableName = tableName;
        this.groupFieldValueMapping = groupFieldValueMapping;
        statResultMap = new HashMap<String, Double>();
        for (String statField : statFields) {
            statResultMap.put(statField, new Double(0));
        }
        this.isNew = true;
    }


    public String getExecuteSql() {
        if (isNew) {
            StringBuilder stringBuilder = new StringBuilder("INSERT INTO " + tableName + "(");
            StringBuilder valueBuilder = new StringBuilder();
            for (String key : statResultMap.keySet()) {
                stringBuilder.append(key + ",");
                valueBuilder.append("?,");
            }

            for (String key : groupFieldValueMapping.keySet()) {
                stringBuilder.append(key + ",");
                valueBuilder.append("?,");
            }

            return stringBuilder.deleteCharAt(stringBuilder.length() - 1)
                    .append(") VALUES(")
                    .append(valueBuilder.deleteCharAt(valueBuilder.length() - 1)).append(")").toString();
        } else {
            StringBuilder stringBuilder = new StringBuilder("UPDATE " + tableName + " SET ");
            for (String key : statResultMap.keySet()) {
                stringBuilder.append(key + "=?,");
            }
            stringBuilder = stringBuilder.deleteCharAt(stringBuilder.length() - 1).append(" WHERE 1 = 1 ");
            for (String key : groupFieldValueMapping.keySet()) {
                stringBuilder.append("AND " + key + "=?");
            }
            return stringBuilder.toString();
        }
    }

    public void stat(Map<String, String> tupleData) {
        for (Map.Entry<String, Double> entry : statResultMap.entrySet()) {
            if (tupleData.get(entry.getKey()) == null) {
                throw new RuntimeException(entry.getKey() + " is null");
            }

            double result = Double.parseDouble(tupleData.get(entry.getKey()));
            statResultMap.put(entry.getKey(), entry.getValue() + result);
        }
    }

    public void saveStatResult() throws Exception {
        Connection connection = JdbcProxy.getConnection(BaseConstants.JDBC_DEFAULT);
        PreparedStatement preparedStatement = connection.prepareStatement(getExecuteSql());
        if (isNew) {
            int index = 1;
            for (String key : statResultMap.keySet()) {
                preparedStatement.setDouble(index++, statResultMap.get(key));
            }

            for (String key : groupFieldValueMapping.keySet()) {
                preparedStatement.setString(index++, groupFieldValueMapping.get(key));
            }

        } else {
            int index = 1;
            for (String key : statResultMap.keySet()) {
                preparedStatement.setDouble(index++, statResultMap.get(key));
            }

            for (String key : groupFieldValueMapping.keySet()) {
                preparedStatement.setString(index++, groupFieldValueMapping.get(key));
            }
        }

        preparedStatement.executeUpdate();
        connection.commit();
    }
}
