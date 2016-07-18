package com.ai.baas.stat.util;

import com.ai.baas.stat.constants.MysqlTableMetaData;
import com.ai.baas.stat.vo.StatResult;
import com.ai.baas.stat.vo.StatResultItem;
import com.ai.baas.stat.vo.rules.ServiceStatConfig;
import com.ai.baas.stat.vo.rules.StatConfig;
import com.ai.baas.storm.failbill.FailBillHandler;
import com.ai.baas.storm.jdbc.JdbcProxy;
import com.ai.baas.storm.util.BaseConstants;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by xin on 16-3-22.
 */
public class DBUtils {
    private static Logger logger = LogManager.getLogger(DBUtils.class);

    private static final String LOAD_STAT_CONFIG_BY_TENANTID_AND_SERIVCETYPE
            = "SELECT " +
            "TENANT_ID, SERVICE_TYPE, STAT_ID,TABLE_NAME,GROUP_FIELDS,ACCU_FIELDS,TABLE_NAME_RULE_TYPE " +
            "FROM stat_accu_rule " +
            "WHERE  TENANT_ID = ? AND SERVICE_TYPE = ?";

    public static StatConfig loadStatConfig(String tenantId, String serviceType,String date) throws Exception {
        PreparedStatement ps = JdbcProxy.getConnection(BaseConstants.JDBC_DEFAULT).prepareStatement(LOAD_STAT_CONFIG_BY_TENANTID_AND_SERIVCETYPE);
        ps.setString(1, tenantId);
        ps.setString(2, serviceType);

        ResultSet resultSet = ps.executeQuery();
        StatConfig statConfig = new StatConfig(tenantId, serviceType);
        while (resultSet.next()) {
            statConfig.addServiceStatConfig(new ServiceStatConfig(
            		date,
                    resultSet.getString(MysqlTableMetaData.STAT_ACC_RULE.FIELDS.TABLE_NAME),
                    resultSet.getString(MysqlTableMetaData.STAT_ACC_RULE.FIELDS.STAT_ID),
                    resultSet.getString(MysqlTableMetaData.STAT_ACC_RULE.FIELDS.GROUP_FIELDS),
                    resultSet.getString(MysqlTableMetaData.STAT_ACC_RULE.FIELDS.ACC_FIELDS),
                    resultSet.getString(MysqlTableMetaData.STAT_ACC_RULE.FIELDS.TABLE_NAME_RULE_TYPE)));
        }

        logger.info("Load the stat config of tenantId[{}], serviceType[{}]. The config is \n {} \n",
                tenantId, serviceType, statConfig);
        return statConfig;
    }

    public static StatResultItem loadStatResult(String tableName, List<String> statFields, Map<String, String> groupFieldValueMapping) throws Exception {
        StatResultItem result = null;
        PreparedStatement ps = JdbcProxy.getConnection(BaseConstants.JDBC_DEFAULT).prepareStatement(generateStatResultSQL(tableName, statFields, groupFieldValueMapping.keySet()));
        int index = 1;
        for (String key : groupFieldValueMapping.keySet()) {
            ps.setString(index++, groupFieldValueMapping.get(key));
        }

        ResultSet resultSet = ps.executeQuery();
        if (resultSet.next()) {
            Map<String, Double> statResult = new HashMap<String, Double>();
            for (String statField : statFields) {
                statResult.put(statField, resultSet.getDouble(statField));
            }
            result = new StatResultItem(tableName, statResult, groupFieldValueMapping);
        } else {
            result = new StatResultItem(tableName, statFields, groupFieldValueMapping);
        }
        return result;
    }

    private static String generateStatResultSQL(String tableName, List<String> statFields, Set<String> groupFields) {
        StringBuilder stringBuilder = new StringBuilder("SELECT ");
        for (String statField : statFields) {
            stringBuilder.append(statField + ",");
        }
        stringBuilder = stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        stringBuilder.append(" FROM " + tableName);
        stringBuilder.append(" WHERE 1=1 ");
        for (String groupField : groupFields) {
            stringBuilder.append(" AND " + groupField + "=? ");
        }
        logger.debug("generateStatResultSQL : " + stringBuilder);
        return stringBuilder.toString();
    }

    public static void batchSaveStatResult(Map<String, StatResult> statResultMap) {
        Iterator<String> iterator = statResultMap.keySet().iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            StatResult statResult = statResultMap.get(key);
            try {
                if (statResult != null)
                    statResult.saveStatResult();
            } catch (Exception e) {
                statResult.doSaveFailedBill(e);
            } finally {
                iterator.remove();
                logger.info("{} has been remove from stat result map, current result map size: {}",
                        key, statResultMap.size());
            }
        }
    }
}
