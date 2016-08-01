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
    private static List<String> groupFields;
    private static List<String> statFields;
    private String date;
    private  String groupFieldAll;
    private String statFieldAll;
   
	public String getGroupFieldAll() {
		return groupFieldAll;
	}
	public String getStatFieldAll() {
		return statFieldAll;
	}
	
	public ServiceStatConfig(String date,String tableName, String statID, String groupFields, String statFields, String tableRuleType) {
        this.date = date;
        this.groupFieldAll = groupFields;
        this.statFieldAll = statFields;
    	this.tableName = tableName;
        this.statID = statID;
        ServiceStatConfig.groupFields = buildGroupFields(groupFields,date);
        ServiceStatConfig.statFields = buildStatFields(statFields,date);
        this.tableRuleType = TableRuleType.convert(tableRuleType);
    }
    public List<String> buildStatFieldsAll(String statFields,String date){
    	 try {
//           return Arrays.asList(statFields.split(Constants.StatTopology.STAT_RULE_FIELDS_SPILT));
       	return Constants.getListStatFields(statFields,date);
       } catch (Exception e) {
           logger.error("Failed to split stat fields. Fields[{}].", statFields);
           throw new RuntimeException("Failed to split stat fields. ");
       }
    }
    private List<String> buildStatFields(String statFields,String date) {
        try {
//            return Arrays.asList(statFields.split(Constants.StatTopology.STAT_RULE_FIELDS_SPILT));
        	return Constants.getListStatFields(statFields,date).subList(0, Constants.getListStatFields(statFields,date).size()-1);
        } catch (Exception e) {
            logger.error("Failed to split stat fields. Fields[{}].", statFields);
            throw new RuntimeException("Failed to split stat fields. ");
        }
    }
    public List<String> buildGroupFieldsAll(String groupFields,String date){
   	 try {
//          return Arrays.asList(statFields.split(Constants.StatTopology.STAT_RULE_FIELDS_SPILT));
      	return Constants.getListStatFields(groupFields,date);
      } catch (Exception e) {
    	  logger.error("Failed to split group fields. Fields[{}].", groupFields);
          throw new RuntimeException("Failed to split group fields. ");
      }
   }
    public String getDate() {
		return date;
	}
	private List<String> buildGroupFields(String groupFields,String date) {
        try {
            return Constants.getListStatFields(groupFields,date).subList(0, Constants.getListStatFields(groupFields,date).size()-1);
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
