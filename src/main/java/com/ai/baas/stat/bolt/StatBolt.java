package com.ai.baas.stat.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.ai.baas.stat.constants.Constants;
import com.ai.baas.stat.util.DBUtils;
import com.ai.baas.stat.vo.StatResult;
import com.ai.baas.stat.vo.rules.ServiceStatConfig;
import com.ai.baas.stat.vo.rules.StatConfig;
import com.ai.baas.storm.failbill.FailBillHandler;
import com.ai.baas.storm.jdbc.JdbcProxy;
import com.ai.baas.storm.message.MappingRule;
import com.ai.baas.storm.message.MessageParser;
import com.ai.baas.storm.util.BaseConstants;
import com.ai.baas.storm.util.HBaseProxy;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

public class StatBolt extends BaseRichBolt {

    private Logger logger = LogManager.getLogger(StatBolt.class);
    private boolean hasBeanChanged = false;
    private static Object lock = new Object();
    // key: tenantId_serviceType
    private Map<String, StatConfig> statRules;
    // key: tenantId_serviceType
    private Map<String, StatResult> statResultMap;

    private OutputCollector outputCollector;
    private MappingRule[] mappingRules = new MappingRule[1];
    private String[] outputFields = new String[]{BaseConstants.RECORD_DATA};
    private AtomicInteger index = new AtomicInteger();

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        JdbcProxy.loadDefaultResource(stormConf);
        HBaseProxy.loadResource(stormConf);
        FailBillHandler.startup();
        //获取映射关系
        mappingRules[0] = MappingRule.getMappingRule(MappingRule.FORMAT_TYPE_INPUT, BaseConstants.JDBC_DEFAULT);

        statRules = new HashMap<String, StatConfig>();
        statResultMap = new HashMap<String, StatResult>();
        this.outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            MessageParser messageParser = null;
            try {
                String line = input.getStringByField(BaseConstants.RECORD_DATA);
                String[] inputDataArr = StringUtils.splitPreserveAllTokens(line, BaseConstants.RECORD_SPLIT);

                if (inputDataArr == null) {
                    throw new RuntimeException("Tuple data is null");
                }

                for (String inputData : inputDataArr) {
                    messageParser = MessageParser.parseObject(inputData, mappingRules, outputFields);
                }
            } catch (Exception e) {
                logger.error("Failed to convert tuple data to map", e);
                throw new RuntimeException("Failed to convert tuple data to map.", e);
            }
            System.out.println("do stat action :" + messageParser.getData());
            doStatAction(input, messageParser.getData());
        } catch (Exception e) {
            logger.error("Failed to stat the {}", input.getString(0), e);
            FailBillHandler.addFailBillMsg(input.getStringByField(BaseConstants.RECORD_DATA), "Stat", "500", e.getMessage());
        } finally {
            outputCollector.ack(input);
        }


    }

    private void doStatAction(Tuple input, Map<String, String> tupleData) {
    	//tupleData.get(BaseConstants.SERVICE_ID) AMOUNT
        String key = tupleData.get(BaseConstants.TENANT_ID) + tupleData.get(BaseConstants.SERVICE_ID);
        String date = tupleData.get(BaseConstants.START_TIME);
        StatConfig config = statRules.get(key);
        // 不存在
        
        try {
            config = DBUtils.loadStatConfig(tupleData.get(BaseConstants.TENANT_ID),
                    tupleData.get(BaseConstants.SERVICE_ID),date);
        } catch (Exception e) {
            logger.error("Failed to load the stat rule of  tenantId[{}] serviceType[{}].",
                    tupleData.get(BaseConstants.TENANT_ID), tupleData.get(BaseConstants.SERVICE_ID), e);
            throw new RuntimeException("Failed to load the stat rule.", e);
        }
        statRules.put(key, config);


        StatResult statResult = statResultMap.get(key);
        if (statResult == null) {
            try {
            	//将衍生字段添加进来
            	Map<String, ServiceStatConfig> serviceStatConfigs = config.getServiceStatConfigs();
            	Iterator<String> iterator = serviceStatConfigs.keySet().iterator();
            	while (iterator.hasNext()) {
					String serviceStatConfigKey = (String) iterator.next();
					ServiceStatConfig serviceStatConfig = serviceStatConfigs.get(serviceStatConfigKey);
		        	List<String> groupFields = serviceStatConfig.getGroupFields();
		        	List<String> buildGroupFieldsAll = serviceStatConfig.buildGroupFieldsAll(
		        			serviceStatConfig.getGroupFieldAll(), 
		        			serviceStatConfig.getDate());
		        	tupleData.put(groupFields.get(groupFields.size()-1), buildGroupFieldsAll.get(buildGroupFieldsAll.size()-1));
		        	List<String> statFields = serviceStatConfig.getStatFields();
		        	List<String> buildStatFieldsAll = serviceStatConfig.buildStatFieldsAll(
		        			serviceStatConfig.getStatFieldAll(), serviceStatConfig.getDate());
		        	tupleData.put(statFields.get(statFields.size()-1), tupleData.get(buildStatFieldsAll.get(buildStatFieldsAll.size()-1)));
		            statResult = StatResult.load(config, tupleData);
		            statResultMap.put(key, statResult);
            	}
            } catch (Exception e) {
                logger.error("Failed to load the stat result of config[{}].",
                        tupleData.get(BaseConstants.TENANT_ID), tupleData.get(BaseConstants.SERVICE_ID), config, e);
                throw new RuntimeException("Failed to load the stat result of config", e);
            }
        }

        try {
            statResult.stat(config, tupleData, input);
            index.getAndIncrement();
        } catch (Exception e) {
            logger.error("Failed to stat result of config[{}].tupleData[{}]", config, input, e);
            throw new RuntimeException("Failed to stat result of config");
        }
        System.out.println("Index : " + index);
        if (index.get() >= 1) {
            DBUtils.batchSaveStatResult(statResultMap);
            index.set(0);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(outputFields));
    }

}
