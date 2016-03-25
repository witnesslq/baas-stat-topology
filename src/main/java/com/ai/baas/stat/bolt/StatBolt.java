package com.ai.baas.stat.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.ai.baas.stat.util.DBUtils;
import com.ai.baas.stat.vo.StatResult;
import com.ai.baas.stat.vo.rules.StatConfig;
import com.ai.baas.storm.jdbc.JdbcProxy;
import com.ai.baas.storm.message.MappingRule;
import com.ai.baas.storm.message.MessageParser;
import com.ai.baas.storm.util.BaseConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class StatBolt extends BaseRichBolt {

    private Logger logger = LogManager.getLogger(StatBolt.class);
    private boolean hasBeanChanged = false;
    private static Object lock = new Object();
    // key: tenantId_serviceType
    private Map<String, StatConfig> statRules;
    // key: tenantId_serviceType
    private Map<String, StatResult> statResultMap;

    private OutputCollector outputCollector;
    private MappingRule[] mappingRules = new MappingRule[2];
    private String[] outputFields;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        mappingRules[0] = MappingRule.getMappingRule(MappingRule.FORMAT_TYPE_INPUT, BaseConstants.JDBC_DEFAULT);
        mappingRules[1] = mappingRules[0];

        statRules = new HashMap<String, StatConfig>();
        statResultMap = new HashMap<String, StatResult>();
        JdbcProxy.loadDefaultResource(stormConf);
        this.outputCollector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            MessageParser messageParser = null;
            try {
                String line = input.getString(0);
                String[] inputDatas = StringUtils.splitPreserveAllTokens(line, BaseConstants.RECORD_SPLIT);

                for (String inputData : inputDatas) {
                    messageParser = MessageParser.parseObject(inputData, mappingRules, outputFields);
                }
            } catch (Exception e) {
                logger.error("Failed to convert tuple data to map", e);
                throw new RuntimeException("Failed to convert tuple data to map.", e);
            }

            Map<String, String> tupleData = messageParser.getData();
            String key = tupleData.get(BaseConstants.TENANT_ID) + tupleData.get(BaseConstants.SERVICE_ID);
            StatConfig config = statRules.get(key);
            // 不存在
            if (config == null) {
                try {
                    config = DBUtils.loadStatConfig(tupleData.get(BaseConstants.TENANT_ID),
                            tupleData.get(BaseConstants.SERVICE_ID));
                } catch (Exception e) {
                    logger.error("Failed to load the stat rule of  tenantId[{}] serviceType[{}].",
                            tupleData.get(BaseConstants.TENANT_ID), tupleData.get(BaseConstants.SERVICE_ID), e);
                    throw new RuntimeException("Failed to load the stat rule.", e);
                }
                statRules.put(key, config);
            }


            StatResult statResult = statResultMap.get(key);
            if (statResult == null) {
                try {
                    statResult = StatResult.load(config, tupleData);
                } catch (Exception e) {
                    logger.error("Failed to load the stat result of config[{}].",
                            tupleData.get(BaseConstants.TENANT_ID), tupleData.get(BaseConstants.SERVICE_ID), config, e);
                    throw new RuntimeException("Failed to load the stat result of config", e);
                }
            }

            try {
                statResult.stat(config, input);
            } catch (Exception e) {
                logger.error("Failed to stat result of config[{}].tupleData[{}]", config, input, e);
                throw new RuntimeException("Failed to stat result of config");
            }

            if (statResultMap.size() > 5) {
                DBUtils.batchSaveStatResult(statResultMap);
            }

        } catch (Exception e) {
            //TODO 入错单
            
        } finally {
            outputCollector.ack(input);
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(outputFields));
    }

}