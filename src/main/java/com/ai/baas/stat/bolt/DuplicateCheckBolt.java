package com.ai.baas.stat.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.ai.baas.storm.duplicate.DuplicateCheckingConfig;
import com.ai.baas.storm.duplicate.DuplicateCheckingFromHBase;
import com.ai.baas.storm.duplicate.IDuplicateChecking;
import com.ai.baas.storm.failbill.FailBillHandler;
import com.ai.baas.storm.jdbc.JdbcProxy;
import com.ai.baas.storm.message.MappingRule;
import com.ai.baas.storm.message.MessageParser;
import com.ai.baas.storm.util.BaseConstants;
import com.ai.baas.storm.util.HBaseProxy;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Created by xin on 16-3-23.
 */
public class DuplicateCheckBolt extends BaseRichBolt {
    private Logger logger = LogManager.getLogger(DuplicateCheckBolt.class);
    private OutputCollector collector;
    private MappingRule[] mappingRules = new MappingRule[2];
    private IDuplicateChecking duplicateChecking;
    private String[] outputFields;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        JdbcProxy.loadDefaultResource(stormConf);
        HBaseProxy.loadResource(stormConf);
        DuplicateCheckingConfig.getInstance();
        FailBillHandler.startup();
        this.collector = collector;
        mappingRules[0] = MappingRule.getMappingRule(MappingRule.FORMAT_TYPE_INPUT, BaseConstants.JDBC_DEFAULT);
        mappingRules[1] = mappingRules[0];
        duplicateChecking = new DuplicateCheckingFromHBase();
        outputFields = new String[]{BaseConstants.TENANT_ID, BaseConstants.SERVICE_ID, BaseConstants.RECORD_DATA};
    }

    @Override
    public void execute(Tuple input) {
        MessageParser messageParser = null;
        try {
            String line = input.getString(0);
            String[] inputDatas = StringUtils.splitPreserveAllTokens(line, BaseConstants.RECORD_SPLIT);
            System.out.println("Input Data size :" + inputDatas.length + ". line: {}" + line);
            for (String inputData : inputDatas) {
                logger.debug("input Data : {}", inputData);
                messageParser = MessageParser.parseObject(inputData, mappingRules, outputFields);
            }
        } catch (Exception e) {
            e.printStackTrace();
            FailBillHandler.addFailBillMsg(input.getStringByField(BaseConstants.RECORD_DATA), "Stat", "500", e.getMessage());
            collector.ack(input);
        }

        try {
        	boolean checkData = duplicateChecking.checkData(messageParser.getData());
        	System.out.println("========="+checkData);
            if (!checkData) {
                List<Object> values = messageParser.toTupleData();
                if (CollectionUtils.isNotEmpty(values)) {
                    collector.emit(values);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            FailBillHandler.addFailBillMsg(input.getStringByField(BaseConstants.RECORD_DATA), "Stat", "500", e.getMessage());
        } finally {
            collector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BaseConstants.TENANT_ID, BaseConstants.SERVICE_ID, BaseConstants.RECORD_DATA));
    }
}
