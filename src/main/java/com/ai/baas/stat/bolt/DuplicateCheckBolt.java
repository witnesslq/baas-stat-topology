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
import com.ai.baas.storm.message.MappingRule;
import com.ai.baas.storm.message.MessageParser;
import com.ai.baas.storm.util.BaseConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by xin on 16-3-23.
 */
public class DuplicateCheckBolt extends BaseRichBolt {
    private Logger logger = LogManager.getLogger(DuplicateCheckBolt.class);
    private OutputCollector collector;
    private MappingRule[] mappingRules = new MappingRule[1];
    private IDuplicateChecking duplicateChecking;
    private String[] outputFields;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        DuplicateCheckingConfig.getInstance();
        FailBillHandler.startup();
        this.collector = collector;
        FailBillHandler.startup();
        mappingRules[0] = MappingRule.getMappingRule(MappingRule.FORMAT_TYPE_INPUT, BaseConstants.JDBC_DEFAULT);
        duplicateChecking = new DuplicateCheckingFromHBase();
        outputFields = new String[]{BaseConstants.TENANT_ID, BaseConstants.SERVICE_ID, BaseConstants.RECORD_DATA};
    }

    @Override
    public void execute(Tuple input) {
        MessageParser messageParser = null;
        try {
            String line = input.getString(0);
            String[] inputDatas = StringUtils.splitPreserveAllTokens(line, BaseConstants.RECORD_SPLIT);

            for (String inputData : inputDatas) {
                logger.debug("input Data : {}", inputData);
                messageParser = MessageParser.parseObject(inputData, mappingRules, outputFields);
                messageParser.getData();
                List<Object> values = messageParser.toTupleData();
                if (CollectionUtils.isNotEmpty(values)) {
                    collector.emit(values);
                }
            }
        } catch (Exception e) {
            FailBillHandler.addFailBillMsg(input.getString(0), "Stat", "500", e.getMessage());
            collector.ack(input);
        }


        try {
            if (!duplicateChecking.checkData(messageParser.getData())) {
                List<Object> fields = new ArrayList<Object>();
                fields.add(messageParser.getData().get(BaseConstants.TENANT_ID));
                fields.add(messageParser.getData().get(BaseConstants.SERVICE_ID));
                fields.add(messageParser.getData().get(BaseConstants.RECORD_DATA));
                collector.emit(input, fields);
            }
        } catch (Exception e) {
            FailBillHandler.addFailBillMsg(input.getString(0), "Stat", "500", e.getMessage());
        } finally {
            collector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BaseConstants.TENANT_ID, BaseConstants.SERVICE_ID, BaseConstants.RECORD_DATA));
    }
}
