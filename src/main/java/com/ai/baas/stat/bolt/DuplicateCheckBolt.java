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
import com.ai.baas.storm.message.MappingRule;
import com.ai.baas.storm.message.MessageParser;
import com.ai.baas.storm.util.BaseConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by xin on 16-3-23.
 */
public class DuplicateCheckBolt extends BaseRichBolt {
    private OutputCollector collector;
    private MappingRule[] mappingRules = new MappingRule[2];
    private IDuplicateChecking duplicateChecking;
    //TODO
    private String[] outputFields;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        DuplicateCheckingConfig.getInstance();
        this.collector = collector;
        duplicateChecking = new DuplicateCheckingFromHBase();
    }

    @Override
    public void execute(Tuple input) {
        MessageParser messageParser = null;
        try {
            String line = input.getString(0);
            String[] inputDatas = StringUtils.splitPreserveAllTokens(line, BaseConstants.RECORD_SPLIT);

            for (String inputData : inputDatas) {
                messageParser = MessageParser.parseObject(inputData, mappingRules, outputFields);
                messageParser.getData();
                List<Object> values = messageParser.toTupleData();
                if (CollectionUtils.isNotEmpty(values)) {
                    collector.emit(values);
                }
            }
        } catch (Exception e) {
            //logger.error("Failed to convert tuple data to map", e);
            //TODO 入错单
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
            e.printStackTrace();
        } finally {
            collector.ack(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(BaseConstants.TENANT_ID, BaseConstants.SERVICE_ID, BaseConstants.RECORD_DATA));
    }
}
