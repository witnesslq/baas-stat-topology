package com.ai.baas.stat.vo;

import backtype.storm.tuple.Tuple;
import com.ai.baas.stat.vo.rules.ServiceStatConfig;
import com.ai.baas.stat.vo.rules.StatConfig;
import com.ai.baas.storm.failbill.FailBillHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by xin on 16-3-23.
 */
public class StatResult {
    private Logger logger = LogManager.getLogger(StatResult.class);

    private List<Tuple> hasBeenStatTuples;
    // key: statID
    private Map<String, ServiceStatResult> serviceStatResultMap;

    public StatResult() {
        hasBeenStatTuples = new ArrayList<Tuple>();
        serviceStatResultMap = new HashMap<String, ServiceStatResult>();
    }

    public void stat(StatConfig config, Map<String, String> tupleData, Tuple input) throws Exception {
        for (Map.Entry<String, ServiceStatConfig> entry : config.getServiceStatConfigs().entrySet()) {
            ServiceStatResult serviceStatResult = serviceStatResultMap.get(entry.getKey());
            if (serviceStatResult == null) {
                throw new RuntimeException("Failed to load the statID[" + entry.getKey() + "]");
            }

            serviceStatResult.stat(entry.getValue(), tupleData);
        }


        hasBeenStatTuples.add(input);
    }


    public void saveStatResult() throws Exception {
        for (Map.Entry<String, ServiceStatResult> entry : serviceStatResultMap.entrySet())
            entry.getValue().saveStatResult();
    }


    public static StatResult load(StatConfig config, Map<String, String> tupleData) throws Exception {
        StatResult statResult = new StatResult();
        for (Map.Entry<String, ServiceStatConfig> entry : config.getServiceStatConfigs().entrySet()) {
            statResult.serviceStatResultMap.put(
                    entry.getKey(),
                    ServiceStatResult.load(entry.getValue(), tupleData)
            );
        }
        return statResult;
    }

    public void doSaveFailedBill(Exception e) {
        Iterator<Tuple> tupleIterable = hasBeenStatTuples.iterator();
        while (tupleIterable.hasNext()) {
            Tuple tuple = tupleIterable.next();
            FailBillHandler.addFailBillMsg(tuple.getString(0), "Stat", "500", e.getMessage());
        }
    }
}
