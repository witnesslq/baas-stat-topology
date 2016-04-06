package com.ai.baas.stat;

import com.ai.baas.stat.bolt.DuplicateCheckBolt;
import com.ai.baas.stat.bolt.StatBolt;
import com.ai.baas.storm.flow.BaseFlow;
import com.ai.baas.storm.util.BaseConstants;

public class StatFlow extends BaseFlow {

    private String duplicateCheckBoltId = "BAAS-DUPLICATE-BOLT";
    private String stateBoltId = "BAAS-STAT-BOLT";

    @Override
    public void define() {
        super.setKafkaSpout();
        builder.setBolt(duplicateCheckBoltId, new DuplicateCheckBolt()).shuffleGrouping(BaseConstants.KAFKA_SPOUT_NAME);
        builder.setBolt(stateBoltId, new StatBolt()).shuffleGrouping(duplicateCheckBoltId);
    }

    public static void main(String[] args) {
        StatFlow statFlow = new StatFlow();
        statFlow.run(args);
    }
}
