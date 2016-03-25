package com.ai.baas.stat;

import com.ai.baas.stat.bolt.DuplicateCheckBolt;
import com.ai.baas.stat.bolt.StatBolt;
import com.ai.baas.storm.flow.BaseFlow;

public class StatFlow extends BaseFlow {

    private String duplicateCheckBoltId = "BAAS-DUPLICATE-BOLT";
    private String stateBoltId = "BAAS-STAT-BOLT";

    @Override
    public void define() {
        builder.setBolt(duplicateCheckBoltId, new DuplicateCheckBolt());
        builder.setBolt(stateBoltId, new StatBolt());
    }

    public static void main(String[] args) {
        StatFlow statFlow = new StatFlow();
        statFlow.run(args);
    }
}
