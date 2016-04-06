package com.ai.baas.bmc.topology.test;

import com.ai.baas.storm.util.BaseConstants;
import org.apache.commons.lang.StringUtils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class BaasClientProducer {
    private String encoding = "UTF-8";
    public final static String FIELD_SPLIT = new String(new char[]{(char) 1});
    public final static String RECORD_SPLIT = new String(new char[]{(char) 2});
    public final static String PACKET_HEADER_SPLIT = ",";
    private String service_id = "AMOUNT";//PILE
    private String tenant_id = "TEST";
    private String fileName = "";

    public void send() {
        String message = assembleMessage();
        System.out.println("message----"+message);
        ProducerProxy.getInstance().sendMessage(message);
    }

    private String assembleMessage() {
        StringBuilder busData = new StringBuilder();
        busData.append(tenant_id).append(FIELD_SPLIT);
        busData.append(service_id).append(FIELD_SPLIT);
        busData.append("TestSource").append(FIELD_SPLIT);
        busData.append("Test0").append(FIELD_SPLIT);
        busData.append("test0").append(FIELD_SPLIT);
        busData.append(new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(System.currentTimeMillis()))).append(FIELD_SPLIT);
        busData.append(new SimpleDateFormat("yyyyMMddHHmmss").format(new Date(System.currentTimeMillis()))).append(FIELD_SPLIT);
        //
        busData.append("zx").append(FIELD_SPLIT);
        busData.append("bj").append(FIELD_SPLIT);
        busData.append("10").append(FIELD_SPLIT);

        return busData.substring(0, busData.length() - 1).toString();
    }


    public static void main(String[] args) {
        BaasClientProducer simulator = new BaasClientProducer();
        simulator.send();
        //String[] adat = StringUtils.splitPreserveAllTokens(simulator.assembleMessage(), BaseConstants.RECORD_SPLIT);
        //System.out.println(adat.length);
    }

}
