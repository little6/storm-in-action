package com.wangwenjun.strom.example.helloworld;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class WrapStarBolt extends BaseBasicBolt
{

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector)
    {
        final String value = tuple.getStringByField("stream");
//        debug("WrapStarBolt-execute:" + value, this);

        System.out.println("****** " + value);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer)
    {
        //no need implement
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context)
    {
//        debug("WrapStarBolt-prepare", this);
    }

    @Override
    public void cleanup()
    {
//        debug("WrapStarBolt-cleanup", this);
    }
}
