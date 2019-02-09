package com.wangwenjun.strom.example.guaranteed;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class LogSpout extends BaseRichSpout
{
    private List<String> list;

    private SpoutOutputCollector collector;

    private int index;

    private Map<String, String> map;

    private static final Logger LOG = LoggerFactory.getLogger(LogSpout.class);

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector)
    {
        this.collector = collector;
        this.index = 0;
        this.list = Arrays.asList(
                "JAVA,IO",
                "JAVA,THREAD",
                "JAVA,COLLECTION",
                "JAVA,OO",
                "BIG_DATA,HADOOP",
                "BIG_DATA,STORM",
                "BIG_DATA,KAFKA",
                "BIG_DATA,FLUME",
                "SCALA,FUNCTIONAL",
                "SCALA,AKKA",
                "C,OS"
        );

        this.map = new HashMap<>();
    }

    @Override
    public void nextTuple()
    {
        while (index < list.size())
        {
            String messageID = UUID.randomUUID().toString();
            String value = list.get(index++);
            collector.emit(new Values(value), messageID);
            this.map.put(messageID, value);
        }
    }

    @Override
    public void ack(Object msgId)
    {
        LOG.info("Received the success ack messageID:{}.", msgId);
        this.map.remove(msgId);
    }

    @Override
    public void fail(Object msgId)
    {
        LOG.error("Received the failed messageID:{}.", msgId);
        String value = this.map.get(msgId);
        collector.emit(new Values(value), msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("entity"));
    }
}
