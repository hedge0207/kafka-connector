package org.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class PingSourceConnector extends SourceConnector {

    private String topic;
    public static final String TOPIC_CONFIG = "topic";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, "The topic to publish data to");


    @Override
    public void start(Map<String, String> props) {
        topic = props.get(TOPIC_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PingSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();
        config.put(TOPIC_CONFIG, topic);
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {
        System.out.println("Connector has been stopped");
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return "1.0";
    }
}