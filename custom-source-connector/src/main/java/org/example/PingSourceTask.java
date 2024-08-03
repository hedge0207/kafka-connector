package org.example;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PingSourceTask extends SourceTask {

    private String topic;
    private Long streamOffset;

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        topic = props.get(PingSourceConnector.TOPIC_CONFIG);
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        ArrayList<SourceRecord> records = new ArrayList<>();
        Map<String, String> sourcePartition = Collections.singletonMap("foo", "bar");
        Map<String, Long> sourceOffset = Collections.singletonMap("position", streamOffset);
        records.add(new SourceRecord(sourcePartition, sourceOffset, topic, Schema.STRING_SCHEMA, "ping"));
        Thread.sleep(10000);
        System.out.println("Hello World!");
        return records;
    }

    @Override
    public void stop() {
        System.out.println("Bye!");
    }
}
