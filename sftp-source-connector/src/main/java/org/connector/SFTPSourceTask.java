package org.connector;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class SFTPSourceTask extends SourceTask {

    private String topic;
    private String host;
    private int port;
    private String user;
    private String password;
    private String remoteDirectory;
    private String baseDate;
    private int interval_ms;

    private final String TIME_FILE_PATH = SFTPSourceConnector.TIME_FILE_PATH;
    public static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private final SchemaConverter schemaConverter;
    private final StructConverter structConverter;

    private Long streamOffset;

    public SFTPSourceTask() {
        this.schemaConverter = new SchemaConverter();
        this.structConverter = new StructConverter();
    }

    @Override
    public String version() {
        return "1.0";
    }

    @Override
    public void start(Map<String, String> props) {
        topic = props.get(SFTPSourceConnector.TOPIC_CONFIG);
        host = props.get(SFTPSourceConnector.HOST_CONFIG);
        port = Integer.parseInt(props.get(SFTPSourceConnector.PORT_CONFIG));
        user = props.get(SFTPSourceConnector.USER_CONFIG);
        password = props.get(SFTPSourceConnector.PASSWORD_CONFIG);
        remoteDirectory = props.get(SFTPSourceConnector.REMOTE_DIR_CONFIG);
        baseDate = props.get(SFTPSourceConnector.BASE_DATE_CONFIG);
        interval_ms = Integer.parseInt(props.get(SFTPSourceConnector.INTERVAL_MS_CONFIG));
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        ArrayList<SourceRecord> records = new ArrayList<>();
        Map<String, String> sourcePartition = Collections.singletonMap("foo", "bar");
        Map<String, Long> sourceOffset = Collections.singletonMap("position", streamOffset);

        Date thresholdDateTime = getThresholdDateTime();

        System.out.println(thresholdDateTime.toString());
        FileScanner fileScanner = new FileScanner(thresholdDateTime);
        writeDateToFile(new Date());
        ArrayList<Map<String, Object>> changedFiles = fileScanner.listFiles(host, port, user, password, remoteDirectory);
        for (Map<String, Object> record : changedFiles) {
            Schema schema = schemaConverter.convert(record);
            Struct struct = structConverter.convert(record, schema);
            System.out.println(record);
            System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");
            records.add(new SourceRecord(sourcePartition, sourceOffset, topic, schema, struct));
        }
        Thread.sleep(interval_ms);
        System.out.println("iter!!!!!!!!!!!!!!!");
        return records;
    }

    @Override
    public void stop() {
        System.out.println("Bye!");
    }

    private Date getThresholdDateTime() {
        Date thresholdDateTime;
        Date lastScanDate = readDateFromFile();
        if (lastScanDate == null) {
            try {
                thresholdDateTime = DATE_FORMAT.parse(baseDate);
            } catch (ParseException e) {
                throw new RuntimeException("Invalid date format");
            }
        } else {
            thresholdDateTime = lastScanDate;
        }
        return thresholdDateTime;
    }

    private Date readDateFromFile() {
        try (BufferedReader br = new BufferedReader(new FileReader(TIME_FILE_PATH))) {
            return DATE_FORMAT.parse(br.readLine());
        } catch (IOException e) {
            System.out.println("Fail to read date file");;
        } catch (ParseException e) {
            System.out.println("Fail to parse date file");
        }
        return null;
    }

    private void writeDateToFile(Date dateTime) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(TIME_FILE_PATH))) {
            bw.write(DATE_FORMAT.format(dateTime));
        } catch (IOException e) {
            System.out.println("Fail to write date file");
        }
    }
}
