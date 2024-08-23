package org.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SFTPSourceConnector extends SourceConnector {

    public static final String TIME_FILE_PATH = "/home/appuser/time.txt";
    public static final String FILE_RECORD_PATH = "/home/appuser/all_files.txt";
    private String topic;
    private String host;
    private String port;
    private String user;
    private String password;
    private String remoteDirectory;
    private String baseDate;
    private String interval_ms;
    public static final String TOPIC_CONFIG = "topic";
    public static final String HOST_CONFIG = "host";
    public static final String PORT_CONFIG = "port";
    public static final String USER_CONFIG = "user";
    public static final String PASSWORD_CONFIG = "password";
    public static final String REMOTE_DIR_CONFIG = "remote_directory";
    public static final String BASE_DATE_CONFIG = "base_date";
    public static final String INTERVAL_MS_CONFIG = "interval_ms";

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPIC_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, "The topic to publish data to")
            .define(HOST_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, "The host to connect")
            .define(PORT_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, "The port to connect")
            .define(USER_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, "The user to connect")
            .define(PASSWORD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, "The password to connect")
            .define(REMOTE_DIR_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, "The remote directory to connect")
            .define(BASE_DATE_CONFIG, ConfigDef.Type.STRING, "1970-01-01 00:00:00", new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, "The base time to detect files")
            .define(INTERVAL_MS_CONFIG, ConfigDef.Type.STRING, "60000", new ConfigDef.NonEmptyString(), ConfigDef.Importance.HIGH, "The interval between detection");

    @Override
    public void start(Map<String, String> props) {
        topic = props.get(TOPIC_CONFIG);
        host = props.get(HOST_CONFIG);
        port = props.get(PORT_CONFIG);
        user = props.get(USER_CONFIG);
        password = props.get(PASSWORD_CONFIG);
        remoteDirectory = props.get(REMOTE_DIR_CONFIG);
        baseDate = props.get(BASE_DATE_CONFIG);
        interval_ms = props.get(INTERVAL_MS_CONFIG);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SFTPSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        Map<String, String> config = new HashMap<>();
        config.put(TOPIC_CONFIG, topic);
        config.put(HOST_CONFIG, host);
        config.put(PORT_CONFIG, port);
        config.put(USER_CONFIG, user);
        config.put(PASSWORD_CONFIG, password);
        config.put(REMOTE_DIR_CONFIG, remoteDirectory);
        config.put(BASE_DATE_CONFIG, baseDate);
        config.put(INTERVAL_MS_CONFIG, interval_ms);
        configs.add(config);
        return configs;
    }

    @Override
    public void stop() {
        try {
            deleteFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Bye!!!!!");
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public String version() {
        return "1.0";
    }

    private void deleteFile() throws IOException {
        Files.delete(Paths.get(TIME_FILE_PATH));
    }
}