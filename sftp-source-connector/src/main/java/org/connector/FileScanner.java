package org.connector;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import com.jcraft.jsch.*;

public class FileScanner {
    public enum OperationType {
        CREATE("create"),
        UPDATE("update"),
        DELETE("delete");

        private final String operationType;

        OperationType(String operationType) {
            this.operationType = operationType;
        }

        public String getValue() { return operationType; }

    }
    private Date thresholdDateTime;

    public static final String FILE_PATH = "file_path";
    public static final String MODIFIED_TIME = "modified_time";
    public static final String TYPE = "type";
    private ArrayList<Map<String, Object>> chanedFiles;
    private HashSet<String> filesToDelete;
    private ArrayList<String> currentFiles;

    private final SimpleDateFormat DATE_FORMAT = SFTPSourceTask.DATE_FORMAT;
    private final String FILE_RECORD_PATH = SFTPSourceConnector.FILE_RECORD_PATH;

    public FileScanner(Date thresholdDateTime) {
        this.thresholdDateTime = thresholdDateTime;
        this.chanedFiles = new ArrayList<>();
        this.filesToDelete = readAllFileList();
        this.currentFiles = new ArrayList<>();
    }

    private void detectUpsertedFiles(ChannelSftp sftpChannel, String path) throws SftpException {
        Vector<ChannelSftp.LsEntry> fileList = sftpChannel.ls(path);
        for (ChannelSftp.LsEntry entry : fileList) {
            String fileName = entry.getFilename();
            if (".".equals(fileName) || "..".equals(fileName)) {
                continue;
            }

            SftpATTRS attrs = entry.getAttrs();
            String filePath = path + "/" + fileName;
            if (attrs.isDir()) {
                detectUpsertedFiles(sftpChannel, filePath);
            } else {
                Date fileModifiedDate = new Date((attrs.getMTime()) * 1000L);
                filesToDelete.remove(filePath);
                currentFiles.add(filePath);
                if (fileModifiedDate.after(thresholdDateTime)) {
                    Map<String, Object> record = new HashMap<>();
                    record.put(FILE_PATH, filePath);
                    record.put(MODIFIED_TIME, DATE_FORMAT.format(fileModifiedDate));
                    record.put(TYPE, getOperationType(attrs.getMTime(), attrs.getATime(), filePath));

                    System.out.println(record.toString());
                    System.out.println(thresholdDateTime.toString());
                    System.out.println("---------------------------------------------------");
                    chanedFiles.add(record);
                }
            }
        }
    }

    private void detectDeletedFiles() {
        for (String deletedFilePath : filesToDelete) {
            Map<String, Object> record = new HashMap<>();
            record.put(FILE_PATH, deletedFilePath);
            record.put(MODIFIED_TIME, DATE_FORMAT.format(thresholdDateTime));
            record.put(TYPE, OperationType.DELETE.getValue());
            chanedFiles.add(record);
        }
    }

    public ArrayList<Map<String, Object>> listFiles(String host, int port, String user, String password, String remoteDir) {
        try {
            JSch jsch = new JSch();
            Session session = jsch.getSession(user, host, port);
            session.setPassword(password);

            session.setConfig("StrictHostKeyChecking", "no");

            session.connect();

            ChannelSftp sftpChannel = (ChannelSftp) session.openChannel("sftp");
            sftpChannel.connect();

            detectUpsertedFiles(sftpChannel, remoteDir);
            sftpChannel.disconnect();
            session.disconnect();

            detectDeletedFiles();

            writeCurrentFileList();

            return chanedFiles;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private HashSet<String> readAllFileList() {

        HashSet<String> filePathSet = new HashSet<>();
        try (BufferedReader br = new BufferedReader(new FileReader(FILE_RECORD_PATH))) {
            String line;
            while ((line = br.readLine()) != null) {
                filePathSet.add(line.trim());
            }
        } catch (IOException e) {
            System.out.println("Fail to raed record file");
        }
        return filePathSet;
    }

    private void writeCurrentFileList() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(FILE_RECORD_PATH))) {
            for (String filePath : currentFiles) {
                writer.write(filePath);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String getOperationType(int modifiedDate, int createdDate, String filePath) {
        String opertion_type;
        if (modifiedDate == createdDate) {
            opertion_type = OperationType.CREATE.getValue();
        } else {
            opertion_type = OperationType.UPDATE.getValue();
        }
        return opertion_type;
    }
}