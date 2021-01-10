package com.ultratendency;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class MqConsumer {
    private static final Logger LOG = LogManager.getLogger(MqConsumer.class);

    public static void main(String[] args) throws Exception {
        String mqHostname = args[0];
        Integer mqPort = Integer.parseInt(args[1]);
        String mqChannel = args[2];
        String mqQueueManager = args[3];
        String mqQueueName = args[4];
        String mqKeystoreType = args[5];
        String mqKeystorePassword = args[6];
        String mqKeyStoreLocation = args[7];
        String cipherSuite = args[8];
        MqConnectionManager connectionManager = new MqConnectionManager(mqHostname, mqPort, mqChannel,
                mqQueueManager, mqQueueName, mqKeystoreType, mqKeyStoreLocation, mqKeystorePassword, cipherSuite);

        String outPutFilename = "consumption.out";
        if (Files.exists(Paths.get(outPutFilename))) {
            Files.write((new File(outPutFilename)).toPath(), new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
        } else {
            new File(outPutFilename).createNewFile();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            connectionManager.finalize();
            LOG.info("Terminating consumption from MQ");
        }));

        for(int i = 0;;i++) {
            String consumedMessage = connectionManager.consume();
            System.out.println(i + ", " + consumedMessage);
            Files.write(
                    Paths.get(outPutFilename),
                    consumedMessage.getBytes(),
                    StandardOpenOption.APPEND);
        }
    }
}
