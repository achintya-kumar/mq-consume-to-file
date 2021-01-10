package com.ultratendency;

import com.ibm.mq.jms.JMSC;
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.mq.jms.MQQueue;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.jms.*;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.*;
import java.security.cert.CertificateException;
import java.util.Map;


public class MqConnectionManager {

    private static final Logger LOG = LogManager.getLogger(MqConnectionManager.class);

    private String mqHostname;
    private Integer mqPort;
    private String mqChannel;
    private String mqQueueManager;
    private String mqQueueName;
    private String keystoreType;
    private String keystorePassword;
    private String keystoreLocation;
    private String cipherSuite;
    private Connection connection;
    private Session session;
    private MessageProducer producer;
    private MessageConsumer consumer;

    public MqConnectionManager(String mqHostname, Integer mqPort,
                               String mqChannel, String mqQueueManager, String mqQueueName,
                               String keystoreType, String keystoreLocation, String keystorePassword,
                               String cipherSuite) throws Exception {
        this.mqHostname = mqHostname;
        this.mqPort = mqPort;
        this.mqChannel = mqChannel;
        this.mqQueueManager = mqQueueManager;
        this.mqQueueName = mqQueueName;
        this.cipherSuite = cipherSuite;

        System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings", "false");
        this.keystoreType = keystoreType;
        this.keystorePassword = keystorePassword;
        this.keystoreLocation = keystoreLocation;

        LOG.info("MqConnectionManager - Creating session ...");
        ConnectionFactory cof = getConnectionFactory();
        connection = cof.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        LOG.info("MqConnectionManager - Initializing producer ...");
        Destination sendDest = getSendDest();
        producer = session.createProducer(sendDest);

        LOG.info("MqConnectionManager - Initializing consumer ...");
        consumer = session.createConsumer(sendDest);
    }

    public MqConnectionManager(Map<String, String> mqConnectionConfigs) throws Exception {
        this(mqConnectionConfigs.get("mqHostName"),
                Integer.parseInt(mqConnectionConfigs.get("mqPort")),
                mqConnectionConfigs.get("mqChannel"),
                mqConnectionConfigs.get("mqQueueManager"),
                mqConnectionConfigs.get("mqQueueName"),
                mqConnectionConfigs.get("keystoreType"),
                mqConnectionConfigs.get("keystoreLocation"),
                mqConnectionConfigs.get("keystorePassword"),
                mqConnectionConfigs.get("cipherSuite"));
    }

    public void sendMQ(String message) throws JMSException {

        TextMessage txtMessage = session.createTextMessage();
        txtMessage.setText(message);
        long duration = System.currentTimeMillis();
        producer.send(txtMessage);
        duration = System.currentTimeMillis() - duration;
        LOG.info("MqConnectionManager - Message sent in " + duration + " [ms]!");

    }

    public String consume() throws JMSException {
        TextMessage textMessage = (TextMessage) consumer.receive();
        return textMessage.getText();
    }

    /**
     * @return
     * @throws JMSException
     */
    private ConnectionFactory getConnectionFactory()
            throws JMSException, NoSuchAlgorithmException, KeyManagementException, CertificateException,
            KeyStoreException, IOException, UnrecoverableKeyException {

        MQConnectionFactory qcf = new MQConnectionFactory();

        qcf.setTransportType(JMSC.MQJMS_TP_CLIENT_MQ_TCPIP);
        qcf.setQueueManager(mqQueueManager);
        qcf.setHostName(mqHostname);
        qcf.setChannel(mqChannel);
        qcf.setPort(mqPort);
        // qcf.setClientID("SVC_MQSeries");
        qcf.setFailIfQuiesce(JMSC.MQJMS_FIQ_YES);

        if(!cipherSuite.isEmpty()) {
            qcf.setSSLCipherSuite(cipherSuite);
            qcf.setSSLSocketFactory(buildSocketFactory());
        }

        return qcf;
    }

    private Destination getSendDest()
            throws JMSException {
        MQQueue dest = new MQQueue(mqQueueName);
        dest.setTargetClient(JMSC.MQJMS_CLIENT_NONJMS_MQ);
        dest.setPersistence(DeliveryMode.NON_PERSISTENT);
        return dest;
    }

    @Override
    protected void finalize() {
        try {
            LOG.info("Closing MQ connection...");
            connection.close();
            session.close();
            producer.close();
            consumer.close();
        } catch (JMSException jmsEx) {
            jmsEx.printStackTrace();
        }
    }

    private SSLSocketFactory buildSocketFactory() throws KeyManagementException, NoSuchAlgorithmException,
            KeyStoreException, IOException, CertificateException, UnrecoverableKeyException {
        SSLContext ctx = SSLContext.getInstance("TLS");

        if(keystoreType.equalsIgnoreCase("jks") && !keystoreLocation.isEmpty()) {
            KeyStore truststore = KeyStore.getInstance(KeyStore.getDefaultType());
            truststore.load(new FileInputStream(keystoreLocation), null);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(truststore);

            KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
            keystore.load(new FileInputStream(keystoreLocation), keystorePassword.toCharArray());
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(keystore, keystorePassword.toCharArray());

            ctx.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        } else {
            throw new RuntimeException("Currently only JKS format is supported");
        }

        return ctx.getSocketFactory();
    }
}