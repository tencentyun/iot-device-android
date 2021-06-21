package com.tencent.iot.hub.device.java.core.mqtt;

import com.tencent.iot.hub.device.java.core.util.AsymcSslUtils;

import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import hub.unit.test.BuildConfig;

import static org.junit.Assert.assertSame;

/**
 * Device Websocket Mqtt connect sample
 */
public class WebsocketMqttSampleTest {

    private static final Logger LOG = LoggerFactory.getLogger(WebsocketMqttSampleTest.class);

    private static String mProductID = BuildConfig.TESTWEBSOCKETMQTTSAMPLE_PRODUCT_ID;
    private static String mDevName = BuildConfig.TESTWEBSOCKETMQTTSAMPLE_DEVICE_NAME;
    private static String mDevPSK = BuildConfig.TESTWEBSOCKETMQTTSAMPLE_DEVICE_PSK;
    private static String mCertFilePath = "DEVICE_CERT_FILE_NAME";           // Device Cert File Name
    private static String mPrivKeyFilePath = "DEVICE_PRIVATE_KEY_FILE_NAME";            // Device Private Key File Name
    private static String mDevCert = "DEVICE_CERT_CONTENT_STRING";           // Cert String
    private static String mDevPriv = "DEVICE_PRIVATE_KEY_CONTENT_STRING";           // Priv String

    private static void websocketdisconnect() {
        try {
            Thread.sleep(2000);
            TXWebSocketManager.getInstance().getClient(mProductID, mDevName, mDevPSK).disconnect();
        } catch (MqttException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void websocketConnect() {

        try {
            // init connection
            MqttConnectOptions conOptions = new MqttConnectOptions();
            conOptions.setCleanSession(true);

            if (mDevPSK != null && mDevPSK.length() != 0) {
                LOG.info("Using PSK");
//                conOptions.setSocketFactory(AsymcSslUtils.getSocketFactory());
            } else if (mDevPriv != null && mDevCert != null && mDevPriv.length() != 0 && mDevCert.length() != 0 && !mDevCert.equals("DEVICE_CERT_CONTENT_STRING") && !mDevPriv.equals("DEVICE_PRIVATE_KEY_CONTENT_STRING")) {
                LOG.info("Using cert stream " + mDevPriv + "  " + mDevCert);
                conOptions.setSocketFactory(AsymcSslUtils.getSocketFactoryByStream(new ByteArrayInputStream(mDevCert.getBytes()), new ByteArrayInputStream(mDevPriv.getBytes())));
            } else {
                LOG.info("Using cert file");
                String workDir = System.getProperty("user.dir") + "/src/test/resources/";
                conOptions.setSocketFactory(AsymcSslUtils.getSocketFactoryByFile(workDir + mCertFilePath, workDir + mPrivKeyFilePath));
            }

            conOptions.setConnectionTimeout(8);
            conOptions.setKeepAliveInterval(60);
            conOptions.setAutomaticReconnect(true);

            TXWebSocketManager.getInstance().getClient(mProductID, mDevName, mDevPSK).setMqttConnectOptions(conOptions);

            TXWebSocketManager.getInstance().getClient(mProductID, mDevName, mDevPSK).setTXWebSocketActionCallback(new TXWebSocketActionCallback() {

                @Override
                public void onConnected() {
                    LOG.debug("onConnected " + TXWebSocketManager.getInstance().getClient(mProductID, mDevName, mDevPSK).getConnectionState());
                    unlock("onConnected");
                }

                @Override
                public void onMessageArrived(String topic, MqttMessage message) {
                    LOG.debug("onMessageArrived topic= " + topic);
                }

                @Override
                public void onConnectionLost(Throwable cause) {
                    LOG.debug("onConnectionLost " + TXWebSocketManager.getInstance().getClient(mProductID, mDevName, mDevPSK).getConnectionState());
                }

                @Override
                public void onDisconnected() {
                    LOG.debug("onDisconnected " + TXWebSocketManager.getInstance().getClient(mProductID, mDevName, mDevPSK).getConnectionState());
                    unlock("onDisconnected");
                }
            });
            TXWebSocketManager.getInstance().getClient(mProductID, mDevName, mDevPSK).connect();
            Thread.sleep(2000);
        } catch (MqttException | InterruptedException e) {
            e.printStackTrace();
            LOG.error("MqttException " + e.toString());
        }
    }

    /** ============================================================================== Unit Test ============================================================================== **/

    private static final int COUNT = 1;
    private static final int TIMEOUT = 3000;
    private static CountDownLatch latch = new CountDownLatch(COUNT);

    private static void lock(String tag) {
        LOG.debug("lock"+tag);
        latch = new CountDownLatch(COUNT);
        try {
            latch.await(TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void unlock(String tag) {
        LOG.debug("unlock"+tag);
        latch.countDown();// 回调执行完毕，唤醒主线程
    }

    @Test
    public void testWebsocketMqttConnect() {
        LogManager.resetConfiguration();
        LOG.isDebugEnabled();
        PropertyConfigurator.configure(WebsocketMqttSampleTest.class.getResource("/log4j.properties"));

        websocketConnect();
        lock("websocketConnect");
        assertSame(TXWebSocketManager.getInstance().getClient(mProductID, mDevName, mDevPSK).getConnectionState(), ConnectionState.CONNECTED);
        LOG.debug("after websocketConnect");

        websocketdisconnect();
        lock("websocketdisconnect");
        assertSame(TXWebSocketManager.getInstance().getClient(mProductID, mDevName, mDevPSK).getConnectionState(), ConnectionState.DISCONNECTED);
        LOG.debug("after websocketdisconnect");
    }
}
