package com.tencent.iot.explorer.device.java.core.ota;

import com.tencent.iot.explorer.device.java.data_template.TXDataTemplateDownStreamCallBack;
import com.tencent.iot.explorer.device.java.mqtt.TXMqttRequest;
import com.tencent.iot.hub.device.java.core.common.Status;
import com.tencent.iot.hub.device.java.core.mqtt.TXMqttActionCallBack;
import com.tencent.iot.hub.device.java.core.mqtt.TXMqttConstants;

import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import explorer.unit.test.BuildConfig;
import samples.data_template.DataTemplateSample;

import static org.junit.Assert.assertSame;

/**
 * Device OTA sample
 */
public class TestOTASample {

    private static final Logger LOG = LoggerFactory.getLogger(TestOTASample.class);

    private static String mBrokerURL = null;  //传入null，即使用腾讯云物联网通信默认地址 "${ProductId}.iotcloud.tencentdevices.com:8883"  https://cloud.tencent.com/document/product/634/32546
    private static String mProductID = BuildConfig.TESTMQTTSAMPLE_PRODUCT_ID;
    private static String mDevName = BuildConfig.TESTMQTTSAMPLE_DEVICE_NAME;
    private static String mDevPSK  = BuildConfig.TESTMQTTSAMPLE_DEVICE_PSK; //若使用证书验证，设为null

    private static String mDevCert = "DEVICE_CERT_FILE_NAME";           // Device Cert File Name
    private static String mDevPriv = "DEVICE_PRIVATE_KEY_FILE_NAME";            // Device Private Key File Name
    private static AtomicInteger requestID = new AtomicInteger(0);
    private static String mJsonFileName = "struct.json";

    private static DataTemplateSample mDataTemplateSample;

    private static void connect() {
        try {
            Thread.sleep(2000);
            // init connection
            mDataTemplateSample = new DataTemplateSample(mBrokerURL, mProductID, mDevName, mDevPSK, mDevCert, mDevPriv, new SelfMqttActionCallBack(),
                    mJsonFileName, new SelfDownStreamCallBack());
            mDataTemplateSample.connect();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void checkFirmware() {
        try {
            Thread.sleep(2000);
            mDataTemplateSample.checkFirmware();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        connect();

        checkFirmware();
    }

    public static class SelfMqttActionCallBack extends TXMqttActionCallBack {

        @Override
        public void onConnectCompleted(Status status, boolean reconnect, Object userContext, String msg) {
            String userContextInfo = "";
            if (userContext instanceof TXMqttRequest) {
                userContextInfo = userContext.toString();
            }
            String logInfo = String.format("onConnectCompleted, status[%s], reconnect[%b], userContext[%s], msg[%s]",
                    status.name(), reconnect, userContextInfo, msg);
            LOG.info(logInfo);
        }

        @Override
        public void onConnectionLost(Throwable cause) {
            String logInfo = String.format("onConnectionLost, cause[%s]", cause.toString());
            LOG.info(logInfo);
        }

        @Override
        public void onDisconnectCompleted(Status status, Object userContext, String msg) {
            String userContextInfo = "";
            if (userContext instanceof TXMqttRequest) {
                userContextInfo = userContext.toString();
            }
            String logInfo = String.format("onDisconnectCompleted, status[%s], userContext[%s], msg[%s]", status.name(), userContextInfo, msg);
            LOG.info(logInfo);
        }

        @Override
        public void onPublishCompleted(Status status, IMqttToken token, Object userContext, String errMsg) {
            String userContextInfo = "";
            if (userContext instanceof TXMqttRequest) {
                userContextInfo = userContext.toString();
            }
            String logInfo = String.format("onPublishCompleted, status[%s], topics[%s],  userContext[%s], errMsg[%s]",
                    status.name(), Arrays.toString(token.getTopics()), userContextInfo, errMsg);
            LOG.debug(logInfo);
        }

        @Override
        public void onSubscribeCompleted(Status status, IMqttToken asyncActionToken, Object userContext, String errMsg) {
            String userContextInfo = "";
            if (userContext instanceof TXMqttRequest) {
                userContextInfo = userContext.toString();
            }
            String logInfo = String.format("onSubscribeCompleted, status[%s], topics[%s], userContext[%s], errMsg[%s]",
                    status.name(), Arrays.toString(asyncActionToken.getTopics()), userContextInfo, errMsg);
            if (Status.ERROR == status) {
                LOG.error(logInfo);
            } else {
                LOG.debug(logInfo);
            }
        }

        @Override
        public void onUnSubscribeCompleted(Status status, IMqttToken asyncActionToken, Object userContext, String errMsg) {
            String userContextInfo = "";
            if (userContext instanceof TXMqttRequest) {
                userContextInfo = userContext.toString();
            }
            String logInfo = String.format("onUnSubscribeCompleted, status[%s], topics[%s], userContext[%s], errMsg[%s]",
                    status.name(), Arrays.toString(asyncActionToken.getTopics()), userContextInfo, errMsg);
            LOG.debug(logInfo);
        }

        @Override
        public void onMessageReceived(final String topic, final MqttMessage message) {
            String logInfo = String.format("receive message, topic[%s], message[%s]", topic, message.toString());
            LOG.debug(logInfo);
        }
    }

    /**
     * 实现下行消息处理的回调接口
     */
    private static class SelfDownStreamCallBack extends TXDataTemplateDownStreamCallBack {
        @Override
        public void onReplyCallBack(String replyMsg) {
            //可根据自己需求进行处理属性上报以及事件的回复，根据需求填写
            LOG.debug("reply received : " + replyMsg);
        }

        @Override
        public void onGetStatusReplyCallBack(JSONObject data) {
            //可根据自己需求进行处理状态和控制信息的获取结果
            LOG.debug("event down stream message received : " + data);
        }

        @Override
        public JSONObject onControlCallBack(JSONObject msg) {
            LOG.debug("control down stream message received : " + msg);
            //do something

            //output
            try {
                JSONObject result = new JSONObject();
                result.put("code",0);
                result.put("status", "some message wher errorsome message when error");
                return result;
            } catch (JSONException e) {
                LOG.error("Construct params failed!");
                return null;
            }
        }

        @Override
        public  JSONObject onActionCallBack(String actionId, JSONObject params){
            LOG.debug("action [{}] received, input:" + params, actionId);
            //do something based action id and input
            if(actionId.equals("blink")) {
                try {
                    Iterator<String> it = params.keys();
                    while (it.hasNext()) {
                        String key = it.next();
                        LOG.debug("Input parameter[{}]:" + params.get(key), key);
                    }
                    //construct result
                    JSONObject result = new JSONObject();
                    result.put("code",0);
                    result.put("status", "some message wher errorsome message when error");

                    // response based on output
                    JSONObject response = new JSONObject();
                    response.put("result", 0);

                    result.put("response", response);
                    return result;
                } catch (JSONException e) {
                    return null;
                }
            } else if (actionId.equals("YOUR ACTION")) {
                //do your action
            }
            return null;
        }
    }

    /** ============================================================================== Unit Test ============================================================================== **/

    private static Object mLock = new Object(); // 同步锁
    private static int mCount = 0; // 加解锁条件
    private static boolean mUnitTest = false;

    private static void lock() {
        synchronized (mLock) {
            mCount = 1;  // 设置锁条件
            while (mCount > 0) {
                try {
                    mLock.wait(); // 等待唤醒
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void unlock() {
        if (mUnitTest) {
            synchronized (mLock) {
                mCount = 0;
                mLock.notifyAll(); // 回调执行完毕，唤醒主线程
            }
        }
    }

    @Test
    public void testOTA() {
        mUnitTest = true;
        LogManager.resetConfiguration();
        LOG.isDebugEnabled();
        PropertyConfigurator.configure(TestOTASample.class.getResource("/log4j.properties"));

        connect();
        LOG.debug("after connect");

        checkFirmware();
        assertSame(mDataTemplateSample.getConnectStatus(), TXMqttConstants.ConnectStatus.kConnected);
    }
}
