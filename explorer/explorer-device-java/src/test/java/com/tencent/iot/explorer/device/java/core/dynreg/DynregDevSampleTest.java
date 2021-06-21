package com.tencent.iot.explorer.device.java.core.dynreg;

import com.tencent.iot.explorer.device.java.data_template.TXDataTemplateDownStreamCallBack;
import com.tencent.iot.explorer.device.java.mqtt.TXMqttRequest;
import com.tencent.iot.hub.device.java.core.common.Status;
import com.tencent.iot.hub.device.java.core.dynreg.TXMqttDynreg;
import com.tencent.iot.hub.device.java.core.dynreg.TXMqttDynregCallback;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import explorer.unit.test.BuildConfig;
import com.tencent.iot.explorer.device.java.core.data_template.DataTemplateSample;

import static org.junit.Assert.assertSame;

/**
 * Device Dynreg sample
 */
public class DynregDevSampleTest {

    private static final Logger LOG = LoggerFactory.getLogger(DynregDevSampleTest.class);

    private static final String TAG = "TXMQTT";

    private static String path2Store = System.getProperty("user.dir");

    private static String mBrokerURL = null;  //传入null，即使用腾讯云物联网通信默认地址 "${ProductId}.iotcloud.tencentdevices.com:8883"  https://cloud.tencent.com/document/product/634/32546

    private static final String GW_OPERATION_RES_PREFIX = "$gateway/operation/result/";

    private static String mProductID = BuildConfig.TESTDYNREGDEVSAMPLE_PRODUCT_ID;
    private static String mDevName = BuildConfig.TESTDYNREGDEVSAMPLE_DEVICE_NAME;
    private static String mDevPSK = "DEVICE_PSK";
    private static String mProductSecret = BuildConfig.TESTDYNREGDEVSAMPLE_PRODUCT_SECRET;             // Used for dynamic register
    private static String mDevCert = "DEVICE_CERT_FILE_NAME";           // Device Cert File Name
    private static String mDevPriv = "DEVICE_PRIVATE_KEY_FILE_NAME";            // Device Private Key File Name

    private static String mJsonFileName = "struct.json";

    private static DataTemplateSample mDataTemplateSample;

    private static void connect() {
        // init connection
        mDataTemplateSample = new DataTemplateSample(mBrokerURL, mProductID, mDevName, mDevPSK, mDevCert, mDevPriv, new SelfMqttActionCallBack(),
                mJsonFileName, new SelfDownStreamCallBack());
        mDataTemplateSample.connect();
    }

    private static void dynReg() {
        LOG.debug("Test Dynamic");
        TXMqttDynreg dynreg = new TXMqttDynreg(mProductID, mProductSecret, mDevName, new SelfMqttDynregCallback());
        if (dynreg.doDynamicRegister()) {
            LOG.debug("Dynamic Register OK!");
        } else {
            LOG.error("Dynamic Register failed!");
        }
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
            unlock("onConnectCompleted");
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
            unlock("onDisconnectCompleted");
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

        @Override
        public void onUnbindDeviceCallBack(String msg) {
            //可根据自己需求进行用户删除设备的通知消息处理的回复，根据需求填写
            LOG.debug("unbind device received : " + msg);
        }
    }

    /**
     * Callback for dynamic register
     */
    private static class SelfMqttDynregCallback extends TXMqttDynregCallback {

        @Override
        public void onGetDevicePSK(String devicePsk) {
            mDevPSK = devicePsk;
            String logInfo = String.format("Dynamic register OK! onGetDevicePSK, devicePSK");
            LOG.info(logInfo);
            connect();
        }

        @Override
        public void onGetDeviceCert(String deviceCert, String devicePriv) {
//            mDevCert = deviceCert;   //这里获取的是证书内容字符串 创建对应ssl认证时可使用options.setSocketFactory(AsymcSslUtils.getSocketFactoryByStream(new ByteArrayInputStream(mDevCert.getBytes()), new ByteArrayInputStream(mDevPriv.getBytes())));方式，示例中使用的是读取本地文件路径的方式。
//            mDevPriv = devicePriv;   //这里获取的是秘钥内容字符串
            String logInfo = String.format("Dynamic register OK!onGetDeviceCert, deviceCert devicePriv");
            LOG.info(logInfo);
            connect();
        }

        @Override
        public void onFailedDynreg(Throwable cause, String errMsg) {
            String logInfo = String.format("Dynamic register failed! onFailedDynreg, ErrMsg[%s]", cause.toString() + errMsg);
            LOG.error(logInfo);
        }

        @Override
        public void onFailedDynreg(Throwable cause) {
            String logInfo = String.format("Dynamic register failed! onFailedDynreg, ErrMsg[%s]", cause.toString());
            LOG.error(logInfo);
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
    public void testDynregDev() {
        LogManager.resetConfiguration();
        LOG.isDebugEnabled();
        PropertyConfigurator.configure(DynregDevSampleTest.class.getResource("/log4j.properties"));

        dynReg();
        lock("dynReg");
        assertSame(mDataTemplateSample.getConnectStatus(), TXMqttConstants.ConnectStatus.kConnected);
        LOG.debug("after dynreg connect");

        mDataTemplateSample.disconnect();
        lock("disconnect");
        assertSame(mDataTemplateSample.getConnectStatus(), TXMqttConstants.ConnectStatus.kDisconnected);
        LOG.debug("after disconnect");
    }
}
