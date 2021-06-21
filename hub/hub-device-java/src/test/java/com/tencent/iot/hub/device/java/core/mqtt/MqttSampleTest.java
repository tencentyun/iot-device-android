package com.tencent.iot.hub.device.java.core.mqtt;

import com.tencent.iot.hub.device.java.core.common.Status;
import com.tencent.iot.hub.device.java.core.log.TXMqttLogCallBack;
import com.tencent.iot.hub.device.java.core.log.TXMqttLogConstants;
import com.tencent.iot.hub.device.java.core.util.AsymcSslUtils;
import main.mqtt.MQTTRequest;

import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import hub.unit.test.BuildConfig;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Device Mqtt connect sample
 */
public class MqttSampleTest {

	private static final Logger LOG = LoggerFactory.getLogger(MqttSampleTest.class);

	private static final String TAG = "TXMQTT";

	private static String mBrokerURL = null;  //传入null，即使用腾讯云物联网通信默认地址 "${ProductId}.iotcloud.tencentdevices.com:8883"  https://cloud.tencent.com/document/product/634/32546

	private static String mProductID = BuildConfig.TESTMQTTSAMPLE_PRODUCT_ID;
	private static String mDevName = BuildConfig.TESTMQTTSAMPLE_DEVICE_NAME;
	private static String mDevPSK = BuildConfig.TESTMQTTSAMPLE_DEVICE_PSK;
	private static String mTestTopic = BuildConfig.TESTMQTTSAMPLE_TEST_TOPIC;
	private static String mCertFilePath = "DEVICE_CERT_FILE_NAME";           // Device Cert File Name
	private static String mPrivKeyFilePath = "DEVICE_PRIVATE_KEY_FILE_NAME";            // Device Private Key File Name
	private static String mDevCert = "DEVICE_CERT_CONTENT_STRING";           // Cert String
	private static String mDevPriv = "DEVICE_PRIVATE_KEY_CONTENT_STRING";           // Priv String

	private static TXMqttConnection mqttconnection;
	private static MqttConnectOptions options;

	/**日志保存的路径*/
	private final static String mLogPath = System.getProperty("user.dir") + "/hub/hub-device-java/src/test/resources/";
	/**
	 * 请求ID
	 */
	private static AtomicInteger requestID = new AtomicInteger(0);

	private static void connect() {
		String workDir = System.getProperty("user.dir") + "/hub/hub-device-java/src/test/resources/";

		// init connection
		options = new MqttConnectOptions();
		options.setConnectionTimeout(8);
		options.setKeepAliveInterval(60);
		options.setAutomaticReconnect(true);
		//客户端证书文件名  mDevPSK是设备秘钥

		if (mDevPriv != null && mDevCert != null && mDevPriv.length() != 0 && mDevCert.length() != 0 && !mDevCert.equals("DEVICE_CERT_CONTENT_STRING") && !mDevPriv.equals("DEVICE_PRIVATE_KEY_CONTENT_STRING")) {
			LOG.info("Using cert stream " + mDevPriv + "  " + mDevCert);
			options.setSocketFactory(AsymcSslUtils.getSocketFactoryByStream(new ByteArrayInputStream(mDevCert.getBytes()), new ByteArrayInputStream(mDevPriv.getBytes())));
		} else if (mDevPSK != null && mDevPSK.length() != 0){
			LOG.info("Using PSK");
//				options.setSocketFactory(AsymcSslUtils.getSocketFactory());   如果您使用的是3.3.0及以下版本的 hub-device-java sdk，由于密钥认证默认配置的ssl://的url，请添加此句setSocketFactory配置。
		} else {
			LOG.info("Using cert file");
			options.setSocketFactory(AsymcSslUtils.getSocketFactoryByFile(workDir + mCertFilePath, workDir + mPrivKeyFilePath));
		}
		mqttconnection = new TXMqttConnection(mBrokerURL, mProductID, mDevName, mDevPSK,null,null ,true, new SelfMqttLogCallBack(), new callBack());
		mqttconnection.connect(options, null);
	}

	private static void disconnect() {
		mqttconnection.disConnect(null);
	}

	private static void subscribeTopic() {
		mqttconnection.subscribe(mTestTopic, 1, null);
	}

	private static void unSubscribeTopic() {
		mqttconnection.unSubscribe(mTestTopic, null);
	}

	private static void publishTopic() {
		// 要发布的数据
		Map<String, String> data = new HashMap<String, String>();
		// 车辆类型
		data.put("car_type", "suv");
		// 车辆油耗
		data.put("oil_consumption", "6.6");
		// 车辆最高速度
		data.put("maximum_speed", "205");
		// 温度信息
		data.put("temperature", "25");
		// MQTT消息
		MqttMessage message = new MqttMessage();

		JSONObject jsonObject = new JSONObject();
		try {
			for (Map.Entry<String, String> entrys : data.entrySet()) {
				jsonObject.put(entrys.getKey(), entrys.getValue());
			}
		} catch (JSONException e) {
			LOG.error(e.getMessage()+"pack json data failed!");
		}
		message.setQos(TXMqttConstants.QOS1);
		message.setPayload(jsonObject.toString().getBytes());

		// 用户上下文（请求实例）

		LOG.debug("pub topic " + mTestTopic + message);
		// 发布主题
		mqttconnection.publish(mTestTopic, message, null);
	}

	private static void subscribeRRPCTopic() {
		// 用户上下文（请求实例）
		MQTTRequest mqttRequest = new MQTTRequest("subscribeTopic", requestID.getAndIncrement());
		// 订阅主题
		mqttconnection.subscribeRRPCTopic(TXMqttConstants.QOS0, mqttRequest);
	}

	private static void subscribeBroadCastTopic() {
		// 用户上下文（请求实例）
		MQTTRequest mqttRequest = new MQTTRequest("subscribeTopic", requestID.getAndIncrement());
		// 订阅广播主题 Topic
		mqttconnection.subscribeBroadcastTopic(TXMqttConstants.QOS1, mqttRequest);
	}

	private static void deviceLog() {
		mqttconnection.mLog(TXMqttLogConstants.LEVEL_ERROR,TAG,"Error level log for test!!!");
		mqttconnection.mLog(TXMqttLogConstants.LEVEL_WARN,TAG,"Warning level log for test!!!");
		mqttconnection.mLog(TXMqttLogConstants.LEVEL_INFO,TAG,"Info level log for test!!!");
		mqttconnection.mLog(TXMqttLogConstants.LEVEL_DEBUG,TAG,"Debug level log for test!!!");
	}

	private static void uploadLog() {
		mqttconnection.uploadLog();//上传设备日志
	}

	public static class callBack extends TXMqttActionCallBack {

		@Override
		public void onConnectCompleted(Status status, boolean reconnect, Object userContext, String msg) {
			String userContextInfo = "";

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

			String logInfo = String.format("onDisconnectCompleted, status[%s], userContext[%s], msg[%s]", status.name(), userContextInfo, msg);
			LOG.info(logInfo);
			unlock("onDisconnectCompleted");
		}

		@Override
		public void onPublishCompleted(Status status, IMqttToken token, Object userContext, String errMsg) {
			String userContextInfo = "";

			String logInfo = String.format("onPublishCompleted, status[%s], topics[%s],  userContext[%s], errMsg[%s]",
					status.name(), Arrays.toString(token.getTopics()), userContextInfo, errMsg);
			LOG.debug(logInfo);
			if (status == Status.OK && Arrays.toString(token.getTopics()).contains(mTestTopic)) {
				publishTopicSuccess = true;
				unlock("onPublishCompleted mTestTopic");
			}
		}

		@Override
		public void onSubscribeCompleted(Status status, IMqttToken asyncActionToken, Object userContext, String errMsg) {
			String userContextInfo = "";

			String logInfo = String.format("onSubscribeCompleted, status[%s], topics[%s], userContext[%s], errMsg[%s]",
					status.name(), Arrays.toString(asyncActionToken.getTopics()), userContextInfo, errMsg);
			if (Status.ERROR == status) {
				LOG.error(logInfo);
			} else {
				LOG.debug(logInfo);
				if (Arrays.toString(asyncActionToken.getTopics()).contains(mTestTopic)){ // 订阅mTestTopic成功
					subscribeTopicSuccess = true;
					unlock("onSubscribeCompleted mTestTopic");
				} else if (Arrays.toString(asyncActionToken.getTopics()).contains("rrpc/rxd")) { // 订阅rrpc Topic成功
					subscribeRRPCTopicSuccess = true;
					unlock("onSubscribeCompleted subscribeRRPCTopic");
				} else if (Arrays.toString(asyncActionToken.getTopics()).contains("broadcast/rxd")) { // broadcast Topic成功
					subscribeBroadCastTopicSuccess = true;
					unlock("onSubscribeCompleted subscribeBroadCastTopic");
				}
			}
		}

		@Override
		public void onUnSubscribeCompleted(Status status, IMqttToken asyncActionToken, Object userContext, String errMsg) {
			String userContextInfo = "";

			String logInfo = String.format("onUnSubscribeCompleted, status[%s], topics[%s], userContext[%s], errMsg[%s]",
					status.name(), Arrays.toString(asyncActionToken.getTopics()), userContextInfo, errMsg);
			LOG.debug(logInfo);
			if (status == Status.OK && Arrays.toString(asyncActionToken.getTopics()).contains(mTestTopic)) {
				unSubscribeTopicSuccess = true;
				unlock("onUnSubscribeCompleted mTestTopic");
			}
		}

		@Override
		public void onMessageReceived(final String topic, final MqttMessage message) {
			String logInfo = String.format("receive message, topic[%s], message[%s]", topic, message.toString());
			LOG.debug(logInfo);
		}
	}

	/**
	 * 实现TXMqttLogCallBack回调接口
	 */
	private static class SelfMqttLogCallBack extends TXMqttLogCallBack {

		@Override
		public String setSecretKey() {
			String secertKey;
			if (mDevPSK != null && mDevPSK.length() != 0) {  //密钥认证
				secertKey = mDevPSK;
				secertKey = secertKey.length() > 24 ? secertKey.substring(0,24) : secertKey;
				return secertKey;
			} else {
				BufferedReader cert;

				if (mDevCert != null && mDevCert.length() != 0) { //动态注册,从DevCert中读取
					cert = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(mDevCert.getBytes(Charset.forName("utf8"))), Charset.forName("utf8")));

				} else { //证书认证，从证书文件中读取

					try {
						String workDir = System.getProperty("user.dir") + "/hub/hub-device-java/src/test/resources/";
						cert=new BufferedReader(new InputStreamReader(new FileInputStream(new File(workDir + mCertFilePath))));
					} catch (IOException e) {
						LOG.error("getSecertKey failed, cannot open CRT Files.");
						return null;
					}
				}
				//获取密钥
				try {
					if (cert.readLine().contains("-----BEGIN")) {
						secertKey = cert.readLine();
						secertKey = secertKey.length() > 24 ? secertKey.substring(0,24) : secertKey;
					} else {
						secertKey = null;
						LOG.error("Invaild CRT Files.");
					}
					cert.close();
				} catch (IOException e) {
					LOG.error("getSecertKey failed.");
					return null;
				}
			}

			return secertKey;
		}

		@Override
		public void printDebug(String message){
			LOG.debug(message);
		}

		@Override
		public boolean saveLogOffline(String log){

			String logFilePath = mLogPath + mProductID + mDevName + ".log";

			LOG.debug("Save log to %s", logFilePath);

			try {
				BufferedWriter wLog = new BufferedWriter(new FileWriter(new File(logFilePath), true));
				wLog.write(log);
				wLog.flush();
				wLog.close();
				return true;
			} catch (IOException e) {
				String logInfo = String.format("Save log to [%s] failed, check the Storage permission!", logFilePath);
				LOG.error(logInfo);
				e.printStackTrace();
				return false;
			}
		}

		@Override
		public String readOfflineLog(){

			String logFilePath = mLogPath + mProductID + mDevName + ".log";

			LOG.debug("Read log from %s", logFilePath);

			try {
				BufferedReader logReader = new BufferedReader(new FileReader(logFilePath));
				StringBuilder offlineLog = new StringBuilder();
				int data;
				while (( data = logReader.read()) != -1 ) {
					offlineLog.append((char)data);
				}
				logReader.close();
				return offlineLog.toString();
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}

		@Override
		public boolean delOfflineLog(){

			String logFilePath = mLogPath + mProductID + mDevName + ".log";

			File file = new File(logFilePath);
			if (file.exists() && file.isFile()) {
				if (file.delete()) {
					return true;
				}
			}
			return false;
		}

	}

	/** ============================================================================== Unit Test ============================================================================== **/

	private static final int COUNT = 1;
	private static final int TIMEOUT = 3000;
	private static CountDownLatch latch = new CountDownLatch(COUNT);

	private static boolean subscribeTopicSuccess = false;
	private static boolean publishTopicSuccess = false;
	private static boolean unSubscribeTopicSuccess = false;
	private static boolean subscribeRRPCTopicSuccess = false;
	private static boolean subscribeBroadCastTopicSuccess = false;

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
	public void testMqttConnect() {

		LogManager.resetConfiguration();
		LOG.isDebugEnabled();
		PropertyConfigurator.configure(MqttSampleTest.class.getResource("/log4j.properties"));

		connect();
		lock("connect");
		assertSame(mqttconnection.getConnectStatus(), TXMqttConstants.ConnectStatus.kConnected);
		LOG.debug("after connect");

		subscribeTopic();
		lock("subscribeTopic");
		assertTrue(subscribeTopicSuccess);
		LOG.debug("after subscribe");

		publishTopic();
		lock("publishTopic");
		assertTrue(publishTopicSuccess);
		LOG.debug("after publish");

		unSubscribeTopic();
		lock("unSubscribeTopic");
		assertTrue(unSubscribeTopicSuccess);
		LOG.debug("after unSubscribe");

		subscribeRRPCTopic();
		lock("subscribeRRPCTopic");
		assertTrue(subscribeRRPCTopicSuccess);
		LOG.debug("after subscribeRRPCTopic");

		subscribeBroadCastTopic();
		lock("subscribeBroadCastTopic");
		assertTrue(subscribeBroadCastTopicSuccess);
		LOG.debug("after subscribeBroadCastTopic");

		disconnect();
		lock("disconnect");
		assertSame(mqttconnection.getConnectStatus(), TXMqttConstants.ConnectStatus.kDisconnected);
		LOG.debug("after disconnect");
	}
}
