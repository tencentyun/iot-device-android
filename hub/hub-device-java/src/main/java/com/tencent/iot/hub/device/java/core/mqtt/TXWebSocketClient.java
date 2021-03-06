package com.tencent.iot.hub.device.java.core.mqtt;

import com.tencent.iot.hub.device.java.core.util.Base64;
import com.tencent.iot.hub.device.java.core.util.HmacSha256;
import com.tencent.iot.hub.device.java.utils.Loggor;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

import javax.net.SocketFactory;


public class TXWebSocketClient extends MqttAsyncClient implements MqttCallbackExtended {

    private static final String TAG = TXWebSocketClient.class.getName();
    private static final Logger logger = LoggerFactory.getLogger(TXWebSocketClient.class);
    static { Loggor.setLogger(logger); }

    private volatile TXWebSocketActionCallback connectListener;
    private boolean automicReconnect = true;
    private String clientId;
    private String secretKey = null;
    private MqttConnectOptions conOptions;
    // 状态机
    private AtomicReference<ConnectionState> state = new AtomicReference<>(ConnectionState.DISCONNECTED);

    public TXWebSocketClient(String serverURI, String clientId, String secretKey) throws MqttException {
        super(serverURI, clientId, new MemoryPersistence());
        this.secretKey = secretKey;
        this.clientId = clientId;
        setCallback(this);
    }

    // 连接接口
    public IMqttToken connect() throws MqttException {
        if (state.get() == ConnectionState.CONNECTED) { // 已经连接过
            Loggor.debug(TAG, "already connect");
            throw new MqttException(MqttException.REASON_CODE_CLIENT_CONNECTED);
        }

        IMqttToken ret = super.connect(conOptions);
        ret.waitForCompletion(-1);
        state.set(ConnectionState.CONNECTING);
        return ret;
    }

    // 重连接口
    public void reconnect() throws MqttException {
        super.reconnect();
    }

    public void setMqttConnectOptions(MqttConnectOptions mqttConnectOptions) {
        this.conOptions = mqttConnectOptions;

        // 设置密钥之后可以进行 mqtt 连接
        String userName = generateUsername();
        conOptions.setUserName(userName);
        if (secretKey != null && secretKey.length() != 0) {
            try {
                conOptions.setPassword(generatePwd(userName).toCharArray());
            } catch (IllegalArgumentException e) {
                Loggor.debug(TAG, "Failed to set password");
            }
        }
        conOptions.setMqttVersion(MqttConnectOptions.MQTT_VERSION_3_1_1);
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setTXWebSocketActionCallback(TXWebSocketActionCallback connectListener) {
        this.connectListener = connectListener;
    }

    public TXWebSocketActionCallback getTXWebSocketActionCallback() {
        return this.connectListener;
    }

    // 主动断开连接
    public synchronized IMqttToken disconnect() throws MqttException {
        if (state.get() == ConnectionState.DISCONNECTED || state.get() == ConnectionState.DISCONNECTING) {      // 已经处于断开连接状态
            throw new MqttException(MqttException.REASON_CODE_CLIENT_ALREADY_DISCONNECTED);
        }

        IMqttToken ret = this.disconnect(null, mActionListener);
        state.set(ConnectionState.DISCONNECTING);   // 接口调用成功后重新设置状态
        onDisconnected();
        return ret;
    }

    private void onDisconnected() {
        state.set(ConnectionState.DISCONNECTED);
        if (connectListener != null) {
            connectListener.onDisconnected();
        }
    }

    IMqttActionListener mActionListener = new IMqttActionListener() {
        @Override
        public void onSuccess(IMqttToken asyncActionToken) {
            Loggor.debug(TAG, "disconnect onSuccess");
            onDisconnected();
        }

        @Override
        public void onFailure(IMqttToken asyncActionToken, Throwable cause) {
            Loggor.error(TAG, "disconnect onFailure");
            onDisconnected();
        }
    };

    // 获取连接状态 true:上线 false:掉线
    public ConnectionState getConnectionState() {
        return state.get();
    }

    private String generatePwd(String userName) {
        if (secretKey != null) {
            try {
                String passWordStr = HmacSha256.getSignature(userName.getBytes(),
                        Base64.decode(secretKey, Base64.DEFAULT)) + ";hmacsha256";
                return passWordStr;
            } catch (IllegalArgumentException e) {
                Loggor.error(TAG, "Failed to set password");
            }
        }
        return null;
    }

    private String generateUsername() {
        Long timestamp;
        if (automicReconnect) {
            timestamp = (long) Integer.MAX_VALUE;
        } else {
            timestamp = System.currentTimeMillis() / 1000 + 600;
        }

        return clientId + ";" + TXMqttConstants.APPID + ";" + getConnectId() + ";" + timestamp;
    }

    protected String getConnectId() {
        StringBuffer connectId = new StringBuffer();
        for (int i = 0; i < TXMqttConstants.MAX_CONN_ID_LEN; i++) {
            int flag = (int) (Math.random() * Integer.MAX_VALUE) % 3;
            int randNum = (int) (Math.random() * Integer.MAX_VALUE);
            switch (flag) {
                case 0:
                    connectId.append((char) (randNum % 26 + 'a'));
                    break;
                case 1:
                    connectId.append((char) (randNum % 26 + 'A'));
                    break;
                case 2:
                    connectId.append((char) (randNum % 10 + '0'));
                    break;
            }
        }

        return connectId.toString();
    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        state.set(ConnectionState.CONNECTED);
        Loggor.debug(TAG, "connectComplete");
        if (connectListener != null) {
            connectListener.onConnected();
        }

        // 根据实际情况注释
//        testPublish();
    }

    // 测试使用的自动发布消息
    private void testPublish() {
        MqttMessage msg = new MqttMessage();
        msg.setPayload("str".getBytes());
        msg.setQos(0);  // 最多发送一次，不做必达性保证
        Loggor.debug(TAG, "start send");
        try {
            this.publish("/", msg);
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        Loggor.error(TAG, "connectionLost");
        state.set(ConnectionState.CONNECTION_LOST);
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        Loggor.debug(TAG, "messageArrived");
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        Loggor.debug(TAG, "deliveryComplete");
    }
}
