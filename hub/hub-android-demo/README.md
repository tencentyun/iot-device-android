## 概述

该演示Demo通过四个tab页面演示了[HUB Android SDK](https://github.com/tencentyun/iot-device-java/tree/master/hub/hub-device-android) 基础功能和个别使用场景
## Demo入口示意图
```
├── Demo
│   ├── 基础功能
│   ├── 设备互通
│   ├── 设备影子
│   ├── 远程服务
```

## 演示Demo的执行路径
### 基础功能
该页面从上至下包含三部分：`参数设置区`、`功能操作区`、`日志输出区`
1. 参数设置区
    * 包含下拉选择参数key和参数value输入框(可以不在此处设置)，在[app-config.json](https://github.com/tencentyun/iot-device-java/blob/master/hub/hub-android-demo/app-config.json)中设置即可
2. 功能操作区
    * 包含连接MQTT、断开MQTT、动态注册、Topic相关(订阅主题/取消订阅主题/发布主题/订阅RRPC主题/订阅广播主题)、子设备上下线、检查固件更新、日志、绑定与解绑子设备、查询设备拓扑关系、Websocket相关等操作
    * 注意：在操作功能区其他功能的前提是点击`连接MQTT`且日志输出区打印出`onConnectComplete，status[ok]`字样

3. 日志输出区
    * 功能区的操作在功能区会有对应的日志输出，比如点击了`订阅主题`，日志输出区会打印`onSubscribeCompleted`字样

### 设备互通
1. 功能操作区
    * [智能家居场景上下文](https://cloud.tencent.com/document/product/634/11913)
    * 包含进门和出门两个操作，其中进门会使房间的空调开启，出门会使房间的空调关闭

2. 日志输出区
    * 功能区的操作在功能区会有对应的日志输出，比如点击了`进门`，日志输出区会打印`receive command: open airconditioner`字样

### 设备影子
1. 功能操作区
    * 包含连接IOT、断开连接、注册设备属性、获取设备文档、定时更新设备影子、Topic相关(订阅主题/取消订阅主题/发布主题)操作

2. 日志输出区
    * 功能区的操作在功能区会有对应的日志输出，比如点击了`获取设备文档`，日志输出区会打印`document[{...`字样


### 远程服务
1. 功能操作区
    * 包含开启远程服务、停止远程服务、是否使用shadow等操作，其中远程服务指在Android Service组件中运行Mqtt断开连接等功能
    * 勾选使用shadow选项，会多出注册设备属性、获取设备影子、更新设备影子操作

2. 日志输出区
    * 功能区的操作在功能区会有对应的日志输出，比如点击了`开启远程服务`，日志输出区会打印`remote service has been started!`字样
