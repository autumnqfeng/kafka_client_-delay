# kafka_client_delay
kafka 客户端延迟测试工具

启动命令：`java -jar kafka_client-0.0.1-SNAPSHOT.jar -Dspring.config.location=./application.properties` 



功能介绍：

这款工具是检测kafka延迟的一款工具，主要检测2方面的延迟。

1. 生产端消息写成功的延迟
2. 消息从生产到消费的延迟



消息日志：

<img src="./README.assets/截屏2021-08-13 下午6.37.39.png" style="zoom:100%;" />
