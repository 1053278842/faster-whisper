import pika
import time
import os


class RabbitMQClient:
    def __init__(self, host, port, user, password, queue_name, heartbeat=60, retry_delay=5):
        self.host = host
        self.port = port
        self.queue_name = queue_name
        self.credentials = pika.PlainCredentials(user, password)
        self.heartbeat = heartbeat
        self.retry_delay = retry_delay
        self._connect()

    def _connect(self):
        """建立连接并创建通道，带重试"""
        while True:
            try:
                params = pika.ConnectionParameters(
                    host=self.host,
                    port=self.port,
                    virtual_host="/",
                    credentials=self.credentials,
                    heartbeat=self.heartbeat,
                    blocked_connection_timeout=300,
                )
                self.connection = pika.BlockingConnection(params)
                self.channel = self.connection.channel()
                # 声明队列（幂等）
                self.channel.queue_declare(queue=self.queue_name, durable=True)
                print("RabbitMQ 连接成功")
                break
            except Exception as e:
                print(f"RabbitMQ 连接失败: {e}，{self.retry_delay}s 后重试...")
                time.sleep(self.retry_delay)

    # 生产者方法
    def produce(self, message):
        """发送消息，失败时重连并重试一次"""
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message.encode("utf-8"),
                properties=pika.BasicProperties(delivery_mode=2),  # 持久化消息
            )
            print(f"生产者发送消息: {message}")
        except (pika.exceptions.AMQPError, pika.exceptions.StreamLostError) as e:
            print(f"发送失败，尝试重连: {e}")
            self._connect()
            # 再发一次
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message.encode("utf-8"),
                properties=pika.BasicProperties(delivery_mode=2),
            )
            print(f"生产者发送消息(重试成功): {message}")

    # 消费者方法，允许外部传入回调
    def consume(self, callback=None):
        """
        callback: 外部自定义回调函数，函数签名必须是 (ch, method, properties, body)
        如果不传，使用默认内部 _callback
        """
        actual_callback = callback if callback else self._callback
        print("消费者启动，等待消息...")
        while True:
            try:
                self.channel.basic_consume(
                    queue=self.queue_name,
                    auto_ack=True,
                    on_message_callback=actual_callback,
                )
                self.channel.start_consuming()
            except (pika.exceptions.AMQPError, pika.exceptions.StreamLostError) as e:
                print(f"消费过程中断开，正在重连: {e}")
                self._connect()

    # 内部回调
    def _callback(self, ch, method, properties, body):
        text = body.decode("utf-8")
        print(f"消费者收到: {text}")

    # 关闭连接
    def close(self):
        if self.connection and self.connection.is_open:
            self.connection.close()
            print("RabbitMQ 连接已关闭")


# 创建全局可用的RabbitMQ客户端实例
host = os.environ.get("RABBITMQ_HOST")
port = int(os.environ.get("RABBITMQ_PORT", "5672"))
usr = os.environ.get("RABBITMQ_USR")
paw = os.environ.get("RABBITMQ_PAW")
queue = os.environ.get("RABBITMQ_QUEUE")

if not host or not port or usr is None or paw is None or queue is None:
    raise ValueError(
        "Rabbitmq 配置不完整，请检查环境变量 RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USR, RABBITMQ_PAW, RABBITMQ_QUEUE 是否已配置"
    )

rabbitmq_client = RabbitMQClient(host, port, usr, paw, queue)
