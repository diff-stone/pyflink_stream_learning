# _*_coding:utf-8_*_
from datetime import timedelta

from pyflink.common import RestartStrategies
from pyflink.datastream import StreamExecutionEnvironment, CheckpointingMode


def env_setting(env: StreamExecutionEnvironment):
    """
    设置环境
    :param env:
    :return:
    """
    # 配置检查点周期5分钟, 至少一次
    env.enable_checkpointing(60 * 1000 * 5, CheckpointingMode.AT_LEAST_ONCE)
    # 上一个检查点与下一个检查点之间必须间隔 3 分钟启动
    env.get_checkpoint_config().set_min_pause_between_checkpoints(60 * 1000 * 3)
    # 设置检查点超时40分钟
    env.get_checkpoint_config().set_checkpoint_timeout(60 * 1000 * 40)
    # 固定间隔重启策略 5分钟内若失败了3次则认为该job失败，重试间隔为30s
    env.set_restart_strategy(RestartStrategies.failure_rate_restart(3, timedelta(minutes=5), timedelta(seconds=30)))
    # 设置 checkpoint 失败 不重启任务
    # env.get_checkpoint_config().set_fail_on_checkpointing_errors(False)


def get_kafka_customer_properties(kafka_servers: str, group_id: str):
    """
    获取kafka 消费者 配置
    :return:
    """
    properties = {
        "bootstrap.servers": kafka_servers,
        "fetch.max.bytes": "67108864",
        "auto.offset.reset": "earliest",
        "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
        "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
        "enable.auto.commit": "false",  # 关闭kafka 自动提交，此处不能传bool 类型会报错
        "group.id": group_id,
    }
    return properties

def get_kafka_Producer_properties(servers):
    """
    kafka 生产者配置
    :param servers:
    :return:
    """
    properties = {
        "bootstrap.servers": servers,
        "max.request.size": "14999999"
    }
    return properties
