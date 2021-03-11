# _*_coding:utf-8_*_
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.typeinfo import BasicTypeInfo
from env_setting import env_setting, get_kafka_Producer_properties, get_kafka_customer_properties
import os, json
from config_file import TEST_KAFKA_SERVERS, TEST_GROUP_ID, TEST_KAFKA_TOPIC, TEST_SINK_TOPIC


def run():
    # 获取运行环境
    env = StreamExecutionEnvironment.get_execution_environment()

    # 设置运行环境
    env_setting(env)
    # 设置并行度
    env.set_parallelism(1)
    # 添加jar文件 windows 系统改成自己的jar 所在文件地址
    kafka_jar = f"file://{os.getcwd()}/jars/flink-connector-kafka_2.11-1.12.0.jar"
    kafka_client = f"file://{os.getcwd()}/jars/kafka-clients-2.4.1.jar"
    env.add_jars(kafka_jar, kafka_client)

    # 添加文件
    env.add_python_file(f"{os.getcwd()}/config_file.py")
    env.add_python_file(f"{os.getcwd()}/env_setting.py")

    # 使用打包的运行环境 (自定义环境打包)
    env.add_python_archive(f"{os.getcwd()}/venv.zip")
    env.set_python_executable("env.zip/venv/bin/python")
    # 使用本地运行环境
    # env.set_python_executable(PYTHON_EXECUTABLE)
    env.disable_operator_chaining()

    kafka_product_properties = get_kafka_Producer_properties(TEST_KAFKA_SERVERS)

    properties = get_kafka_customer_properties(TEST_KAFKA_SERVERS, TEST_GROUP_ID)

    data_stream = env.add_source(
        FlinkKafkaConsumer(topics=TEST_KAFKA_TOPIC,
                           properties=properties,
                           deserialization_schema=SimpleStringSchema()) \
            .set_commit_offsets_on_checkpoints(True)
    ) \
        .name(f"消费{TEST_KAFKA_TOPIC}主题数据")

    data_stream.map(lambda value: json.loads(s=value, encoding="utf-8")) \
        .name("转成json") \
        .map(lambda value: json.dumps(value), BasicTypeInfo.STRING_TYPE_INFO()) \
        .name("转成str") \
        .add_sink(FlinkKafkaProducer(topic=TEST_SINK_TOPIC,
                                     producer_config=kafka_product_properties,
                                     serialization_schema=SimpleStringSchema())) \
        .name("存入kafka")

    env.execute("测试pyflink 读取和写入kafka")


if __name__ == '__main__':
    run()