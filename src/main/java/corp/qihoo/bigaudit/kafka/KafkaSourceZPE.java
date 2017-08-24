package corp.qihoo.bigaudit.kafka;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

/**
 * Created by chaiwentao on 2017/8/11.
 * provide KafkaSource that can consumer from Kafka Cluster 0.8
 */
public class KafkaSourceZPE {

    private String bootstrap = "10.203.23.212:9092";
    private String zk = "10.203.23.212:2181,10.203.23.213:2181,10.203.23.214:2181,10.203.23.240:2181,10.203.23.241:2181/kafka-sys-shbt";
    private String groupId = "flink-es-test-002";
    private String topic = "namenode-log-filter";
    private String value1 = "17825792";

    public KafkaSourceZPE(String groupid){
        this.groupId = groupid;
    }

    public SourceFunction<String> getKafkaSource() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", this.bootstrap);
        properties.setProperty("zookeeper.connect", this.zk);
        properties.setProperty("group.id", this.groupId);
        properties.setProperty("replica.fetch.max.bytes",Integer.toString(17825792));
        properties.setProperty("fetch.message.max.bytes",Integer.toString(17825792));
        properties.setProperty("max.partition.fetch.bytes",Integer.toString(17825792));
        properties.setProperty("fetch.buffer.bytes",Integer.toString(17825792));
        properties.setProperty("socket.receive.buffer.bytes",Integer.toString(17825792));
        properties.put("fetch.message.max.bytes",Integer.toString(17825792));
        properties.put("max.partition.fetch.bytes",Integer.toString(17825792));
        properties.put("fetch.buffer.bytes",Integer.toString(17825792));
        properties.put("socket.receive.buffer.bytes",Integer.toString(17825792));
        FlinkKafkaConsumer08<String> kafkaSource = new FlinkKafkaConsumer08<String>(this.topic, new SimpleStringSchema(), properties);
        return kafkaSource;
    }
}
