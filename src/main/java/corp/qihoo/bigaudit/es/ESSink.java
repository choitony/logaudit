package corp.qihoo.bigaudit.es;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch2.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch2.RequestIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by chaiwentao on 2017/8/11.
 */
public class ESSink {

    private static String clusterName = "bigaudit";
    private static String bulkFlushInterval = "1000";
    private static String ESNODE = "10.198.66.247:9300,10.198.66.248:9300";
    private static List<InetSocketAddress> transportAddresses = new ArrayList<>();
    private static String indexName = "my-index";
    private static String typeName = "my-type";

    public static SinkFunction<Map<String, String>> getESSink() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put("cluster.name", clusterName);
        // This instructs the sink to emit after every element, otherwise they would be buffered
        config.put("bulk.flush.max.actions", bulkFlushInterval);

        transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.198.66.247"), 9300));

        SinkFunction<Map<String, String>> esSink = new ElasticsearchSink<>(config, transportAddresses, new ElasticsearchSinkFunction<Map<String, String>>() {

            public IndexRequest createIndexRequest(Map<String, String> json) {

                String jsonString = "";
                for (Map.Entry<String, String> entry : json.entrySet()) {
                    jsonString += "<" + entry.getKey() + ":" + entry.getValue() + ">";
                }
                return Requests.indexRequest()
                        .index("cert_test_flow_local_mulit")
                        .type("qihoo_audit_test")
                        .source(json);
            }

            @Override
            public void process(Map<String, String> element, RuntimeContext ctx, RequestIndexer indexer) {
                indexer.add(createIndexRequest(element));
            }
        });

        return esSink;
    }
}
