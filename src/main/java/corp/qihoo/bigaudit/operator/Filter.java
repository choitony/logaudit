package corp.qihoo.bigaudit.operator;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Created by chaiwentao on 2017/8/11.
 */
public class Filter {

    public FilterFunction<String> getFilter(){
        FilterFunction<String> filterFunction = new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if(value.indexOf("org.apache.hadoop.hdfs.server.namenode.FSNamesystem.audit")!=-1)
                    return true;
                else
                    return false;
            }
        };
        return filterFunction;
    }
}
