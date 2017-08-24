package corp.qihoo.bigaudit.log;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Created by chaiwentao on 2017/8/11.
 */
public interface LogInterface<T, O, S> {

    FilterFunction<O> getFilterFunction();

    FlatMapFunction<T,O> getFlatMapFunction();

    MapFunction<T,S> getMapFunction();
}
