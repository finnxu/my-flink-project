package myflink;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * PackageName : myflink
 * ProjectName : my-flink-project
 * Author : finnxu
 * Date : 2019-08-30 22:05
 * Description : 用于输出窗口的结果
 */
public class WindowResultFunction implements WindowFunction<Long, ItemViewCount, Tuple, TimeWindow> {

    /**
     * @param key             窗口的主键，即 itemId
     * @param window          窗口
     * @param aggregateResult 聚合函数的结果，即 count 值
     * @param collector       输出类型为 ItemViewCount
     * @throws Exception
     */
    @Override
    public void apply(Tuple key,
                      TimeWindow window,
                      Iterable<Long> aggregateResult,
                      Collector<ItemViewCount> collector) throws Exception {
        Long itemId = ((Tuple1<Long>) key).f0;
        Long count = aggregateResult.iterator().next();
        collector.collect(ItemViewCount.of(itemId, window.getEnd(), count));
    }
}
