package chapter01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

//Flink批处理案例开发

public class WordCountBatch {
    public static void main(String[] args) throws Exception {
        // set up the execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 读取数据源
        DataSet<String> text = env.readTextFile("D:\\workspace\\idea_location\\FlinkChapter\\src\\main\\java\\chapter01\\word.txt");
        DataSet<Tuple2<String, Integer>> sumData = text
                // split up the lines in pairs (2-tuples) containing: (word,1)
                .flatMap(new Tokenizer())
                // group by the tuple field "0" and sum up tuple field "1"
                .groupBy(0)
                .sum(1);

        sumData.print();
    }


    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    collector.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
