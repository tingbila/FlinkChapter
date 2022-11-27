package chapter05;

//readTextFile(path) - Reads text files, i.e. files that respect the TextInputFormat specification, line-by-line and returns them as Strings.

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Source_File {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> fileDS = env.readTextFile("D:\\workspace\\idea_location\\FlinkChapter\\input\\UserBehavior.log");

        fileDS.print();

        env.execute("Source_File");
    }
}
