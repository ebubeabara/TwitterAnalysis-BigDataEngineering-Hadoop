import java.io.IOException;
import java.util.*;
import java.lang.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TimeAnalysisReducerB extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable mostPopularHashTags = new IntWritable(1);

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sumHashTags = 0;

        for(IntWritable value : values) {
            sumHashTags = sumHashTags + value.get();
        }
        mostPopularHashTags.set(sumHashTags);
        context.write(key, mostPopularHashTags);
    }
}