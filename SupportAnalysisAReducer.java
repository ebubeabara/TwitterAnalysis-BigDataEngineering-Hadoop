//package join;

import java.io.IOException;
import java.util.*;
import java.lang.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SupportAnalysisAReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable athletesMentionedCount = new IntWritable(1);

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for(IntWritable value : values) {
            sum = sum + value.get();
        }
        athletesMentionedCount.set(sum);
        context.write(key, athletesMentionedCount);
    }
}