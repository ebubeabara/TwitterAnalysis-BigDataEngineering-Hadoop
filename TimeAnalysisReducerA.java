import java.io.IOException;
import java.util.*;
import java.lang.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TimeAnalysisReducerA extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable totalTweetsPerHour = new IntWritable(1);

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sumTweets = 0;

        for(IntWritable value : values) {
            sumTweets = sumTweets + value.get();
        }
        totalTweetsPerHour.set(sumTweets);
        context.write(key, totalTweetsPerHour);
    }
}