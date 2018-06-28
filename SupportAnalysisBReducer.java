import java.io.IOException;
import java.util.*;
import java.lang.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class SupportAnalysisBReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private final IntWritable athletesAndTheirSport = new IntWritable(1);

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

        for(IntWritable value : values) {
            sum = sum + value.get();
        }
        athletesAndTheirSport.set(sum);
        context.write(key, athletesAndTheirSport);
    }
}