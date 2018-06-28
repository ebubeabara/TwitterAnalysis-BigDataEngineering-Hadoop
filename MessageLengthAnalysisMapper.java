import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class MessageLengthAnalysisMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(";");
        if(line.length == 4) {
            if((line[2].length() <= 140)) {
                double tweetLengthCeilDouble = new Double(Math.ceil(line[2].length()/5));
                int tweetLengthCeil = (int) tweetLengthCeilDouble;

                if((tweetLengthCeil > 0) && (tweetLengthCeil <=28)) {
                    context.write(new Text("Group" + (Integer.toString(tweetLengthCeil)) + ":Range:" + (Integer.toString((tweetLengthCeil*5)-4)) + "-" + (Integer.toString(tweetLengthCeil*5))), one);
                }
            }
        }
    }
}