import java.io.IOException;
import java.util.*;
import java.text.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TimeAnalysisMapperA extends Mapper<Object, Text, Text, IntWritable> {

    private Text hour = new Text();
    private final IntWritable one = new IntWritable(1);
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(";");

        if((line.length == 4) && (java.util.regex.Pattern.matches("\\d+", line[0]))) {
            LocalDateTime time = LocalDateTime.ofEpochSecond(Long.parseLong(line[0])/1000, 0, ZoneOffset.ofHours(0));
            hour.set(new Text(new String(Integer.toString(time.getHour()))));

            context.write(hour, one);
        }
    }
}