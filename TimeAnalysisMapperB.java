import java.io.IOException;
import java.util.*;
import java.text.*;
import java.util.regex.*;
import  java.util.StringTokenizer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class TimeAnalysisMapperB extends Mapper<Object, Text, Text, IntWritable> {

    private final IntWritable one = new IntWritable(1);
    private final int mostPopularHour = new Integer(1);
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(";");

        if((line.length == 4) && (java.util.regex.Pattern.matches("\\d+", line[0]))) {
            LocalDateTime time = LocalDateTime.ofEpochSecond(Long.parseLong(line[0])/1000, 0, ZoneOffset.ofHours(0));

            if(time.getHour() == mostPopularHour) {
                Pattern pattern = Pattern.compile("#[a-zA-Z0-9_]+");
                Matcher matcher = pattern.matcher(line[2].toLowerCase());

                while(matcher.find()) {
                    context.write(new Text(matcher.group()), one);
                }
            }
        }
    }
}