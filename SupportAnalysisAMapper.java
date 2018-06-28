//package join;

import java.util.*;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Calendar;
import java.util.Hashtable;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SupportAnalysisAMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Hashtable<String, String> athletes;
    private final IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] line = value.toString().split(";");
        if((line.length == 4) && (line[2].length() <= 140)) {
            Set<String> set = athletes.keySet();
            for(String s : set) {
                String athleteName = athletes.get(s);

                if(line[2].toLowerCase().contains(athleteName)) {
                    context.write(new Text(athleteName), one);
                }
            }
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        athletes = new Hashtable<String, String>();

        URI fileURI = context.getCacheFiles()[0];
        FileSystem fileSystem = FileSystem.get(context.getConfiguration());
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path(fileURI));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream));

        String line = null;
        try {
            bufferedReader.readLine();

            while((line = bufferedReader.readLine()) != null) {
                String[] athleteMedalRecord = line.split(",");
                
                if(athleteMedalRecord.length == 11) {
                    athletes.put(athleteMedalRecord[0], athleteMedalRecord[1].toLowerCase());
                }
            }
            bufferedReader.close();
        } catch (IOException ioEx) {

        }
        super.setup(context);
    }
}