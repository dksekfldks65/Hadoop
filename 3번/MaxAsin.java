package kr.ac.kookmin.cs.bigdata;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.util.Map;
import java.util.TreeMap;
import java.util.Stack;
import java.util.*;

public class MaxAsin extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new MaxAsin(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));

        Job job = Job.getInstance(getConf());
        job.setJarByClass(MaxAsin.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;
    }

 public static class Map extends Mapper<LongWritable, Text, Text,FloatWritable> {
        private final static IntWritable ONE = new IntWritable(1);
        private Text word = new Text();
        Gson gson = new Gson();
        float[] data;
        String asin =" ";

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

                JsonParser parser = new JsonParser();
                JsonElement element = parser.parse(value.toString());
                asin = element.getAsJsonObject().get("asin").getAsString();
                data = gson.fromJson(element.getAsJsonObject().get("helpful"), float[].class);

                if(data[1] >10){
                        float ratio = data[0]/data[1];
                        context.write(new Text(asin),new FloatWritable(ratio));
                }
            }
        }

 public static class Reduce extends Reducer<Text , FloatWritable, Text, FloatWritable> {

        float maxRatio = -1;
        Stack<String> asinIdStack = new Stack();
        @Override
        public void reduce(Text key, Iterable<FloatWritable> values, Context context)throws IOException, InterruptedException {
            float currentRatio = 0;
            for(FloatWritable val : values){
                currentRatio = val.get();
                if(maxRatio<currentRatio){
                    maxRatio = val.get();
                    while(asinIdStack.empty()!=true){
                        asinIdStack.pop();
                    }
                    asinIdStack.push(key.toString());
                }
                else if(maxRatio == currentRatio){
                        asinIdStack.push(key.toString());
                }
            }
            context.write(key ,new FloatWritable(currentRatio));
        }
        @Override
        protected void cleanup(Context context) throws IOException,InterruptedException{
            context.write(new Text("end"), new FloatWritable(1));
            while(asinIdStack.empty()!=true){
                context.write(new Text(asinIdStack.peek()),new FloatWritable(maxRatio));
                asinIdStack.pop();
            }
        }
     }
}
