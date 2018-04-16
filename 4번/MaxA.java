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
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.util.Map;
import java.util.TreeMap;
import java.util.Stack;
public class MaxA extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new MaxA(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));

        Job job = Job.getInstance(getConf());
        job.setJarByClass(MaxA.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;
    }

 public static class Map extends Mapper<LongWritable, Text, Text,Text> {
        Gson gson = new Gson();
        int[] data;
        String reviewerId =" ";
        String aCommaB = "";
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

                JsonParser parser = new JsonParser();
                JsonElement element = parser.parse(value.toString());
                reviewerId = element.getAsJsonObject().get("reviewerID").getAsString();
                data = gson.fromJson(element.getAsJsonObject().get("helpful"), int[].class);
                aCommaB = Integer.toString(data[0])+","+Integer.toString(data[1]);
                context.write(new Text(reviewerId),new Text(aCommaB));
            }
        }

 public static class Reduce extends Reducer<Text , Text, Text, Text> {

        int maxA = 0;
        int maxB = 0;
        Stack<String> maxAReviewerStack = new Stack();
        Stack<String> maxBReviewerStack = new Stack();
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)throws IOException, InterruptedException {
            int sumA = 0;
            int sumB = 0;
            for(Text val : values){
                String[] valueArray = val.toString().split(",");
                sumA += Integer.parseInt(valueArray[0]);
                sumB += Integer.parseInt(valueArray[1]);
            }
            if(maxA<sumA){
                maxA = sumA;
                while(maxAReviewerStack.empty()!=true){
                    maxAReviewerStack.pop();
                }
                maxAReviewerStack.push(key.toString());
            }
            else if(maxA == sumA){
                maxAReviewerStack.push(key.toString());
            }

            if(maxB<sumB){
                maxB = sumB;
                while(maxBReviewerStack.empty()!=true){
                    maxBReviewerStack.pop();
                }
                maxBReviewerStack.push(key.toString());
            }
            else if(maxB == sumB){
                maxBReviewerStack.push(key.toString());
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException,InterruptedException{
            while(maxAReviewerStack.empty()!=true){
                context.write(new Text(maxAReviewerStack.peek()),new Text("max a: "+Integer.toString(maxA)));
                maxAReviewerStack.pop();
            }
            while(maxBReviewerStack.empty()!=true){
                context.write(new Text(maxBReviewerStack.peek()), new Text("max b: "+Integer.toString(maxB)));
                maxBReviewerStack.pop();
            }

        }
     }

}

