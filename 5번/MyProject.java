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

public class MyProject extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));
        int res = ToolRunner.run(new Configuration(), new MyProject(), args);

        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.toString(args));

        Job job = Job.getInstance(getConf());
        job.setJarByClass(MyProject.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);

        return 0;
    }

 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

                JsonParser parser = new JsonParser();
                JsonElement element = parser.parse(value.toString());
                String reviewerID = element.getAsJsonObject().get("reviewerID").getAsString();
                String reviewText = element.getAsJsonObject().get("reviewText").getAsString();
                int reviewLength = reviewText.length();
                context.write(new Text(reviewerID), new IntWritable(reviewLength));
            }
        }

 public static class Reduce extends Reducer<Text , IntWritable, Text, IntWritable> {

        Text mostReviewerId = new Text();
        int maxReviewLength=0;
        int equalNumber=0;

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int currentLengthSum=0;
            for (IntWritable val : values) {
                currentLengthSum += val.get();
            }
            if(maxReviewLength<currentLengthSum){
                maxReviewLength = currentLengthSum;
                mostReviewerId.set(key);
                equalNumber=0;
            }
            else if(maxReviewLength == currentLengthSum){
                equalNumber+=1;
            }
            context.write(key, new IntWritable(currentLengthSum));
        }
        @Override
        protected void cleanup(Context context) throws IOException,InterruptedException{
            context.write(mostReviewerId,new IntWritable(maxReviewLength));
            context.write(new Text("equal num: "), new IntWritable(equalNumber));
        }
     }

}