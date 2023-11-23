package com.TargetCountMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LoanDefaultPredict {

    public static class LoanApplicationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(key.get()==0){
                return;
            }
            String[] parts = value.toString().split(",");
            for(int i = 0; i < parts.length-1; i++){
                if(parts[parts.length-1].equals("0")){
                    word.set(i+" "+"0" +" "+parts[i]);
                    context.write(word, one);
                }
                else{
                    word.set(i+" "+"1" +" "+parts[i]);
                    context.write(word, one);
                }
            }

        }
    }

    public static class LoanApplicationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable Num = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int count=0;
            for (IntWritable val : values) {
                count+=val.get();
            }
            Num.set(count);
            context.write(key, Num);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "loan application count");
        job.setJarByClass(LoanApplicationCount.class);
        job.setMapperClass(LoanApplicationMapper.class);
        job.setCombinerClass(LoanApplicationReducer.class);
        job.setReducerClass(LoanApplicationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
