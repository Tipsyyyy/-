package com.TargetCountMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.io.IOException;

public class WeekdayLoanCount {

    public static class LoanApplicationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if(key.get()==0){
                return;
            }
            String[] parts = value.toString().split(",");
            word.set(parts[25]);//使用星期几为键值
            context.write(word, one);
        }
    }

    public static class LoanApplicationReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<Text, Integer> counts = new HashMap<>();
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count=0;

            for (IntWritable val : values) {
                count+=val.get();
            }
            counts.put(new Text(key), count);
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 使用 TreeMap 对 counts 进行排序
            TreeMap<Text, Integer> sortedCounts = new TreeMap<>(counts);

            for (Map.Entry<Text, Integer> entry : sortedCounts.entrySet()) {
                result.set(entry.getValue());
                context.write(entry.getKey(), result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "weekday loan count");
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
