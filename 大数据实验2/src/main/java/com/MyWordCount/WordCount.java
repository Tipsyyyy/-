package com.MyWordCount;

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
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {
    public static void main(String[] args) throws Exception{
//为任务设定配置文件
        Configuration conf = new Configuration();
//命令行参数
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2){
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf,"word count");
        job.setMapperClass(TokenizerMapper.class); //设置Mapper类
        job.setCombinerClass(IntSumReducer.class); //设置Combine类
        job.setReducerClass(IntSumReducer.class); //设置Reducer类
        job.setOutputKeyClass(Text.class); //设置job输出的key
//设置job输出的value
        job.setOutputValueClass(IntWritable.class);
//设置输入文件的路径
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

//设置输出文件的路径
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//提交任务并等待任务完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context )
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) { sum += val.get(); }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class TokenizerMapper //定义Map类实现字符串分解
            extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        //实现map()函数
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
//将字符串拆解成单词
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken()); //将分解后的一个单词写入word
                context.write(word, one); //收集<key, value>
            }
        }
    }
}
