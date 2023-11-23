package com.TargetCountMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class FinalPredict{


    public static class LoanApplicationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Map<String, String> myMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if (cacheFiles != null && cacheFiles.length > 0) {
                    String line;
                    BufferedReader reader = new BufferedReader(new FileReader(cacheFiles[0].toString()));
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\\s+");
                        String key = parts[0]+" "+parts[1]+" "+parts[2];
                        myMap.put(key,parts[3]);
                    }
                    reader.close();
                }
            } catch (IOException e) {
                System.err.println("Exception reading DistributedCache: " + e);
            }
        }

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //读取测试集数据
            double zero_F = 11.36;
            double one_F = 1;
            Text target = new Text();

            if(key.get()==0){
                return;
            }
            String[] parts = value.toString().split(",");
            for (int i = 0; i < parts.length - 1; i++) {
                String xnj = Integer.toString(i);
                String classLabel = parts[parts.length - 1];
                String xvj = parts[i];

                word.set(xnj + " " + classLabel + " " + xvj);

                if (myMap.containsKey(word.toString())) {
                    String p = myMap.get(word.toString());
                    double prob = Double.parseDouble(p);

                    if (classLabel.equals("0")) {
                        zero_F *= prob;
                    } else {
                        one_F *= prob;
                    }
                } else {
                    // 朴素贝叶斯模型中，如果某个属性值在训练集中没有出现过，那么就会导致概率为0，所以这里直接跳过
                    continue;
                }

                double sum = zero_F + one_F;
                if (zero_F / sum > one_F / sum + 0.1) {
                    // 预测为0
                    String resultKey = classLabel.equals("0") ? "R_0_P_0" : "R_1_P_0";
                    target.set(resultKey);
                    context.write(target, one);
                } else {
                    // 预测为1
                    String resultKey = classLabel.equals("0") ? "R_0_P_1" : "R_1_P_1";
                    target.set(resultKey);
                    context.write(target, one);
                }
            }



            context.write(word, one);
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
        conf.set("fs.default.name", "hdfs://localhost:9000");
        Job job = Job.getInstance(conf, "Final Predict Job");
        job.setJarByClass(FinalPredict.class);


        job.setMapperClass(FinalPredict.LoanApplicationMapper.class);
        job.setCombinerClass(FinalPredict.LoanApplicationReducer.class);
        job.setReducerClass(FinalPredict.LoanApplicationReducer.class);
        // 设置输出键值类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        // 设置分布式缓存文件
        URI path = URI.create("hdfs://192.168.52.133:9000/output_temp/part-r-00000");
        job.addCacheFile(path);

        // 等待作业完成
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
