package com.imooc.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 需求：读取HDFS上的hello.txt文件，计算文件中每个单词出现的总次数
 * 原始文件hello.txt文件内容如下：
 * Hello Java Hello Scala Hello C
 * Hello BigData
 * 最终需要的结果文件内容如下：
 * C    1
 * Hello    4
 * Java 1
 * Scala    1
 */
public class WordCountJob {

    /**
     * key1     KEYIN:      LongWritable
     * value1   VALUEIN:    Text
     * key2     KEYOUT:     Text
     * value2   VALUEOUT:   LongWritable
     */
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        /**
         * @param key1   每一行行首偏移量
         * @param value1 每一行内容
         */
        @Override
        protected void map(LongWritable key1, Text value1, Context context) throws IOException, InterruptedException {
            String[] words = value1.toString().split(" ");
            for (String word : words) {
                Text key2 = new Text(word);
                LongWritable value2 = new LongWritable(1);
                context.write(key2, value2);
            }
        }
    }

    /**
     * key2     KEYIN:      Text
     * value2   VALUEIN:    LongWritable
     * key3     KEYOUT:     Text
     * value3   VALUEOUT:   LongWritable
     */
    public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key2, Iterable<LongWritable> value2s, Context context) throws IOException, InterruptedException {
            long sum = 0L;
            for (LongWritable value2 : value2s) {
                sum += value2.get();
            }
            Text key3 = new Text(key2);
            LongWritable value3 = new LongWritable(sum);
            context.write(key3, value3);
        }
    }

    /**
     * WordCountJob = WordCountMapper + WordCountReducer
     */
    public static void main(String[] args) {
        if (args.length != 2) {
            System.exit(-1);
        }
        try {
            /* 配置Job参数 */
            Configuration conf = new Configuration();
            /* 创建一个Job实例 */
            Job job = Job.getInstance(conf);
            /* 配置输入路径（可以是文件，也可以是目录） */
            FileInputFormat.setInputPaths(job, new Path(args[0]));
            /* 配置输出路径（必须指定一个不存在的目录） */
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            /* 配置Mapper（无需配置Mapper input，框架已知） */
            job.setMapperClass(WordCountMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(LongWritable.class);
            /* 配置Reducer（Reducer input即 Mapper output） */
            job.setReducerClass(WordCountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(LongWritable.class);
            /* 提交Job */
            job.waitForCompletion(true);
            /* 如果不设置下面一行，那么在集群中找不到WordCountJob这个类 */
            job.setJarByClass(WordCountJob.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
