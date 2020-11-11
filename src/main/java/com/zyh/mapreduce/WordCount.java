package com.zyh.mapreduce;

import org.apache.calcite.rel.core.SetOp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {

    public static class MyMap extends Mapper<Object, Text,Text, IntWritable>{
        public static final IntWritable one = new IntWritable(1);
        public static Text word = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer words = new StringTokenizer(value.toString(), " ");

            word.set(words.nextToken());

            context.write(word,one);
        }
    }

    public static class MyReduce extends Reducer<Text,IntWritable,Text, LongWritable>{
        private static LongWritable count = new LongWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value:values){
                sum += value.get();
            }
            count.set(sum);
            context.write(key,count);
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        System.out.println("--start--");
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJobName("WordCount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        FileSystem fileSystem = output.getFileSystem(conf);
        if (fileSystem.exists(output)){
            fileSystem.delete(output,true);
        }
        FileInputFormat.addInputPath(job,input);
        FileOutputFormat.setOutputPath(job,output);

        System.exit(job.waitForCompletion(true) ? 0 : 1 );
        System.out.println("--finished--");
    }



}
