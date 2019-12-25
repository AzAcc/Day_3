package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;


public class SalesTask {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Path output = new Path("output\\day_3");
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }
        Job job = Job.getInstance();
        job.setJarByClass(Main.class);
        TextInputFormat.addInputPath(job, new Path("input\\SalesJan2009.csv"));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesReducer.class);
        job.setCombinerClass(SalesReducer.class);
        TextOutputFormat.setOutputPath(job, new Path("output/day_3_sales"));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.waitForCompletion(true);

    }

    public static class SalesMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (key.equals(new LongWritable(0))){
                return;
            }
            String curString = value.toString();
            if (curString.contains("\"")){
                int i = curString.indexOf("\"");
                String subString = curString.substring(i+1);
                int j = subString.indexOf("\"");
                subString = subString.substring(0,j);
                subString = subString.replace(",","");
                curString = curString.substring(0,i)+subString+curString.substring(i+subString.length()+2);
            }
            String[] parts = curString.split(",");
            context.write(new Text(parts[4]),new Text(parts[3]));

        }
    }

    public static class SalesReducer
            extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values,
                              Context context)
                throws IOException, InterruptedException {
            HashMap<String,Integer> paymentSity = new HashMap<String, Integer>();
            String maxKey ="";
            Integer maxValue = 0;
            for(Text value:values){
                paymentSity.put(value.toString(),paymentSity.getOrDefault(paymentSity.get(value.toString()),0)+1);
                maxKey = value.toString();
                maxValue = paymentSity.get(maxKey);
            }

            for(String payKey:paymentSity.keySet()){
                if (maxValue < paymentSity.get(payKey)){
                    maxValue = paymentSity.get(payKey);
                    maxKey = payKey;
                }
            }
            context.write(key,new Text(maxKey));
        }
    }
}