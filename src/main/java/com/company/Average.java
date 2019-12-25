package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;

public class Average {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Path output = new Path("output\\day_3_Average");
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
        TextOutputFormat.setOutputPath(job, new Path("output/day_3_Average"));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.waitForCompletion(true);

    }

    public static class SalesMapper
            extends Mapper<LongWritable, Text, Text, DoubleWritable> {
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
                curString = curString.substring(0,i)+subString+curString.substring(i+subString.length()+3);
            }
            String[] parts = curString.split(",");
            String tempDay = "State: "+parts[7]+"| Date: "+parts[0].split(" ")[0]+" | Average Price:";
            Double tempPrice = Double.valueOf(parts[2]);
            context.write(new Text(tempDay),new DoubleWritable(tempPrice));
        }
    }
    public static class SalesReducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        protected void reduce(Text key, Iterable<DoubleWritable> values,
                              Context context)
                throws IOException, InterruptedException {
            Double sum = 0.0;
            Integer count = 0;
            for(DoubleWritable value:values){
                sum += value.get();
                count ++;
            }
            Double avg = (sum/count);
            context.write(key,new DoubleWritable(avg));
        }
    }
}