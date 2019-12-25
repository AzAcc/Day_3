package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

public class JoinUserTask {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Path output = new Path("output\\doc2.txt");
        FileSystem hdfs = FileSystem.get(conf);

        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }

        Job job = Job.getInstance();
        job.setJarByClass(Main.class);
        Path usersInputPath = new Path("input\\users.csv");
        Path departmentsInputPath = new Path("input\\departments.csv");

        MultipleInputs.addInputPath(job, usersInputPath, TextInputFormat.class, UserMapper.class);
        MultipleInputs.addInputPath(job, departmentsInputPath, TextInputFormat.class, DepartmentMapper.class);
        job.setReducerClass(NameReducer.class);
        TextOutputFormat.setOutputPath(job, output);

        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class UserMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");
            if (parts.length < 5)
                return;
            context.write(new Text(parts[4]), new Text("USER"+parts[3]));

        }
    }

        public static class DepartmentMapper
                extends Mapper<LongWritable, Text, Text, Text> {
            @Override
            protected void map(LongWritable key, Text value, Context context)
                    throws IOException, InterruptedException {
                String[] department = value.toString().split(",");
                context.write(new Text(department[0]), new Text(department[1]));
            }
        }

        public static class NameReducer
                extends Reducer<Text, Text, Text, Text> {
            protected void reduce(Text key, Iterable<Text> values,
                                  Context context)
                    throws IOException, InterruptedException {
                String tempOne = "";
                String tempTwo = "";
                for (Text value : values) {
                    if (value.toString().contains("USER")) {
                        tempTwo = tempTwo+value.toString().substring(4)+"fwqoqwfonc";
                    } else {
                        tempOne = value.toString();
                    }

                }
                String[] splList = tempTwo.split("fwqoqwfonc");
                if((tempTwo.length()>0)&&(tempOne.length()>0)) {
                    for (String item : splList) {
                        context.write(new Text(tempOne + " : "), new Text(item));
                    }
                }
            }
        }
    }

