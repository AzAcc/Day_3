package com.company;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import javax.xml.crypto.Data;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Main {

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Path output = new Path("output\\doc2");
        FileSystem hdfs = FileSystem.get(conf);

        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }

        Job job = Job.getInstance();
        job.setJarByClass(Main.class);
        TextInputFormat.addInputPath(job, new Path("input\\doc1.txt"));
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(LoginMapper.class);
        job.setReducerClass(LoginReducer.class);
        TextOutputFormat.setOutputPath(job, new Path("output\\doc1"));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DataWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

    public static class LoginMapper
            extends Mapper<LongWritable, Text, Text, DataWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] kosmos = value.toString().split(",");
            SimpleDateFormat tempTime = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            Date myTime = null;
            try {
                myTime = tempTime.parse((kosmos[5]));
            } catch (ParseException e) {
                e.printStackTrace();
            }
            DataWritable tempData = new DataWritable(kosmos[2],myTime.getTime());
            context.write(new Text(kosmos[4]),tempData);
        }
    }

    public static class LoginReducer
            extends Reducer<Text, DataWritable, Text, Text> {
        protected void reduce(Text key, Iterable<DataWritable> values,
                              Context context)
                throws IOException, InterruptedException {
            HashSet<String> tempMap = new HashSet<String>();
            Long lastValue = 0L;
            String lastString = "";
            Boolean first = true;
            Boolean active = false;
            HashMap<String,String> result = new HashMap<>();
            for (DataWritable value : values) {
                if (first){
                    lastString = value.getLogin();
                    lastValue = value.getData();
                    first = false;
                    tempMap.add(value.getLogin());
                }else{
                    if(!lastString.equals(value.getLogin())){
                        if((lastValue-value.getData())<180000){
                            active = true;
                            context.write(key,new Text((lastString)+" "+
                                    (new Date(lastValue).toString())+"|"+(value.getLogin())+" "
                                    +(new Date(value.getData()).toString())));
                        }
                        lastString = value.getLogin();
                        lastValue = value.getData();
                        tempMap.add(value.getLogin());
                    }else{
                        lastString = value.getLogin();
                        lastValue = value.getData();
                        tempMap.add(value.getLogin());
                    }
                }
               /* if(result.containsKey(value.getLogin())){
                    result.replace(value.getLogin(),result.get(value.getLogin()),result.get(value.getLogin())+(new Date(value.getData()*1000)).toString()+"|");
                }else{
                    result.put(value.getLogin(),(new Date(value.getData())).toString());
                }
            }
            if(active){
                for(String mykey:result.keySet()){
                    context.write(key,new Text(mykey+result.get(mykey)));
                }

            }*/
        }
    }


}
}
