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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class TfIdf {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path output = new Path("output\\day_3_corpus");
        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }

        Path input = new Path("input\\corpus");
        File listFile = new File("input");
        File exportFiles[] = listFile.listFiles();
        final Integer countFiles = exportFiles.length;

        //First Job
        Job firstJob = Job.getInstance();
        firstJob.setJarByClass(Main.class);
        TextInputFormat.addInputPath(firstJob, new Path("input\\corpus"));
        firstJob.setInputFormatClass(TextInputFormat.class);
        firstJob.setMapperClass(FormatMapper.class);
        firstJob.setReducerClass(FormatReducer.class);
        TextOutputFormat.setOutputPath(firstJob, new Path("output/day_3_corpus"));
        firstJob.setOutputFormatClass(TextOutputFormat.class);
        firstJob.setOutputKeyClass(Text.class);
        firstJob.setOutputValueClass(Text.class);
        firstJob.waitForCompletion(true);

        Path output2 = new Path("output/day_3_corpus_answer");
        FileSystem hdfss = FileSystem.get(conf);
        if (hdfss.exists(output2)) {
            hdfss.delete(output2, true);
        }
        //Second Job
        Job secondJob = Job.getInstance();
        secondJob.setJarByClass(Main.class);
        TextInputFormat.addInputPath(secondJob, new Path("output\\day_3_corpus\\part-r-00000"));
        secondJob.setInputFormatClass(TextInputFormat.class);
        secondJob.setMapperClass(AnswerMapper.class);
        secondJob.setReducerClass(AnswerReducer.class);
        TextOutputFormat.setOutputPath(secondJob, new Path("output/day_3_corpus_answer"));
        secondJob.setOutputFormatClass(TextOutputFormat.class);
        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(Text.class);
        secondJob.waitForCompletion(true);

    }
    //Mapper for firstJob
    public static class FormatMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String curString = value.toString();
            curString = curString.replaceAll("[0-9]{1,}-[a-zA-Zа-яА-Я]"," ");
            final String regex = "[^a-zA-Zа-яА-Я]";
            curString = curString.replaceAll(regex," ");
            while(curString.contains("  ")){
                curString = curString.replaceAll("  "," ");
            }
            String[] stringParts = curString.split(" ");
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            for(String item:stringParts){
                if(!item.equals("")) {
                    String tempWord = item.toLowerCase() + " " + fileName;
                    context.write(new Text(item.toLowerCase()), new Text(fileName));
                }
            }
        }
    }
    //Reducer for firstJob
    public static class FormatReducer
            extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            HashMap<String,Integer> tempMap = new HashMap<>();
        for(Text value:values){
            tempMap.put(value.toString(),tempMap.getOrDefault((value.toString()),0)+1);
        }
        for(String keyMap:tempMap.keySet()){
            Integer wordInFiles = tempMap.keySet().size();
            context.write(key,new Text(","+wordInFiles.toString()+","+keyMap+","+tempMap.get(keyMap)));
        }
        }
    }
    //Mapper for secondJob
    public static class AnswerMapper
            extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String curLine = value.toString();
            while(curLine.contains(" ")){curLine.replaceAll("  ","");}
            String[] lineParts = curLine.split(",");
            String tempLine = lineParts[0]+","+lineParts[1]+","+lineParts[3];
            context.write(new Text(lineParts[2]),new Text(tempLine));
        }
    }
    //Reducer for secondJob
    public static class AnswerReducer
            extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            HashMap<String,String> tempMap = new HashMap<>();
            for(Text value:values){
                tempMap.put(value.toString(),key.toString());
            }
            Path input = new Path("input\\corpus");
            File listFile = new File("input");
            File exportFiles[] = listFile.listFiles();
            final Integer countFiles = exportFiles.length;
            String answer = "";
            for(String mapKey:tempMap.keySet()){
                String[] parts = mapKey.split(",");
                System.out.println(parts[0]+" "+parts[1]+" "+parts[2]);
                Double n = Double.valueOf(tempMap.size());
                Integer nt = Integer.valueOf(parts[2]);
                Double N = Double.valueOf(countFiles);
                Integer NT = Integer.valueOf(parts[1]);
                Double toLn = N/NT;
                double TFIDF =(nt/n)*(Math.log(toLn));
                context.write(new Text(parts[0]+":"+tempMap.get(mapKey)+""+"\t"),new Text(String.valueOf(TFIDF)));
            }
        }
    }
}
