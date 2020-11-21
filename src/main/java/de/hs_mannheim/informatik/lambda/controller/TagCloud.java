package de.hs_mannheim.informatik.lambda.controller;

import java.awt.Color;
import java.awt.Dimension;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.log4j.BasicConfigurator;

import com.kennycason.kumo.CollisionMode;
import com.kennycason.kumo.WordCloud;
import com.kennycason.kumo.WordFrequency;
import com.kennycason.kumo.bg.CircleBackground;
import com.kennycason.kumo.font.scale.SqrtFontScalar;
import com.kennycason.kumo.nlp.FrequencyAnalyzer;
import com.kennycason.kumo.palette.ColorPalette;

public class TagCloud {
	
	static int countWords;
	
	public static class SingleTokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text("word");

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//			String[] worte = value.toString().split("\\W+");
			//
			//			for (String wort : worte) {
			//				word.set(wort);
			//				context.write(word, one);
			//			}

			Pattern pattern = Pattern.compile("(\\b[^\\s]+\\b)");
			Matcher matcher = pattern.matcher(value.toString());
			while (matcher.find()) {
				//word.set(value.toString().substring(matcher.start(), matcher.end()).toLowerCase());
				context.write(word, one);
			}
		}
	}
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//			String[] worte = value.toString().split("\\W+");
			//
			//			for (String wort : worte) {
			//				word.set(wort);
			//				context.write(word, one);
			//			}

			Pattern pattern = Pattern.compile("(\\b[^\\s]+\\b)");
			Matcher matcher = pattern.matcher(value.toString());
			while (matcher.find()) {
				word.set(value.toString().substring(matcher.start(), matcher.end()).toLowerCase());
				context.write(word, one);
			}
		}
	}

	public static class SwitchMapper extends Mapper<Text, IntWritable, IntWritable, Text>{

		public void map(Text word, IntWritable count, Context context) throws IOException, InterruptedException {
			if(word.toString().length() <=4) {
				return;
			}
			context.write(count, word);
		}

	}
	
	public static class CountMapper extends Mapper<Text, IntWritable, Text, IntWritable>{
		Text word = new Text("word");
		public void map(Text word, IntWritable count, Context context) throws IOException, InterruptedException {
			countWords = count.get();
			context.write(word, count);
		}

	}
	
	/*
	
	public int readCount() {
		int count = 0;
		File file = new File("resources/globalwc-output/part-r-00000"); 
		Scanner sc = new Scanner(file); 
  
		while (sc.hasNextLine()) 
			string(sc.nextLine()); 
		} 
	
		return count;
	}*/

	public static void GlobalJob() throws Exception {
		File wcOutput = new File("resources/global-wc-output");
		File fsOutput = new File("resources/global-fs-output");
		File globalwcOutput = new File("resources/global-count-output");
		TagCloud.deleteDir(wcOutput);
		TagCloud.deleteDir(fsOutput);
		TagCloud.deleteDir(globalwcOutput);
		//wcOutput.mkdirs();
		//fsOutput.mkdirs();
		//globalwcOutput.mkdirs();
		
		BasicConfigurator.configure(); 					// Log4j Config oder ConfigFile in Resources Folder
		System.setProperty("hadoop.home.dir", "/");  	// für Hadoop 3.3.0

		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "global word count");
		job.setJarByClass(TagCloud.class);
		job.setMapperClass(SingleTokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("resources/tagcloudfiles/*"));
		FileOutputFormat.setOutputPath(job, new Path("resources/global-count-output"));
		job.waitForCompletion(true);
		
		System.out.println(countWords);
		
		
		
		job = Job.getInstance(conf, "word count");
		job.setJarByClass(TagCloud.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("resources/tagcloudfiles/*"));
		FileOutputFormat.setOutputPath(job, new Path("resources/global-wc-output"));

		job.waitForCompletion(true);

		// -----------------------> Job 2 

		job = Job.getInstance(conf, "freq sort");
		job.setJarByClass(TagCloud.class);
		job.setMapperClass(SwitchMapper.class);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);
		
		job.setSortComparatorClass(MyDescendingComparator.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		FileInputFormat.addInputPath(job, new Path("resources/global-wc-output"));
		FileOutputFormat.setOutputPath(job, new Path("resources/global-fs-output"));
		
		job.waitForCompletion(true);

		
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		createTagCloud("resources/global-fs-output/part-r-00000", "global");
	}
	
	public static void LocalJob(String fileName) throws Exception {
		File wcOutput = new File("resources/local-wc-output");
		File fsOutput = new File("resources/local-fs-output");
		File globalwcOutput = new File("resources/local-count-output");
		TagCloud.deleteDir(wcOutput);
		TagCloud.deleteDir(fsOutput);
		TagCloud.deleteDir(globalwcOutput);
		//wcOutput.mkdirs();
		//fsOutput.mkdirs();
		//globalwcOutput.mkdirs();
		
		BasicConfigurator.configure(); 					// Log4j Config oder ConfigFile in Resources Folder
		System.setProperty("hadoop.home.dir", "/");  	// für Hadoop 3.3.0

		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "global word count");
		job.setJarByClass(TagCloud.class);
		job.setMapperClass(SingleTokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("resources/tagcloudfiles/"+fileName));
		FileOutputFormat.setOutputPath(job, new Path("resources/local-count-output"));
		job.waitForCompletion(true);
		
		System.out.println(countWords);
		
		
		
		job = Job.getInstance(conf, "word count");
		job.setJarByClass(TagCloud.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("resources/tagcloudfiles/"+fileName));
		FileOutputFormat.setOutputPath(job, new Path("resources/local-wc-output"));

		job.waitForCompletion(true);

		// -----------------------> Job 2 

		job = Job.getInstance(conf, "freq sort");
		job.setJarByClass(TagCloud.class);
		job.setMapperClass(SwitchMapper.class);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);
		
		job.setSortComparatorClass(MyDescendingComparator.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		FileInputFormat.addInputPath(job, new Path("resources/local-wc-output"));
		FileOutputFormat.setOutputPath(job, new Path("resources/local-fs-output"));
		
		job.waitForCompletion(true);

		
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		createTagCloud("resources/local-fs-output/part-r-00000", fileName);
        
	}
	
	public static void main(String[] args) throws Exception {
		GlobalJob();
		LocalJob("Faust.txt");
		/*
		BasicConfigurator.configure(); 					// Log4j Config oder ConfigFile in Resources Folder
		System.setProperty("hadoop.home.dir", "/");  	// für Hadoop 3.3.0

		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "global word count");
		job.setJarByClass(TagCloud.class);
		job.setMapperClass(SingleTokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("resources/klassiker/*.txt"));
		FileOutputFormat.setOutputPath(job, new Path("resources/globalwc-output"));
		job.waitForCompletion(true);
		
		System.out.println(countWords);
		
		
		
		job = Job.getInstance(conf, "word count");
		job.setJarByClass(TagCloud.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("resources/klassiker/*.txt"));
		FileOutputFormat.setOutputPath(job, new Path("resources/wc-output"));

		job.waitForCompletion(true);

		// -----------------------> Job 2 

		job = Job.getInstance(conf, "freq sort");
		job.setJarByClass(TagCloud.class);
		job.setMapperClass(SwitchMapper.class);
		job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setNumReduceTasks(1);
		
		job.setSortComparatorClass(MyDescendingComparator.class);
		job.setInputFormatClass(SequenceFileInputFormat.class);

		FileInputFormat.addInputPath(job, new Path("resources/wc-output"));
		FileOutputFormat.setOutputPath(job, new Path("resources/fs-output"));
		
		job.waitForCompletion(true);

		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		*/
	}
	
	private static void createTagCloud(String filePath, String fileName) throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader(filePath));
        String record;
        List<WordFrequency> wf = new LinkedList<WordFrequency>();
        while((record = reader.readLine()) != null) {
            int blankPos = record.indexOf("	");
            //System.out.println(record+"blankPos"+blankPos);
            String keyString = record.substring(0, blankPos);
            String valueString = record.substring(blankPos + 1);
            System.out.println(keyString + " | " + valueString);
            wf.add(new WordFrequency(valueString, Integer.parseInt(keyString)));
        }
	
		final Dimension dimension = new Dimension(600, 600);
		final WordCloud wordCloud = new WordCloud(dimension, CollisionMode.PIXEL_PERFECT);
		wordCloud.setPadding(2);
		wordCloud.setBackground(new CircleBackground(300));
		wordCloud.setColorPalette(new ColorPalette(new Color(0x4055F1), new Color(0x408DF1), new Color(0x40AAF1), new Color(0x40C5F1), new Color(0x40D3F1), new Color(0xFFFFFF)));
		wordCloud.setFontScalar(new SqrtFontScalar(8, 50));
		wordCloud.build(wf);
		wordCloud.writeToFile("tagclouds/batch-" + fileName + ".png");
	}
	
	private static boolean deleteDir(File dir) {
	      if (dir.isDirectory()) {
	          String[] children = dir.list();
	          for (int i = 0; i < children.length; i++) {
	             boolean success = deleteDir (new File(dir, children[i]));
	             
	             if (!success) {
	                return false;
	             }
	          }
	       }
	       return dir.delete();
	    }

	
}

class MyDescendingComparator extends WritableComparator {
	public MyDescendingComparator() {
		super(IntWritable.class, true);
	}

	public int compare(WritableComparable a, WritableComparable b) {
		return super.compare(a, b) * (-1); 
	}
}