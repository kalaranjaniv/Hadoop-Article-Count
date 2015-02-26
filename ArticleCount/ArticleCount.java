import java.io.*;
import java.util.*;

 import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

 public class ArticleCount extends Configured implements Tool {

   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
	   
     private String inputFile;
     private Text content = new Text();
     private Text artifactid = new Text();
     IntWritable one = new IntWritable(1);
    String searchterm;
     

     public void configure(JobConf job) {      
       inputFile = job.get("mapreduce.map.input.file");
       searchterm = job.get("searchterm");
       
      
     }

     public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
    	 
    	 String line = value.toString();
    	 String[] splitstrings = new String[5];
    	 splitstrings= line.split("\t");
    	 	artifactid.set(splitstrings[0]);
    	 	String title = splitstrings[1];
    	 	String maincontent = splitstrings[3];
    	 	
    	 		
    	 content.set(title + " " +maincontent);
    	 StringTokenizer contenttxt = new StringTokenizer(content.toString());
    	 while(contenttxt.hasMoreTokens())
    	 {
    		 if(contenttxt.nextToken().equals(searchterm))
    				 {
    			 output.collect(artifactid,one);
    				 }
    	 }
             
    } 
   }
   public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
    	 
    	 int sum1 = 0;
         while (values.hasNext()) {
        	 sum1 += values.next().get();
         }
         output.collect(key, new IntWritable(sum1));
       }

       
   }
   public static class Combiner extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	     public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
	    	 
	    	 int sum = 0;
	    	 Text count = new Text("count");
	         while (values.hasNext()) {
	        	 sum += values.next().get();
	         }
	         if(sum>0)
	         {
	        	 sum=1;
	         }
	         output.collect(count, new IntWritable(sum));
	       }
   }
   public int run(String[] args) throws Exception {
     JobConf conf = new JobConf(getConf(), WordCount.class);
     conf.setJobName("wordcount");

     conf.setOutputKeyClass(Text.class);
     conf.setOutputValueClass(IntWritable.class);

     conf.setMapperClass(Map.class);
     conf.setCombinerClass(Combiner.class);
     conf.setReducerClass(Reduce.class);

     conf.setInputFormat(TextInputFormat.class);
     conf.setOutputFormat(TextOutputFormat.class);

    
      conf.set("searchterm", args[2]);

     FileInputFormat.setInputPaths(conf, new Path(args[0]));
     FileOutputFormat.setOutputPath(conf, new Path(args[1]));

     JobClient.runJob(conf);
     return 0;
   }
   public static void main(String[] args) throws Exception {
     int res = ToolRunner.run(new Configuration(), new WordCount(), args);
     System.exit(res);
   }
 }
