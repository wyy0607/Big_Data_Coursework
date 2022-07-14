import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise4 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		protected void setup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
    	}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    	String line = value.toString();
			String[] tokens = line.split(",");
			String artist = tokens[2].trim();
			String duration = tokens[3].trim();
			output.collect(new Text(artist), new Text(duration));
		}
	
	protected void cleanup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {
	    
		public void configure(JobConf job) {
		}
		
		protected void setup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
    	}
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException 
		{
			double max_duration = 0;
			
			while (values.hasNext()) {
        		double parsing = Double.parseDouble(values.next().toString());
          		if (max_duration < parsing) {
            		max_duration = parsing;
          		}
        	}
        	
			output.collect(key, new DoubleWritable(max_duration)); //collect the key (year) and the maximum temp for that year
        }

        protected void cleanup(OutputCollector<Text, DoubleWritable> output) throws IOException, InterruptedException {
		}
	}

	public static class MyPartitioner implements Partitioner<Text, Text> {
		public void configure(JobConf job) {
		}

		public int getPartition(Text key, Text value, int numReduceTasks)	{
       		String first_character = key.toString().substring(0,1).toLowerCase(); 
         
        	if(numReduceTasks == 0)	{
				return 0;
        	}
         
        	if (first_character.compareTo("f")<0)	{
            	return 0;
        	}
        	else if (first_character.compareTo("k")<0)	{
            	return 1;
        	}
        	else if (first_character.compareTo("p")<0)	{
            	return 2;
        	}
			else if (first_character.compareTo("u")<0)	{
            	return 3;
        	}
			else {
				return 4;
			}
      	}
	}

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), exercise4.class);
	conf.setJobName("exercise4");

	conf.setMapOutputKeyClass(Text.class);
	conf.setMapOutputValueClass(Text.class);
	
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(DoubleWritable.class);
   
	conf.setMapperClass(Map.class);
	conf.setPartitionerClass(MyPartitioner.class);
	conf.setReducerClass(Reduce.class);
	conf.setNumReduceTasks(5);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new exercise4(), args);
	System.exit(res);
    }
}
