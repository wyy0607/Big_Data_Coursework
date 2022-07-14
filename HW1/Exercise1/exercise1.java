import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise1 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		protected void setup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
    	}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    	String line = value.toString();
			String[] tokens = line.split("\\s");
			if (tokens.length == 4) {
				String word1 = tokens[0].trim();
				String year = tokens[1].trim();
				String vol = tokens[3].trim();
			
				if (year.matches("\\d{4}")){
					if(word1.contains("nu")){
						output.collect(new Text(year+","+"nu"), new Text(vol+", 1.0"));
					}
					if(word1.contains("chi")){
						output.collect(new Text(year+","+"chi"), new Text(vol+", 1.0"));
					}
					if(word1.contains("haw")){
						output.collect(new Text(year+","+"haw"), new Text(vol+", 1.0"));
					}
				}
			}
			else if (tokens.length == 5){
				String word1 = tokens[0].trim();
				String word2 = tokens[1].trim();
				String year = tokens[2].trim();
				String vol = tokens[4].trim();

				if (year.matches("\\d{4}")){
					if(word1.contains("nu")){
						output.collect(new Text(year+","+"nu"), new Text(vol+", 1.0"));
					}
					if(word1.contains("chi")){
						output.collect(new Text(year+","+"chi"), new Text(vol+", 1.0"));
					}
					if(word1.contains("haw")){
						output.collect(new Text(year+","+"haw"), new Text(vol+", 1.0"));
					}
					if(word2.contains("nu")){
						output.collect(new Text(year+","+"nu"), new Text(vol+", 1.0"));
					}
					if(word2.contains("chi")){
						output.collect(new Text(year+","+"chi"), new Text(vol+", 1.0"));
					}
					if(word2.contains("haw")){
						output.collect(new Text(year+","+"haw"), new Text(vol+", 1.0"));
					}
				}
			}
		}	
	
		protected void cleanup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
		}
	}

	public static class AvgCombiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			double sum = 0.0, count = 0.0;
			while (values.hasNext()) {
				Text pair = values.next();
				String[] tokens = pair.toString().split(",");
				count++;
				sum += Double.parseDouble(tokens[0]); 
			}
			output.collect(key, new Text(Double.toString(sum)+", "+Double.toString(count)));
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {

	    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            double sum = 0.0, count = 0.0, avg = 0.0;
	    
	        while (values.hasNext()) {
			Text pair = values.next();
			String[] tokens = pair.toString().split(",");
			count = count + Double.parseDouble(tokens[1]);
		    sum += Double.parseDouble(tokens[0]); 
		    }

			avg = sum/count;
	        output.collect(key, new DoubleWritable(avg));
        }
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), exercise1.class);
	conf.setJobName("exercise1");
	conf.set("mapred.textoutputformat.separator", ",");

	conf.setMapOutputKeyClass(Text.class);
	conf.setMapOutputValueClass(Text.class);
	
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(DoubleWritable.class);
   
	conf.setMapperClass(Map.class);
	conf.setReducerClass(Reduce.class);
	conf.setCombinerClass(AvgCombiner.class);     

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new exercise1(), args);
	System.exit(res);
    }
}
