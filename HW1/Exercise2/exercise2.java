import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise2 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		protected void setup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
    	}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    	String line = value.toString();
			String[] tokens = line.split("\\s");
			if (tokens.length == 4) {
				String vol = tokens[3].trim();
				double volume = Double.parseDouble(vol);
				double volume_squared = volume * volume;
				String vol_squared = String.valueOf(volume_squared);
				output.collect(new Text("standard deviation"), new Text(vol_squared+","+vol));
			}
			else if (tokens.length == 5){
				String vol = tokens[4].trim();
				double volume = Double.parseDouble(vol);
				double volume_squared = volume * volume;
				String vol_squared = String.valueOf(volume_squared);
				output.collect(new Text("standard deviation"), new Text(vol_squared+","+vol));
			}
		}	
	
		protected void cleanup(OutputCollector<Text, Text> output) throws IOException, InterruptedException {
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {

	    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            double sum_of_square = 0.0, count = 0.0, sum = 0.0;
	    
	        while (values.hasNext()) {
				Text line = values.next();
				String[] tokens = line.toString().split(",");
				double volume_squared = Double.parseDouble(tokens[0].trim());
				double volume = Double.parseDouble(tokens[1].trim());
				sum_of_square += volume_squared;
				count ++;
				sum += volume;
		    }

			double avg = sum/count;
			double sd = Math.sqrt(1/count * (sum_of_square - count*avg*avg));
	        output.collect(new Text(""), new DoubleWritable(sd));
        }
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), exercise2.class);
	conf.setJobName("exercise2");
	conf.setStrings("mapreduce.reduce.shuffle.memory.limit.percent", "0.15");

	conf.setMapOutputKeyClass(Text.class);
	conf.setMapOutputValueClass(Text.class);
	
	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(DoubleWritable.class);
   
	conf.setMapperClass(Map.class);
	conf.setReducerClass(Reduce.class);   

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new exercise2(), args);
	System.exit(res);
    }
}
