import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise3 extends Configured implements Tool {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    	String line = value.toString();
			String[] tokens = line.split(",");
	    	String title = tokens[0].trim();
			String artist = tokens[2].trim();
			String duration = tokens[3].trim();
			String year = tokens[165].trim();

			if (year.matches("\\d{4}")){
				int pub_year = Integer.parseInt(year);
				if (pub_year>=2000 && pub_year<=2010){
					output.collect(new Text(title+","+artist+","+duration+","+year), 
					new Text(""));
				}
			}
		}
	}

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), exercise3.class);
	conf.setJobName("exercise3");

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(Text.class);
   
	conf.setMapperClass(Map.class);
	conf.setNumReduceTasks(0);
	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(conf, new Path(args[0]));
	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new exercise3(), args);
	System.exit(res);
    }
}
