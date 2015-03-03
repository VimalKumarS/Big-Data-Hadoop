package MapReduce;


import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class MinValue  extends Configured implements Tool{

	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		MinValue driver = new MinValue();
		
		int exitCode = ToolRunner.run(driver, args);
		
		System.exit(exitCode);
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		if(args.length !=2) {
			System.err.println("Usage: MaxTemperatureDriver <input path> <outputpath>");
			System.err.printf("Usage: %s [generic options] <inputfile> <outputdir>\n",getClass().getSimpleName());
			System.exit(-1);
			
			}
		Job job = new Job();
		job.setJarByClass(MinValue.class);
		job.setJobName("Min Value");
		job.setMapperClass(RecieptMapper.class);
		job.setReducerClass(RecieptReduce.class);
		
		//job.setInputFormatClass(TextInputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		return job.waitForCompletion(true)?0:1;
		}
	}


