package Voter; 

import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import Voter.VoterDriver;
import Voter.VoterMapper;
import Voter.VoterReducer;
import Voter.VoterCounter;

public class VoterDriver extends Configured implements Tool {

	 public int run(String[] args) throws Exception {
	      // check the CLI
	      if (args.length != 2) {
	         System.err.printf("usage: %s [generic options] <inputfile> <outputdir>\n", getClass().getSimpleName());
	         System.exit(1);
	      }
	      // TODO change name below
	      Job job = new Job(getConf(), "Voter Driver _ Lab1 _ ");

	      job.setJarByClass(VoterDriver.class);
	      job.setMapperClass(VoterMapper.class);

	      // TODO comment out the Reducer class definition
	      job.setReducerClass(VoterReducer.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(FloatWritable.class);
	      job.setMapOutputValueClass(IntWritable.class);
	      // setup input and output paths
	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1])); 

	      // launch job syncronously
	      int sucess=  job.waitForCompletion(true) ? 0 : 1;
	      
	      if(sucess==0){
	    	  
	    	  Counters counter=job.getCounters();
	    	 System.out.println("Bad age: " + counter.findCounter(VoterCounter.BADAGE).getValue());
	    	 System.out.println("wrong number of fields: " + counter.findCounter(VoterCounter.WRONGNUMBER).getValue());
	    	  return 0;
	      }
	      else
	    	  return 1;


	   }

	   public static void main(String[] args) throws Exception { 
	      Configuration conf = new Configuration();
	      System.exit(ToolRunner.run(conf, new VoterDriver(), args));
	   } 

}
