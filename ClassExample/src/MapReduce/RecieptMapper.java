package MapReduce;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;


public class RecieptMapper extends Mapper<LongWritable, Text, Text, Text> {

	  public void map(LongWritable key, Text value, Context context)
		      throws IOException, InterruptedException
		      {
		      StringTokenizer iterator =new StringTokenizer(value.toString(),",");
		      String year =iterator.nextToken();
		      iterator.nextToken();
		      iterator.nextToken();
		      String delta= iterator.nextToken();
		      context.write(new Text("summary"), new Text(year+"_"+delta));
		      }
	
}


