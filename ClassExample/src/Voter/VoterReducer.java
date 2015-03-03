package Voter;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import java.io.IOException;
import java.text.DecimalFormat;


public class VoterReducer  extends Reducer <Text,IntWritable,Text,FloatWritable> {
	 private static Text tempYear=new Text();
	   private static Text keyText=new Text();
	   private static Text minYear=new Text();
	   private static Text maxYear=new Text();
	   private static String tempString;
	   private static String[] keyString;
	   
	   public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		     // TODO declare and initialize int values for sum and count
		     // TODO declare and initialize long value for max 

		     long tempValue = 0L;
		     long min=Long.MAX_VALUE;
		     long max=Long.MIN_VALUE;
		     float sum=0f,count=0f,mean=0f;
		     for (IntWritable value: values) {
		    	 tempValue = new Long(value.toString()).longValue(); 
		    	 if(tempValue<min){
		    		 min=tempValue;
		    	 }
		    	 if(tempValue>max){
		    		 max=tempValue;
		    	 }
		       
		    	 sum+=tempValue;
		    	 count+=1;
		     }
		     // TODO declare and calculate float value for mean
		     mean=sum/count;

		     keyText.set("min" + "(" + key.toString() + "): ");
		     context.write(keyText, new FloatWritable(min)); 

		     // TODO set the keyText to "max(year):"
		     // TODO write keyText and max to the context 
		     keyText.set("max" + "(" + key.toString() + "): ");
		     context.write(keyText, new FloatWritable(max)); 
		     // TODO set the keyText to "mean:"
		     // TODO write keyText and mean to the context 
		     
		     keyText.set("mean"+ "(" + key.toString() + "): " );
		     context.write(keyText, new FloatWritable( mean)); 
	   }

}
