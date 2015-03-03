package MapReduce;

import java.io.IOException;
import java.util.Iterator;


import org.apache.hadoop.io.FloatWritable;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

//import org.apache.hadoop.mapred.Reducer;
//import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class RecieptReduce extends Reducer<Text, Text, Text, FloatWritable> {
    public void reduce(Text key, Iterable<Text> values,
    		Context context) 
    				throws IOException, InterruptedException {
       String compositeString;
       String[] compositeStringArray;
       float min=Integer.MAX_VALUE;
       Text tempYear;
       Long tempValue;
       Text minYear=null;
    	for (Text value : values) {
			 compositeString=value.toString();
			 compositeStringArray=compositeString.split("_");
			 tempYear =new Text(compositeStringArray[0]);
			 tempValue=new Long(compositeStringArray[1]).longValue();
			 if(tempValue<min){
				 min =tempValue;
				 minYear=tempYear;
				 
			 }
		}
    	Text keytext=new Text("min"+"("+minYear.toString()+")");
    	context.write(keytext,new FloatWritable(min));
    	
    }
}
