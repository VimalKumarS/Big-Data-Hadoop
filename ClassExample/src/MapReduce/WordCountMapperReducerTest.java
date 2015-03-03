package MapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import Voter.*;
public class WordCountMapperReducerTest {

	MapDriver<LongWritable, Text, Text, Text> mapDriver;
	ReduceDriver<Text, Text, Text, FloatWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, Text, Text, FloatWritable> mapReduceDriver;
	MapDriver<LongWritable,Text,Text,IntWritable> vmapDriver;
	@Before
	public void setUp() {
		RecieptMapper mapper = new RecieptMapper();
		RecieptReduce reducer = new RecieptReduce();
	   Voter.VoterMapper vmap=new VoterMapper();
		vmapDriver = MapDriver.newMapDriver(vmap);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);

		mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	}

	@Test
	public void testMapper() throws IOException {
		//mapDriver.withInput(new LongWritable(), new Text("1902,562,485,77,562"));
		//mapDriver.withInput(new LongWritable(), new Text("1912,562,485,79,562"));
		//mapDriver.withOutput(new Text("summary"), new Text("1902_77"));
		//mapDriver.withOutput(new Text("summary"), new Text("1912_79"));
		//mapDriver.withOutput(new Text("is"), new IntWritable(1));
		//mapDriver.withOutput(new Text("cool"), new IntWritable(1));
		//mapDriver.runTest();
		vmapDriver.withInput(new LongWritable(), new Text("1,david davidson,49.0,socialist,369.78,5108"));
		vmapDriver.withOutput(new Text("socialist"), new IntWritable(49));
		vmapDriver.runTest();
	}

	//@Test
	public void testReducer() throws IOException {
		List<Text> values = new ArrayList<Text>();
		values.add(new Text("1902_77"));
		values.add(new Text("1902_78"));
		reduceDriver.withInput(new Text("summary"), values);
		reduceDriver.withOutput(new Text("min(1902)"), new FloatWritable(77));
		reduceDriver.runTest();
	}

}
