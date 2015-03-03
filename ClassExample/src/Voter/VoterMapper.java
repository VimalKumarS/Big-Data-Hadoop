package Voter;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.StringTokenizer;

enum VoterCounter {
	WRONGNUMBER, BADAGE
}

public class VoterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static Log log = LogFactory.getLog(VoterMapper.class);

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO create iterator over record assuming space-separated fields
		String[] voterrec = value.toString().split(",");
		// TODO check number of tokens in iterator
		if (voterrec.length != 6) {
			// TODO if wrong number of fields, increment
			// "wrong number of fields" counter, write log message with entire
			// record, and return
			log.info("VoterMapper log info "+value.toString());
			System.out.println(value.toString());
			context.getCounter(VoterCounter.WRONGNUMBER).increment(1l);
			return;
		}
		// TODO get age field and convert to int
		int age = 0;
		try {
			if (voterrec.length >= 6) {
				// TODO validate age is a reasonable age
				age = (int) (Double.parseDouble(voterrec[2].toString()));
				// TODO if "bad age", increment "bad age" counter, write log
				// message with entire record, and return
				if (age < 18) {
					log.info("VoterMapper log info "+value.toString());
					System.out.println(value.toString());
					context.getCounter(VoterCounter.BADAGE).increment(1l);
					return;
				}
			}
		} catch (NumberFormatException exp) {
			log.info(value.toString());
			System.out.println(value.toString());
			context.getCounter(VoterCounter.BADAGE).increment(1l);
			return;
		}
		// TODO convert age to IntWritable

		// TODO pull out party from record

		// TODO emit (party, age) as key-value pair
		context.write(new Text(voterrec[3].toString()), new IntWritable(age));
	}

}
