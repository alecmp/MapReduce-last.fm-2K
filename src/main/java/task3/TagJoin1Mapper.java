package task3;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * KEYIN<LongWritable>: userId
 * VALUEIN<Text>: tagId
 * KEYOUT<Text>: tagId
 * VALUEOUT<Text>: userId
 */
public class TagJoin1Mapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text myKey = new Text();
	private Text myVal = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//swaps user and tag in every line of text in order to emit the tag as key.
		String[] tokens = value.toString().split("\t");
		if (tokens.length >= 1) {
			myKey.set(new Text(tokens[1]));
			myVal.set(new Text(tokens[0]));
			context.write(myKey, myVal);
		}

	}
}
