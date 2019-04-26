package task3;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * KEYIN<LongWritable>: byte offset of the line
 * VALUEIN<Text>:line of text
 * KEYOUT<LongWritable>: userId
 * VALUEOUT<Text>: tagId
 */
public class MostFrequentTagIdMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	private LongWritable myKey = new LongWritable();
	private Text myVal = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		//extracts and emits the user and the tag assigned in every line of text
		String[] tokens = value.toString().split("\t");
		if (tokens.length >= 2) {
			myKey.set(Long.parseLong(tokens[0]));
			myVal.set(new Text((tokens[2])));
			context.write(myKey, myVal);
		}

	}
}
