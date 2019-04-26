package task3;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper <KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
 * KEYIN<LongWritable>: byte offset of the line 
 * VALUEIN<Text>: line of text 
 * KEYOUT<Text>: tagId 
 * VALUEOUT<Text>:tagDescription
 */
public class TagJoin2Mapper extends Mapper<LongWritable, Text, Text, Text> {

	private Text myKey = new Text();
	private Text myVal = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// extracts and emits the tag and its description - prefixed by "!" - in
		// every line of text.
		String[] tokens = value.toString().split(";");
		if (tokens.length >= 1) {
			myKey.set(new Text(tokens[0]));
			myVal.set(new Text("!" + tokens[1]));
			context.write(myKey, myVal);
		}

	}
}
