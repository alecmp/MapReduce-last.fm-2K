package task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Reducer <KEYIN, VALUEIN, KEYOUT, VALUEOUT> 
 * KEYIN<LongWritable>: tagId 
 * VALUEIN<Text>: tagDescription/userId 
 * KEYOUT<Text>: userId 
 * VALUEOUT<Text>:tagDescription
 */
public class TagJoinReducer extends Reducer<Text, Text, LongWritable, Text> {

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		Text desc = null;
		ArrayList<Long> users = new ArrayList<Long>();

		// iterates over all the values. If the value starts with a "!", it
		// stored as the tag description. Otherwise it adds it to users array.
		for (Text val : values) {
			if (val.toString().substring(0, 1).equals("!")) {
				desc = new Text(val.toString().substring(1));
			} else {
				users.add(Long.parseLong(val.toString()));
			}
		}

		// for each user in the array, emits a pair of type (user, tag
		// description).
		for (Long u : users) {
			LongWritable key2 = new LongWritable(u);
			context.write(key2, desc);
		}

	}
}
