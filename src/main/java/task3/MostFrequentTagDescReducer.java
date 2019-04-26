package task3;

import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer <KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 * KEYIN<LongWritable>: userId
 * VALUEIN<Text>: tagId
 * KEYOUT<LongWritable>: userId
 * VALUEOUT<Text>: tagId
 */

public class MostFrequentTagDescReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		HashSet<Text> set = new HashSet<Text>();
		String finalVal = "";
		for (Text val : values) {
			if (!set.contains(val)) {
				set.add(val);
				finalVal = finalVal.concat(val.toString()).concat(",");
			}
		}

		finalVal = finalVal.substring(0, finalVal.length() - 1);
		context.write(key, new Text(finalVal));

	}
}
