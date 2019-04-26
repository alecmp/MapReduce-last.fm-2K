package task2;

import java.io.IOException;
import java.util.HashSet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ArtistTagsReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

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