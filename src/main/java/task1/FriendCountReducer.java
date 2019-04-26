package task1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendCountReducer extends Reducer<LongWritable, Text, LongWritable, IntWritable> {

	private TreeMap<LongWritable, IntWritable> countMap = new TreeMap<LongWritable, IntWritable>();

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		HashSet<Text> set = new HashSet<Text>();
		int sum = 0;
		for (Text val : values) {
			if (!set.contains(val)) {
				set.add(val);
				sum++;
			}
		}
		countMap.put(new LongWritable(key.get()), new IntWritable(sum));
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {

		Set<Entry<LongWritable, IntWritable>> set = countMap.entrySet();
		List<Entry<LongWritable, IntWritable>> list = new ArrayList<Entry<LongWritable, IntWritable>>(set);
		Collections.sort(list, new Comparator<Map.Entry<LongWritable, IntWritable>>() {
			public int compare(Map.Entry<LongWritable, IntWritable> o1, Map.Entry<LongWritable, IntWritable> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});
		int counter = 0;
		for (Map.Entry<LongWritable, IntWritable> entry : list) {
			if (counter < 5) {
				context.write(entry.getKey(), entry.getValue());
				counter++;
			}
		}

	}

}