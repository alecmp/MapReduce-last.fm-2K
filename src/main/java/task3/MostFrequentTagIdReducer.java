package task3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Reducer <KEYIN, VALUEIN, KEYOUT, VALUEOUT> KEYIN<LongWritable>: userId
 * KEYIN<LongWritable>: userId
 * VALUEIN<Text>: tagId
 * KEYOUT<LongWritable>: userId
 * VALUEOUT<Text>: tagId
 */

public class MostFrequentTagIdReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

	@Override
	protected void reduce(LongWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		// HashMap of type <tagId, #occurrences> to store all the tags assigned
		// by the user with their respective number of occurrences.
		Map<String, Integer> countMap = new HashMap<String, Integer>();

		// Iterates over all the tags. If the tag is already present in the HashMap,
		// it increments the number of occurrences; otherwise it adds it to the
		// HashMap with occurrences equal to 1.
		for (Text val : values) {
			int freq = 1;
			if (countMap.containsKey(val.toString())) {
				freq += countMap.get(val.toString());
			}
			countMap.put(val.toString(), freq);
		}
		// gets the highest value in the HashMap, i.e. the highest number of
		// occurrences.
		// There could be more than one tag with this number of occurrences.
		Map.Entry<String, Integer> maxEntry = null;
		for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
			if (maxEntry == null || entry.getValue().compareTo(maxEntry.getValue()) > 0) {
				maxEntry = entry;
			}
		}

		// Iterates over all the entries in the HashMap. If the value (i.e.
		// number of occurrences) is equal to the max value found in the
		// previous step, it emits a pair (user, tag).
		for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
			if (entry.getValue().compareTo(maxEntry.getValue()) == 0) {
				context.write(key, new Text(entry.getKey().toString()));

			}
		}

	}

}