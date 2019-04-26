package task1;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendCountMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

	private LongWritable myKey = new LongWritable();
	private Text myVal = new Text();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] tokens = value.toString().split("\t");
		if (tokens.length >= 1) {
			myKey.set(Long.parseLong(tokens[0]));
			myVal.set(new Text((tokens[1])));
			context.write(myKey, myVal); // tagID, UserID
		}

	}
}
