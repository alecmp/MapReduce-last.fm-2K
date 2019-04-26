package task2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ArtistTagsMapper extends Mapper<Object, Text, LongWritable, Text> {

	private LongWritable myKey = new LongWritable();
	private Text myVal = new Text();
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String[] tokens = value.toString().split("\t");
		if (tokens.length >= 3) {
			myKey.set(Long.parseLong(tokens[1]));
			myVal.set(new Text((tokens[0])));
			
			if (Utils.isMoreRecentThan10Years(tokens[3])) {
				context.write(myKey, myVal); // tagID, UserID
			}

		}

	}
}