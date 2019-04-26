package task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Driver(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		Configuration conf = new Configuration();

		int returnValue = 0;
		Path inputDir = new Path(args[0]);
		Path inputDir2 = new Path(args[1]);
		Path outputDir = new Path(args[2]);
		Path outputDirIntermediate = new Path(args[2] + "_int");
		Path outputDirIntermediate2 = new Path(args[2] + "_int2");

		Job job = new org.apache.hadoop.mapreduce.Job();
		job.setJarByClass(Driver.class);
		job.setJobName("MostFrequentTagId");

		FileInputFormat.addInputPath(job, inputDir);
		FileOutputFormat.setOutputPath(job, outputDirIntermediate);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(MostFrequentTagIdMapper.class);
		job.setReducerClass(MostFrequentTagIdReducer.class);

		returnValue = job.waitForCompletion(true) ? 0 : 1;
		System.out.println("job.isSuccessful " + job.isSuccessful());

		if (returnValue == 0) {

			Job job2 = new org.apache.hadoop.mapreduce.Job();
			job2.setJarByClass(Driver.class);
			job2.setJobName("TagJoin");

			MultipleInputs.addInputPath(job2, outputDirIntermediate, TextInputFormat.class, TagJoin1Mapper.class);
			MultipleInputs.addInputPath(job2, inputDir2, TextInputFormat.class, TagJoin2Mapper.class);
			FileOutputFormat.setOutputPath(job2, outputDirIntermediate2);
		
			job2.setMapperClass(TagJoin1Mapper.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(Text.class);
			job2.setOutputKeyClass(LongWritable.class);
			job2.setOutputValueClass(Text.class);
			job2.setOutputFormatClass(TextOutputFormat.class);
			job2.setReducerClass(TagJoinReducer.class);
			
			returnValue = job2.waitForCompletion(true) ? 0 : 1;
			FileSystem.get(conf).delete(outputDirIntermediate, true);
			System.out.println("job.isSuccessful " + job2.isSuccessful());

			if (returnValue == 0) {

				Job job3 = new org.apache.hadoop.mapreduce.Job();
				job3.setJarByClass(Driver.class);
				job3.setJobName("MostFrequentTagDesc");

				FileInputFormat.addInputPath(job3, outputDirIntermediate2);
				FileOutputFormat.setOutputPath(job3, outputDir);
				
				job3.setMapOutputKeyClass(LongWritable.class);
				job3.setMapOutputValueClass(Text.class);
				job3.setOutputKeyClass(LongWritable.class);
				job3.setOutputValueClass(Text.class);
				job3.setOutputFormatClass(TextOutputFormat.class);
				job3.setMapperClass(MostFrequentTagDescMapper.class);
				job3.setReducerClass(MostFrequentTagDescReducer.class);

				returnValue = job3.waitForCompletion(true) ? 0 : 1;
				FileSystem.get(conf).delete(outputDirIntermediate2, true);
				System.out.println("job.isSuccessful " + job3.isSuccessful());
			}

		}

		return returnValue;
	}
}

