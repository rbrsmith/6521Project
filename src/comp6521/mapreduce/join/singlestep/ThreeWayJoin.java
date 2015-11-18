package comp6521.mapreduce.join.singlestep;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import comp6521.mapreduce.join.TaggedJoiningGroupingComparator;
import comp6521.mapreduce.join.TaggedJoiningPartitioner;
import comp6521.mapreduce.join.util.TaggedElement;
import comp6521.mapreduce.join.util.TaggedKey;

public class ThreeWayJoin {
	
	static final int REDUCER_COUNT = 125;
	static final int REDUCER_GRID_SIDE_LENGTH = 5;
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("separator", "\t");

	    Job job = Job.getInstance(conf, "Reduce-Side Three-Way Join");
	    job.setJarByClass(ThreeWayJoin.class);
		job.setSpeculativeExecution(false);

		job.setMapperClass(ThreeWayJoinMapper.class);
		job.setPartitionerClass(TaggedJoiningPartitioner.class);
        job.setGroupingComparatorClass(TaggedJoiningGroupingComparator.class);
		job.setReducerClass(ThreeWayJoinReducer.class);
		job.setNumReduceTasks(REDUCER_COUNT);

		job.setMapOutputKeyClass(TaggedKey.class);
		job.setMapOutputValueClass(TaggedElement.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
	    
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
