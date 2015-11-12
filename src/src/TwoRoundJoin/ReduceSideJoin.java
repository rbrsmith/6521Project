package TwoRoundJoin;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class ReduceSideJoin {

  public static void main(String[] args) throws Exception {

	 int NUM_REDUCERS = 32; 
	  
	// First Round  
	JobConf conf = new JobConf(ReduceSideJoinFirstRound.class);
    conf.setJobName("First Round");

    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(ReduceSideJoinFirstRound.SourcedElement.class);

    conf.setMapperClass(ReduceSideJoinFirstRound.Map.class);
    conf.setReducerClass(ReduceSideJoinFirstRound.Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    

	conf.setNumReduceTasks(NUM_REDUCERS);
    JobClient.runJob(conf);
    
    File currentDirFile = new File(".");
    String helper = currentDirFile.getAbsolutePath();
    String currentDir = helper.substring(0, helper.length() -1);
    
    File source = new File(currentDir + "input/t.txt");
    File dest = new File(currentDir + "output/t.txt");
    try {
        FileUtils.copyFile(source, dest);
    } catch (IOException e) {
        e.printStackTrace();
    }
    
    
    // Second Round
    JobConf conf2 = new JobConf(ReduceSideJoinSecondRound.class);
    conf2.setJobName("Second Round");
    conf2.setMapOutputKeyClass(IntWritable.class);
    conf2.setMapOutputValueClass(ReduceSideJoinSecondRound.Tuple.class);
    conf2.setOutputKeyClass(IntWritable.class);
    conf2.setOutputValueClass(Text.class);
    
    conf2.setMapperClass(ReduceSideJoinSecondRound.Map.class);
    conf2.setReducerClass(ReduceSideJoinSecondRound.Reduce.class);
    FileInputFormat.setInputPaths(conf2, new Path(args[1]));
    FileOutputFormat.setOutputPath(conf2, new Path(args[2]));
	conf.setNumReduceTasks(NUM_REDUCERS);
    JobClient.runJob(conf2);
    
  }
}