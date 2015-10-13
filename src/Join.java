import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


/**
 * Assignment 1
 * Reduce-side join
 *
 */
public class Join {
	
	
	/**
	 * Class to hold where the input came from (R or S)
	 * And the value which was read 
	 *
	 */
	public static class TwovalueWritable implements Writable {
	    private int type;
	    private int val;

	    public  TwovalueWritable() {
	        set(type, val);
	    }
	    public  TwovalueWritable(int first, int second) {
	        set(type, val);
	    }
	    public void set(int type, int val) {
	        this.type = type;
	        this.val = val;
	    }
	    public int getType() {
	        return type;
	    }
	    public int getVal() {
	        return val;
	    }
	    @Override
	    public void write(DataOutput out) throws IOException {
	        out.writeInt(type);
	        out.writeInt(val);
	    }
	    @Override
	    public void readFields(DataInput in) throws IOException {
	        type = in.readInt();
	        val = in.readInt();
	    }
	    @Override
	    public String toString() {
	    	return type + " " + val;
	    }
	}
	
	
	/**
	 * Map each line to its key, in R the key is the second input
	 * In S the key is the first input
	 *
	 */
	 public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, TwovalueWritable> {
		 private Text tupleType = new Text();
	     private IntWritable a = new IntWritable();
	     private IntWritable b = new IntWritable();
	     private TwovalueWritable valueType = new TwovalueWritable();

	     public void map(LongWritable key, Text value, OutputCollector<IntWritable, TwovalueWritable> output, Reporter reporter) throws IOException {
	       
	    	String line = value.toString();
	       String[] tuple = line.split("\t");
	       tupleType.set(tuple[0]);
	       a.set(Integer.parseInt(tuple[1]));
	       b.set(Integer.parseInt(tuple[2]));
	       Text s = new Text("S");
	       // Set key and value depending on which tuple the input ccame from
	       if(tupleType.equals(s)) {
	    	   valueType.set(0, b.get());
	    	   output.collect(a, valueType);
	       } else {
	    	   valueType.set(1, a.get());
	    	   output.collect(b, valueType);
	       }
	     }
	   }

	 
	 /**
	  *  All keys are unique and list of values are tuple containing value and where the value came from (R or S) 
	  *
	  */
	 public static class Reduce extends MapReduceBase implements Reducer<IntWritable, TwovalueWritable, Text, NullWritable> {

		 public void reduce(IntWritable key, Iterator<TwovalueWritable> values, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {
			 // Capture all R values in this key and S values
			 ArrayList<Integer> sList = new ArrayList<Integer>();
	    	 ArrayList<Integer> rList = new ArrayList<Integer>();
			 Text joinStr = new Text();
	    	 int type;
	    	 int val;
	    	 while (values.hasNext()) {
	    		 TwovalueWritable value = values.next();    
	    		 type = value.getType();
	    		 val = value.getVal();
	    		 // Add to appropriate list
	    		 if(type == 1) {
	    			 rList.add(value.getVal());
	    		 } else {
	    			 sList.add(value.getVal());
	    		 }
	        }
	    	 // Gather results
	    	for(int sVal : sList){
    			for (int rVal : rList) {
    				joinStr.set(rVal + " " + key + " " + sVal);
    				TwovalueWritable fVal = new TwovalueWritable(1, 2);
    				output.collect(joinStr, NullWritable.get());

	    		}
	    	}

	     }
	 }
	    

	 // Run the job
	 public static void main(String[] args) throws Exception {
	     JobConf conf = new JobConf(Join.class);
	     conf.setJobName("join");

	     conf.setSpeculativeExecution(false);
	     conf.setBoolean("mapreduce.map.speculative", false);
	     conf.setInt("mapred.tasktracker.reduce.tasks.maximum", 32);
	     
	     conf.setOutputKeyClass(IntWritable.class);
	     conf.setOutputValueClass(TwovalueWritable.class);

	     conf.setMapperClass(Map.class);
	  //   conf.setCombinerClass(Reduce.class);
	     conf.setReducerClass(Reduce.class);

	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);

	     FileInputFormat.setInputPaths(conf, new Path(args[0]));
	     FileOutputFormat.setOutputPath(conf, new Path(args[1]));

	     JobClient.runJob(conf);
	   }
	
	
}
