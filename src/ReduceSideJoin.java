package comp6521.mapreduce.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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

public class ReduceSideJoin {
  
  private enum Relation {
    R(0), S(1);
    
    private int key;
    
    private Relation(int pKey) {
      key = pKey;
    }
    
    public int getKey() {
      return key;
    }
  }

  /**
   * Utility class to hold the source set and the actual value
   * of an integer element.
   */
  public static class SourcedElement implements Writable {
    private Relation sourceRelation;
    private int val;

    public SourcedElement() {
      set(sourceRelation, val);
    }

    public SourcedElement(Relation source, int val) {
      set(source, val);
    }

    public void set(Relation source, int val) {
      this.sourceRelation = source;
      this.val = val;
    }

    public Relation getSourceRelation() {
      return sourceRelation;
    }

    public int getVal() {
      return val;
    }

    @Override
    public void write(DataOutput out) throws IOException {
      out.writeInt(sourceRelation.getKey());
      out.writeInt(val);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      sourceRelation = in.readInt() == 0 ? Relation.R : Relation.S;
      val = in.readInt();
    }

    @Override
    public String toString() {
      return sourceRelation + " " + val;
    }
  }

  /**
   * Map each line to its key, in R the key is the second input In S the key
   * is the first input
   *
   */
  public static class Map extends MapReduceBase 
    implements Mapper<LongWritable, Text, IntWritable, SourcedElement> {
    private Text sourceRelation = new Text();
    private IntWritable x = new IntWritable();
    private IntWritable y = new IntWritable();
    private SourcedElement sourcedElement = new SourcedElement();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<IntWritable, SourcedElement> output,
        Reporter reporter) throws IOException {
      String line = value.toString();
      String[] tuple = line.split("\t");

      sourceRelation.set(tuple[0]);
      x.set(Integer.parseInt(tuple[1]));
      y.set(Integer.parseInt(tuple[2]));

      Text s = new Text(Relation.S.name());
      // Set key and value depending on which relation the input came from
      if (sourceRelation.equals(s)) {
        sourcedElement.set(Relation.S, y.get());
        output.collect(x, sourcedElement);
      } else {
        sourcedElement.set(Relation.R, x.get());
        output.collect(y, sourcedElement);
      }
    }
  }

  /**
   * At each reducer the key is the join key and the list of values consists of tuples 
   * containing the source relation and the actual value. Then the reducer
   * combines all values from R and S to form the result of the join.
   */
  public static class Reduce extends MapReduceBase
      implements Reducer<IntWritable, SourcedElement, Text, NullWritable> {
    @Override
    public void reduce(IntWritable key, Iterator<SourcedElement> values,
        OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {
      // FIXME: Can't do this - these might very well not fit in memory.
      List<Integer> sList = new ArrayList<Integer>();
      List<Integer> rList = new ArrayList<Integer>();
      while (values.hasNext()) {
        SourcedElement value = values.next();
        Relation sourceRelation = value.getSourceRelation();
        ((sourceRelation == Relation.R) ? rList : sList).add(value.getVal());
      }

      // Gather results
      Text joinStr = new Text();
      for (int sVal : sList) {
        for (int rVal : rList) {
          joinStr.set(rVal + " " + key + " " + sVal);
          output.collect(joinStr, NullWritable.get());
        }
      }

    }
  }

  public static void main(String[] args) throws Exception {
    JobConf conf = new JobConf(ReduceSideJoin.class);
    conf.setJobName("Reduce-side Join");

    conf.setSpeculativeExecution(false);
    conf.setBoolean("mapreduce.map.speculative", false);
    conf.setInt("mapred.tasktracker.reduce.tasks.maximum", 32);

    conf.setOutputKeyClass(IntWritable.class);
    conf.setOutputValueClass(SourcedElement.class);

    conf.setMapperClass(Map.class);
    conf.setReducerClass(Reduce.class);

    conf.setInputFormat(TextInputFormat.class);
    conf.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    JobClient.runJob(conf);
  }

}

