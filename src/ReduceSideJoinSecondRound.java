package TwoRoundJoin;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;





public class ReduceSideJoinSecondRound {

  /**
   *  Tuple holds either single or double values 	
   *
   */
  public static class Tuple implements Writable {
	  private IntWritable x;
	  private IntWritable y;
	  // This can be done better
	  // Simple flag indicating size of tuple
	  private boolean ignoreY;
	  
	  public Tuple(){
		  set(x, y);
	  }
	  public Tuple(int x, int y) {
		  set(new IntWritable(x), new IntWritable(y));
	  }
	  public Tuple(IntWritable x, IntWritable y) {
		  set(x, y);
	  }
	  public Tuple(int x) {
		  set(new IntWritable(x), new IntWritable(-1));
		  this.ignoreY = true;
	  }
	  public Tuple(IntWritable x) {
		  set(x,  new IntWritable(-1));
		  this.ignoreY = true;
	  }
	  
	  public void set(IntWritable x, IntWritable y) {
		  this.x = x;
		  this.y = y;
		  this.ignoreY = false;
	  }
	  
	  public void set(IntWritable x) {
		  this.x = x;
		  this.y = new IntWritable(-1);
		  this.ignoreY = true;
	  }
	  
	  public IntWritable getX() {
		  return this.x;
	  }
	  public IntWritable getY() {
		  return this.y;
	  }
	  
	  public int numValues() {
		  if(this.ignoreY) { 
			  return 1;
		  } else {
			  return 2;
		  }
	  }
	  
	  @Override
	   public void write(DataOutput out) throws IOException {
		  out.writeInt(this.x.get());
			  out.writeInt(this.y.get());
			  out.writeBoolean(this.ignoreY);
		}

	    @Override
	    public void readFields(DataInput in) throws IOException {
	      this.x = new IntWritable(in.readInt());
	      this.y = new IntWritable(in.readInt());
	      this.ignoreY = in.readBoolean();
	    }

	    @Override
	    public String toString() {
	      return "(" + this.x + " " + this.y + ")";
	    }
	  
  }
  
  
  /**
   * Maps values to key -> tuple (a,b) or key -> tuple(a, null)
   */
  public static class Map extends MapReduceBase 
  	 implements Mapper<LongWritable, Text, IntWritable, Tuple> {
	  private Text sourceRelation = new Text();
	  private Tuple tuple = new Tuple();
	  private int a;
	  private int b;
	  private int c;
	  
	  public void map(LongWritable key, Text value, OutputCollector<IntWritable, Tuple> output, Reporter reporter) throws IOException {
		  String line = value.toString();
		  String[] split = line.split("\t");
		  sourceRelation.set(split[0]);
		  
		  
		  if(sourceRelation.equals(new Text("Z"))) {
			  // This is a triple tuple (Z, a,b,c)
			  // we emit (a, b) to key c
			  a =  Integer.parseInt(split[1]);
			  b = Integer.parseInt(split[2]);
			  c = Integer.parseInt(split[3]);
			  tuple.set(new IntWritable(a), new IntWritable(b));
			  output.collect(new IntWritable(c), tuple);
			  
		  } else {
			  // This is double tuple (T, c, a)
			  // we emit (a, null) to key c
			  a = Integer.parseInt(split[2]);
			  c = Integer.parseInt(split[1]);
			  tuple.set(new IntWritable(a));
			  output.collect(new IntWritable(c), tuple);
		  }
		  
		  
	  }
  }
  

  /**
   * For key k values are tuples in form (a1,b) or (a2, null)
   * If a1 = a2 we have a multi-join and emit a,b,k
   *
   */
  public static class Reduce extends MapReduceBase
  		implements Reducer<IntWritable, Tuple, Text, NullWritable> {
	  
	  public void reduce(IntWritable key, Iterator<Tuple> values, OutputCollector<Text, NullWritable> output, Reporter reporter) throws IOException {
		 List<Tuple> zList = new ArrayList<Tuple>();
		 List<Tuple> tList = new ArrayList<Tuple>();
		 Tuple tuple;
		 while(values.hasNext()) {
			 tuple = values.next();
			 int size = tuple.numValues();
			 if(size == 1) {
				 Tuple tup = new Tuple();
				 tup.set(tuple.getX());
				 tList.add(tup);
			 } else {
				 Tuple tup = new Tuple();
				 tup.set(tuple.getX(), tuple.getY());
				 zList.add(tup);
			 }
		 }
		 
		 
		 Text joinStr = new Text();
		 for(Tuple zTuple : zList ) {
			 IntWritable zA = zTuple.getX();
			 IntWritable zB = zTuple.getY();
			 for(Tuple tTuple : tList) {
				 IntWritable tA = tTuple.getX();
				 if(tA.get() == zA.get()) {
					 joinStr.set(zA + ", " + zB + ", " + key.get());
					 output.collect(joinStr, NullWritable.get());
				 }
			 }
		 }
	  }
   }
}
