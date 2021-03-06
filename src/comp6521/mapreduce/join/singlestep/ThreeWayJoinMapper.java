package comp6521.mapreduce.join.singlestep;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;

import comp6521.mapreduce.join.util.HashedKey;
import comp6521.mapreduce.join.util.Relation;
import comp6521.mapreduce.join.util.TaggedElement;
import comp6521.mapreduce.join.util.TaggedKey;
import javafx.util.Pair;

public class ThreeWayJoinMapper extends Mapper<LongWritable, Text, TaggedKey, TaggedElement> {

	private Splitter splitter;

	@Override
	protected void setup(Context context) {
		splitter = Splitter.on(context.getConfiguration().get("separator")).trimResults();
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		List<String> values = Lists.newArrayList(splitter.split(value.toString()));
		Relation sourceRelation = Relation.valueOf(values.get(0));
		int first = Integer.parseInt(values.get(1));
		int second = Integer.parseInt(values.get(2));

		// Hash the relation's join keys so that they end up in a reducer where
		// they'll potentially find other matches
		int hashedFirst = first % ThreeWayJoin.REDUCER_GRID_SIDE_LENGTH;
		int hashedSecond = second % ThreeWayJoin.REDUCER_GRID_SIDE_LENGTH;

		for (int i = 0; i < ThreeWayJoin.REDUCER_GRID_SIDE_LENGTH; ++i) {
			if (sourceRelation == Relation.R) {
				context.write(new TaggedKey(new HashedKey(hashedFirst, hashedSecond, i), sourceRelation),
						new TaggedElement(sourceRelation, new Pair<Integer, Integer>(first, second)));
			} else if (sourceRelation == Relation.S) {
				context.write(new TaggedKey(new HashedKey(i, hashedFirst, hashedSecond), sourceRelation),
						new TaggedElement(sourceRelation, new Pair<Integer, Integer>(first, second)));
			} else {
				context.write(new TaggedKey(new HashedKey(hashedSecond, i, hashedFirst), sourceRelation),
						new TaggedElement(sourceRelation, new Pair<Integer, Integer>(first, second)));
			}
		}
	}
}
