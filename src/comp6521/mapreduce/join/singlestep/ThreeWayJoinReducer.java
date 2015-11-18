package comp6521.mapreduce.join.singlestep;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import comp6521.mapreduce.join.util.Relation;
import comp6521.mapreduce.join.util.TaggedElement;
import comp6521.mapreduce.join.util.TaggedKey;
import javafx.util.Pair;

public class ThreeWayJoinReducer extends Reducer<TaggedKey, TaggedElement, NullWritable, Text> {

	@Override
	public void reduce(TaggedKey key, Iterable<TaggedElement> values,
			Context context) throws IOException, InterruptedException {
		Text result = new Text();

		// Buffer relations R and S. 
		// Generally speaking we would want to buffer the larger relations to
		// minimize IO
		Set<Pair<Integer, Integer>> rSet = new HashSet<>();
		Set<Pair<Integer, Integer>> sSet = new HashSet<>();
		
		for (TaggedElement val : values) {
			if (val.getSourceRelation() == Relation.R) {
				rSet.add(val.getValue());
			} else if (val.getSourceRelation() == Relation.S) {
				sSet.add(val.getValue());
			} else {
				Integer c = val.getValue().getKey();
				Integer a = val.getValue().getValue();
				for (Pair<Integer, Integer> s : sSet) {
					Integer b = s.getKey();
					if (c == s.getValue() && rSet.contains(new Pair<>(a, b))) {
						result.set(a + " " + b + " " + c);
						context.write(NullWritable.get(), result);
					}
				}
			}
		}
	}

}