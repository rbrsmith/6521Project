package comp6521.mapreduce.join.singlestep;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import comp6521.mapreduce.join.util.Relation;
import comp6521.mapreduce.join.util.TaggedElement;
import comp6521.mapreduce.join.util.TaggedKey;
import javafx.util.Pair;

public class ThreeWayJoinReducer extends Reducer<TaggedKey, TaggedElement, NullWritable, Text> {

	@Override
	public void reduce(TaggedKey key, Iterable<TaggedElement> values, Context context)
			throws IOException, InterruptedException {
		Text result = new Text();

		// Use maps with counts to handle duplicates
		Map<Pair<Integer, Integer>, Integer> rCounts = new HashMap<>();
		Map<Pair<Integer, Integer>, Integer> sCounts = new HashMap<>();

		for (TaggedElement val : values) {
			if (val.getSourceRelation() == Relation.R) {
				rCounts.computeIfPresent(val.getValue(), (k, oldValue) -> oldValue + 1);
				rCounts.putIfAbsent(val.getValue(), 1);
			} else if (val.getSourceRelation() == Relation.S) {
				sCounts.computeIfPresent(val.getValue(), (k, oldValue) -> oldValue + 1);
				sCounts.putIfAbsent(val.getValue(), 1);
			} else {
				Integer c = val.getValue().getKey();
				Integer a = val.getValue().getValue();

				for (Pair<Integer, Integer> sEntry : sCounts.keySet()) {
					if (c != sEntry.getValue()) {
						continue;
					}
					Integer b = sEntry.getKey();
					for (int i = 0; i < sCounts.get(sEntry); ++i) {
						Integer rCount = rCounts.get(new Pair<>(a, b));
						if (rCount == null) {
							break;
						}
						for (int j = 0; j < rCount; ++j) {
							result.set(a + " " + b + " " + c);
							context.write(NullWritable.get(), result);
						}
					}
				}
			}
		}
	}

}
