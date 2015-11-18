package comp6521.mapreduce.join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import comp6521.mapreduce.join.util.TaggedKey;

public class TaggedJoiningGroupingComparator extends WritableComparator {

	public TaggedJoiningGroupingComparator() {
		super(TaggedKey.class, true);
	}

	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TaggedKey key1 = (TaggedKey) a;
		TaggedKey key2 = (TaggedKey) b;
		return key1.getJoinKey().compareTo(key2.getJoinKey());
	}

}
