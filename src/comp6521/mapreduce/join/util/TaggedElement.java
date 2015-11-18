package comp6521.mapreduce.join.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

import javafx.util.Pair;

/**
 * Utility class to hold the source relation and the actual value
 * of an integer element.
 */
public class TaggedElement implements Writable, Comparable<TaggedElement> {

	private Relation sourceRelation;
	private Pair<Integer, Integer> value;

	public TaggedElement() {}
	
	public TaggedElement(Relation source, Pair<Integer, Integer> value) {
		this.sourceRelation = source;
		this.value = value;
	}

	public Relation getSourceRelation() {
		return sourceRelation;
	}

	public Pair<Integer, Integer> getValue() {
		return value;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(sourceRelation.ordinal());
		out.writeInt(value.getKey());
		out.writeInt(value.getValue());
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		sourceRelation = Relation.values()[in.readInt()];
		value = new Pair<Integer, Integer>(in.readInt(), in.readInt());
	}

	@Override
	public String toString() {
		return sourceRelation + " " + value;
	}

	@Override
	public int compareTo(TaggedElement o) {
		int relationOrdering = sourceRelation.compareTo(o.sourceRelation);

		if (relationOrdering == 0) {
			if (value.getKey() == o.getValue().getKey()) {
				return value.getValue().compareTo(o.getValue().getValue());
			}
			return value.getKey() < o.getValue().getKey() ? - 1 : 1;
		}
		
		return relationOrdering;
	}

}
