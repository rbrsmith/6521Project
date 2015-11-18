package comp6521.mapreduce.join.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class TaggedKey implements Writable, WritableComparable<TaggedKey> {

	private HashedKey joinKey = new HashedKey();
	private Relation tag;

	public TaggedKey() {}

	public TaggedKey(HashedKey joinKey, Relation tag) {
		this.joinKey = joinKey;
		this.tag = tag;
	}

	public void setJoinKey(HashedKey joinKey) {
		this.joinKey = joinKey;
	}

	public void setTag(Relation tag) {
		this.tag = tag;
	}

	public HashedKey getJoinKey() {
		return joinKey;
	}

	public Relation getTag() {
		return tag;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		joinKey = new HashedKey(in.readInt(), in.readInt(), in.readInt());
		tag = Relation.values()[in.readInt()];
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(joinKey.getA());
		out.writeInt(joinKey.getB());
		out.writeInt(joinKey.getC());
		out.writeInt(tag.ordinal());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((joinKey == null) ? 0 : joinKey.hashCode());
		result = prime * result + ((tag == null) ? 0 : tag.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TaggedKey other = (TaggedKey) obj;
		if (joinKey == null) {
			if (other.joinKey != null)
				return false;
		} else if (!joinKey.equals(other.joinKey))
			return false;
		if (tag != other.tag)
			return false;
		return true;
	}

	@Override
	public int compareTo(TaggedKey taggedKey) {
		int compareValue = this.joinKey.compareTo(taggedKey.getJoinKey());
		if (compareValue == 0) {
			compareValue = this.tag.compareTo(taggedKey.getTag());
		}
		return compareValue;
	}

	@Override
	public String toString() {
		return "TaggedKey [joinKey=" + joinKey + ", tag=" + tag + "]";
	}

}
