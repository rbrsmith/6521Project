package comp6521.mapreduce.join.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * This class is essentially a three-tuple representing coordinates of a reducer
 * in a cube.
 */
public class HashedKey implements WritableComparable<HashedKey> {
	
	private int a, b, c;
	
	public HashedKey() {};

	public HashedKey(int a, int b, int c) {
		super();
		this.a = a;
		this.b = b;
		this.c = c;
	}

	public int getA() {
		return a;
	}

	public int getB() {
		return b;
	}

	public int getC() {
		return c;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + a;
		result = prime * result + b;
		result = prime * result + c;
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
		HashedKey other = (HashedKey) obj;
		if (a != other.a)
			return false;
		if (b != other.b)
			return false;
		if (c != other.c)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "HashedKey [a=" + a + ", b=" + b + ", c=" + c + "]";
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		a = input.readInt();
		b = input.readInt();
		c = input.readInt();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(a);
		output.writeInt(b);
		output.writeInt(c);
	}

	@Override
	public int compareTo(HashedKey o) {
		if (a == o.a) {
			if (b == o.b) {
				if (c == o.c) {
					return 0;
				}
				return c < o.c ? - 1 : 1;
			}
			return b < o.b ? -1 : 1;
		}
		
		return a < o.a ? -1 : 1;
	}
	
}
