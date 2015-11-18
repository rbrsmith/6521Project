package comp6521.mapreduce.join.util;

public enum Relation {
	
	R, S, T;
	
	private final int value;
	
	private Relation() {
		this.value = ordinal();
	}
	
	public int getValue() {
		return value;
	}

}
