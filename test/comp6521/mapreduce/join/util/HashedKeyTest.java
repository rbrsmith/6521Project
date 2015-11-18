package comp6521.mapreduce.join.util;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class HashedKeyTest {

	@Test
	public void test() {
		HashedKey x = new HashedKey(1, 1, 1);
		HashedKey y = new HashedKey(1, 2, 1);
		HashedKey z = new HashedKey(1, 1, 1);
		HashedKey z0 = new HashedKey(0, 0, 0);
		HashedKey w = new HashedKey(3, 0, 1);
		HashedKey a = new HashedKey(1, 2, 3);
		assertTrue(x.compareTo(y) == -1);
		assertTrue(x.compareTo(z) == 0);
		assertTrue(x.compareTo(z0) == 1);
		assertTrue(x.compareTo(w) == -1);
		assertTrue(y.compareTo(a) == -1);
		assertTrue(w.compareTo(y) == 1);
	}

}
