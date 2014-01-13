package org.springframework.data.cassandra.mapping;

import java.util.Comparator;

import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKeyType;

/**
 * {@link Comparator} implementation that uses, in order, the
 * <ul>
 * <li>{@link PrimaryKeyColumn#type()}, then, if ordered the same,</li>
 * <li>{@link PrimaryKeyColumn#ordinal()}, then, if ordered the same</li>
 * <li>{@link PrimaryKeyColumn#name()}, then, if ordered the same,</li>
 * <li>{@link PrimaryKeyColumn#ordering()}.</li>
 * </ul>
 * 
 * @see PrimaryKeyType#compareTo(PrimaryKeyType)
 * @see Ordering#compareTo(Ordering)
 * 
 * @author Matthew T. Adams
 */
public enum DefaultCassandraPrimaryKeyColumnComparator implements Comparator<PrimaryKeyColumn> {

	/**
	 * The sole instance of this class.
	 */
	IT;

	@Override
	public int compare(PrimaryKeyColumn o1, PrimaryKeyColumn o2) {

		int comparison = o1.type().compareTo(o2.type());
		if (comparison != 0) {
			return comparison;
		}

		comparison = new Integer(o1.ordinal()).compareTo(o2.ordinal());
		if (comparison != 0) {
			return comparison;
		}

		comparison = o1.name().compareTo(o2.name());
		if (comparison != 0) {
			return comparison;
		}

		return o1.ordering().compareTo(o2.ordering());
	}
}