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
public enum CassandraPrimaryKeyColumnAnnotationComparator implements Comparator<PrimaryKeyColumn> {

	/**
	 * The sole instance of this class.
	 */
	IT;

	@Override
	public int compare(PrimaryKeyColumn left, PrimaryKeyColumn right) {

		int comparison = left.type().compareTo(right.type());
		if (comparison != 0) {
			return comparison;
		}

		comparison = new Integer(left.ordinal()).compareTo(right.ordinal());
		if (comparison != 0) {
			return comparison;
		}

		comparison = left.name().compareTo(right.name());
		if (comparison != 0) {
			return comparison;
		}

		return left.ordering().compareTo(right.ordering());
	}
}