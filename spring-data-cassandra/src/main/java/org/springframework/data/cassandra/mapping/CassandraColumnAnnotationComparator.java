package org.springframework.data.cassandra.mapping;

import java.util.Comparator;

/**
 * {@link Comparator} implementation that uses {@link Column#value()}.
 * 
 * @author Matthew T. Adams
 */
public enum CassandraColumnAnnotationComparator implements Comparator<Column> {

	/**
	 * The sole instance of this class.
	 */
	IT;

	@Override
	public int compare(Column left, Column right) {
		return left.value().compareTo(right.value());
	}
}