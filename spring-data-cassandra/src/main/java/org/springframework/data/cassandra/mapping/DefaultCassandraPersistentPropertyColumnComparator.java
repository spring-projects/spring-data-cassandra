package org.springframework.data.cassandra.mapping;

import java.util.Comparator;

/**
 * {@link Comparator} implementation that uses the {@link CassandraPersistentProperty}'s column name for ordering.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
public enum DefaultCassandraPersistentPropertyColumnComparator implements Comparator<CassandraPersistentProperty> {

	/**
	 * The sole instance of this class.
	 */
	IT;

	@Override
	public int compare(CassandraPersistentProperty o1, CassandraPersistentProperty o2) {
		return o1.getColumnName().compareTo(o2.getColumnName());
	}
}