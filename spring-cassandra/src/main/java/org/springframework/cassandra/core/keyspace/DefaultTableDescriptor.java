package org.springframework.cassandra.core.keyspace;

/**
 * Convenient default implementation of {@link TableDescriptor} as an extension of {@link TableSpecification} that
 * doesn't require the use of generics.
 * 
 * @author Matthew T. Adams
 */
public class DefaultTableDescriptor extends TableSpecification<DefaultTableDescriptor> {

	/**
	 * Factory method to produce a new {@link DefaultTableDescriptor}. Convenient if imported statically.
	 */
	public static DefaultTableDescriptor table() {
		return new DefaultTableDescriptor();
	}
}
