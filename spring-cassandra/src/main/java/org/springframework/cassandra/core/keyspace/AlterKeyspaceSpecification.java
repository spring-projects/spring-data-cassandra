package org.springframework.cassandra.core.keyspace;

public class AlterKeyspaceSpecification extends KeyspaceOptionsSpecification<AlterKeyspaceSpecification> {

	/**
	 * Entry point into the {@link AlterKeyspaceSpecification}'s fluent API to alter a keyspace. Convenient if imported
	 * statically.
	 */
	public static AlterKeyspaceSpecification alterKeyspace() {
		return new AlterKeyspaceSpecification();
	}
}
