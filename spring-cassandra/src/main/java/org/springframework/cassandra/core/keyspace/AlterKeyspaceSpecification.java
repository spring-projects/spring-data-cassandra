package org.springframework.cassandra.core.keyspace;

public class AlterKeyspaceSpecification extends KeyspaceOptionsSpecification<AlterKeyspaceSpecification> {

	/**
	 * Entry point into the {@link CreateKeyspaceSpecification}'s fluent API to create a keyspace. Convenient if imported
	 * statically.
	 */
	public static AlterKeyspaceSpecification alterKeyspace() {
		return new AlterKeyspaceSpecification();
	}
}
