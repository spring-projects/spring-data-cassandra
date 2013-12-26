package org.springframework.cassandra.test.unit.core.cql.generator;

import org.junit.Test;
import org.springframework.cassandra.core.cql.generator.KeyspaceNameCqlGenerator;
import org.springframework.cassandra.core.cql.generator.TableNameCqlGenerator;
import org.springframework.cassandra.core.keyspace.KeyspaceNameSpecification;
import org.springframework.cassandra.core.keyspace.TableNameSpecification;

/**
 * Useful test class that specifies just about as much as you can for a CQL generation test. Intended to be extended by
 * classes that contain methods annotated with {@link Test}. Everything is public because this is a test class with no
 * need for encapsulation, and it makes for easier reuse in other tests like integration tests (hint hint).
 * 
 * @author Matthew T. Adams
 * 
 * @param <S> The type of the {@link TableNameSpecification}
 * @param <G> The type of the {@link TableNameCqlGenerator}
 */
public abstract class KeyspaceOperationCqlGeneratorTest<S extends KeyspaceNameSpecification<?>, G extends KeyspaceNameCqlGenerator<?>> {

	public abstract S specification();

	public abstract G generator();

	public String keyspace;
	public S specification;
	public G generator;
	public String cql;

	public void prepare() {
		this.specification = specification();
		this.generator = generator();
		this.cql = generateCql();
	}

	public String generateCql() {
		return generator.toCql();
	}
}