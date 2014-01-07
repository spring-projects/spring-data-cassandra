package org.springframework.cassandra.test.unit.core.cql.generator;

import org.junit.Test;
import org.springframework.cassandra.core.cql.generator.IndexNameCqlGenerator;
import org.springframework.cassandra.core.keyspace.IndexNameSpecification;

/**
 * Useful test class that specifies just about as much as you can for a CQL generation test. Intended to be extended by
 * classes that contain methods annotated with {@link Test}. Everything is public because this is a test class with no
 * need for encapsulation, and it makes for easier reuse in other tests like integration tests (hint hint).
 * 
 * @author Matthew T. Adams
 * @author David Webb
 * 
 * @param <S> The type of the {@link IndexNameSpecification}
 * @param <G> The type of the {@link IndexNameCqlGenerator}
 */
public abstract class IndexOperationCqlGeneratorTest<S extends IndexNameSpecification<?>, G extends IndexNameCqlGenerator<?>> {

	public abstract S specification();

	public abstract G generator();

	public String indexName;
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