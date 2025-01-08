/*
 * Copyright 2014-2025 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository.query;

import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.StatementFactory;
import org.springframework.data.cassandra.core.convert.UpdateMapper;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * {@link RepositoryQuery} implementation for Cassandra.
 *
 * @author Matthew Adams
 * @author Mark Paluch
 * @see org.springframework.data.cassandra.repository.query.AbstractCassandraQuery
 */
public class PartTreeCassandraQuery extends AbstractCassandraQuery {

	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	private final PartTree tree;

	private final StatementFactory statementFactory;

	/**
	 * Create a new {@link PartTreeCassandraQuery} from the given {@link QueryMethod} and {@link CassandraTemplate}.
	 *
	 * @param queryMethod must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public PartTreeCassandraQuery(CassandraQueryMethod queryMethod, CassandraOperations operations) {

		super(queryMethod, operations);

		this.tree = new PartTree(queryMethod.getName(), queryMethod.getResultProcessor().getReturnedType().getDomainType());
		this.mappingContext = operations.getConverter().getMappingContext();
		this.statementFactory = operations instanceof CassandraTemplate ct ? ct.getStatementFactory()
				: new StatementFactory(new UpdateMapper(operations.getConverter()));
	}

	/**
	 * Returns the {@link MappingContext} used by this query to access mapping meta-data used to store (map) objects to
	 * Cassandra tables.
	 *
	 * @return the {@link MappingContext} used by this query.
	 * @see CassandraMappingContext
	 */
	protected MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> getMappingContext() {
		return this.mappingContext;
	}

	/**
	 * Returns the {@link StatementFactory} used by this query to construct and run Cassandra CQL statements.
	 *
	 * @return the {@link StatementFactory} used by this query to construct and run Cassandra CQL statements.
	 * @see org.springframework.data.cassandra.core.StatementFactory
	 */
	protected StatementFactory getStatementFactory() {
		return this.statementFactory;
	}

	/**
	 * Return the {@link PartTree} backing the query.
	 *
	 * @return the tree
	 */
	protected PartTree getTree() {
		return this.tree;
	}

	@Override
	protected SimpleStatement createQuery(CassandraParameterAccessor parameterAccessor) {

		if (isCountQuery()) {
			return getQueryStatementCreator().count(getStatementFactory(), getTree(), parameterAccessor);
		}

		if (isExistsQuery()) {
			return getQueryStatementCreator().exists(getStatementFactory(), getTree(), parameterAccessor);
		}

		if (getTree().isDelete()) {
			return getQueryStatementCreator().delete(getStatementFactory(), getTree(), parameterAccessor);
		}

		return getQueryStatementCreator().select(getStatementFactory(), getTree(), parameterAccessor,
				getQueryMethod().getResultProcessor());
	}

	@Override
	protected boolean isCountQuery() {
		return getTree().isCountProjection();
	}

	@Override
	protected boolean isExistsQuery() {
		return getTree().isExistsProjection();
	}

	@Override
	protected boolean isLimiting() {
		return getTree().isLimiting();
	}

	@Override
	protected boolean isModifyingQuery() {
		return getTree().isDelete();
	}
}
