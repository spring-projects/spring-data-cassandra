/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository.query;

import org.springframework.data.cassandra.core.ReactiveCassandraOperations;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * Reactive PartTree {@link RepositoryQuery} implementation for Cassandra.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class ReactivePartTreeCassandraQuery extends AbstractReactiveCassandraQuery {

	private final CassandraMappingContext mappingContext;

	private final PartTree tree;

	/**
	 * Creates a new {@link ReactivePartTreeCassandraQuery} from the given {@link QueryMethod} and
	 * {@link ReactiveCassandraOperations}.
	 *
	 * @param queryMethod must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public ReactivePartTreeCassandraQuery(CassandraQueryMethod queryMethod, ReactiveCassandraOperations operations) {

		super(queryMethod, operations);

		this.tree = new PartTree(queryMethod.getName(), queryMethod.getEntityInformation().getJavaType());
		this.mappingContext = operations.getConverter().getMappingContext();
	}

	/**
	 * Return the {@link PartTree} backing the query.
	 *
	 * @return the tree
	 */
	public PartTree getTree() {
		return tree;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.AbstractCassandraQuery#createQuery(org.springframework.data.cassandra.repository.query.CassandraParameterAccessor, boolean)
	 */
	@Override
	protected String createQuery(CassandraParameterAccessor parameterAccessor) {

		CassandraQueryCreator queryCreator = new CassandraQueryCreator(tree, parameterAccessor, mappingContext,
				getQueryMethod().getEntityInformation());

		return queryCreator.createQuery().toString();
	}
}
