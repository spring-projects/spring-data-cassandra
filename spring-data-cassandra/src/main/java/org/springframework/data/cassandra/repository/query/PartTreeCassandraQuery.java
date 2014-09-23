/*
 * Copyright 2002-2013 the original author or authors.
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

import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * {@link RepositoryQuery} implementation for Cassandra.
 */
public class PartTreeCassandraQuery extends AbstractCassandraQuery {

	private final PartTree tree;
	private final CassandraMappingContext context;

	/**
	 * Creates a new {@link PartTreeCassandraQuery} from the given {@link QueryMethod} and {@link CassandraTemplate}.
	 * 
	 * @param method must not be {@literal null}.
	 * @param template must not be {@literal null}.
	 */
	public PartTreeCassandraQuery(CassandraQueryMethod method, CassandraOperations cassandraOperations) {

		super(method, cassandraOperations);
		this.tree = new PartTree(method.getName(), method.getEntityInformation().getJavaType());
		this.context = cassandraOperations.getConverter().getMappingContext();
	}

	/**
	 * Return the {@link PartTree} backing the query.
	 * 
	 * @return the tree
	 */
	public PartTree getTree() {
		return tree;
	}

	@Override
	protected String createQuery(CassandraParameterAccessor accessor) {

		CassandraQueryCreator creator = new CassandraQueryCreator(tree, accessor, context);
		return creator.createQuery().getQueryString();
	}
}
