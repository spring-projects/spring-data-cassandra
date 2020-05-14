/*
 * Copyright 2017-2020 the original author or authors.
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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

/**
 * Base class for Cassandra {@link RepositoryQuery} implementations providing common infrastructure such as
 * {@link EntityInstantiators} and {@link QueryStatementCreator}.
 *
 * @author Mark Paluch
 * @see org.springframework.data.repository.query.RepositoryQuery
 * @since 2.0
 */
public abstract class CassandraRepositoryQuerySupport implements RepositoryQuery {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	private final CassandraQueryMethod queryMethod;

	private final EntityInstantiators instantiators;

	private final QueryStatementCreator queryStatementCreator;

	/**
	 * Create a new {@link AbstractCassandraQuery} from the given {@link CassandraQueryMethod} and
	 * {@link CassandraOperations}.
	 *
	 * @param queryMethod must not be {@literal null}.
	 * @deprecated use {@link #CassandraRepositoryQuerySupport(CassandraQueryMethod, MappingContext)}
	 */
	@Deprecated
	public CassandraRepositoryQuerySupport(CassandraQueryMethod queryMethod) {
		this(queryMethod, new CassandraMappingContext());
	}

	/**
	 * Create a new {@link AbstractCassandraQuery} from the given {@link CassandraQueryMethod} and
	 * {@link CassandraOperations}.
	 *
	 * @param queryMethod must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 * @since 2.1
	 */
	public CassandraRepositoryQuerySupport(CassandraQueryMethod queryMethod,
			MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext) {

		Assert.notNull(queryMethod, "CassandraQueryMethod must not be null");
		Assert.notNull(mappingContext, "CassandraMappingContext must not be null");

		this.queryMethod = queryMethod;
		this.instantiators = new EntityInstantiators();
		this.queryStatementCreator = new QueryStatementCreator(queryMethod, mappingContext);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#getQueryMethod()
	 */
	@Override
	public CassandraQueryMethod getQueryMethod() {
		return this.queryMethod;
	}

	protected EntityInstantiators getEntityInstantiators() {
		return this.instantiators;
	}

	protected QueryStatementCreator getQueryStatementCreator() {
		return this.queryStatementCreator;
	}

	class CassandraReturnedType {

		private final ReturnedType returnedType;
		private final CustomConversions customConversions;

		CassandraReturnedType(ReturnedType returnedType, CustomConversions customConversions) {
			this.returnedType = returnedType;
			this.customConversions = customConversions;
		}

		boolean isProjecting() {

			if (!this.returnedType.isProjecting()) {
				return false;
			}

			// Spring Data Cassandra allows List<Map<String, Object> and Map<String, Object> declarations
			// on query methods so we don't want to let projection kick in
			if (ClassUtils.isAssignable(Map.class, this.returnedType.getReturnedType())) {
				return false;
			}

			// Type conversion using registered conversions is handled on template level
			if (this.customConversions.hasCustomWriteTarget(this.returnedType.getReturnedType())) {
				return false;
			}

			// Don't apply projection on Cassandra simple types
			return !this.customConversions.isSimpleType(this.returnedType.getReturnedType());
		}

		Class<?> getDomainType() {
			return this.returnedType.getDomainType();
		}

		Class<?> getReturnedType() {
			return this.returnedType.getReturnedType();
		}
	}
}
