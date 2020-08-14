/*
 * Copyright 2018-2020 the original author or authors.
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
package org.springframework.data.cassandra.core;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Implementation of {@link ExecutableSelectOperation}.
 *
 * @author Mark Paluch
 * @author Tomasz Lelek
 * @see org.springframework.data.cassandra.core.ExecutableSelectOperation
 * @see org.springframework.data.cassandra.core.query.Query
 * @since 2.1
 */
class ExecutableSelectOperationSupport implements ExecutableSelectOperation {

	private final CassandraTemplate template;

	public ExecutableSelectOperationSupport(CassandraTemplate template) {
		this.template = template;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ExecutableSelectOperation#query(java.lang.Class)
	 */
	@Override
	public <T> ExecutableSelect<T> query(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ExecutableSelectSupport<>(this.template, domainType, domainType, Query.empty(), null, null);
	}

	static class ExecutableSelectSupport<T> implements ExecutableSelect<T> {

		private final CassandraTemplate template;

		private final Class<?> domainType;

		private final Class<T> returnType;

		private final Query query;

		private final @Nullable CqlIdentifier keyspaceName;
		private final @Nullable CqlIdentifier tableName;

		public ExecutableSelectSupport(CassandraTemplate template, Class<?> domainType, Class<T> returnType, Query query,
				@Nullable CqlIdentifier keyspaceName, @Nullable CqlIdentifier tableName) {
			this.template = template;
			this.domainType = domainType;
			this.returnType = returnType;
			this.query = query;
			this.keyspaceName = keyspaceName;
			this.tableName = tableName;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableSelectOperation.SelectWithTable#inTable(org.springframework.data.cassandra.core.cql.CqlIdentifier)
		 */
		@Override
		public SelectWithProjection<T> inTable(CqlIdentifier tableName) {

			Assert.notNull(tableName, "Table name must not be null");

			return new ExecutableSelectSupport<>(this.template, this.domainType, this.returnType, this.query, null,
					tableName);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableSelectOperation.SelectWithTable#inTable(CqlIdentifier, CqlIdentifier)
		 */
		@Override
		public SelectWithProjection<T> inTable(@Nullable CqlIdentifier keyspaceName, CqlIdentifier tableName) {

			Assert.notNull(tableName, "Table name must not be null");

			return new ExecutableSelectSupport<>(this.template, this.domainType, this.returnType, this.query, keyspaceName,
					tableName);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableSelectOperation.SelectWithProjection#as(java.lang.Class)
		 */
		@Override
		public <R> SelectWithQuery<R> as(Class<R> returnType) {

			Assert.notNull(returnType, "ReturnType must not be null");

			return new ExecutableSelectSupport<>(this.template, this.domainType, returnType, this.query, this.keyspaceName,
					this.tableName);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableSelectOperation.SelectWithQuery#matching(org.springframework.data.cassandra.core.query.Query)
		 */
		@Override
		public TerminatingSelect<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null");

			return new ExecutableSelectSupport<>(this.template, this.domainType, this.returnType, query, this.keyspaceName,
					this.tableName);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableSelectOperation.TerminatingSelect#count()
		 */
		@Override
		public long count() {
			return this.template.doCount(this.query, this.domainType, getTableCoordinates());
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableSelectOperation.TerminatingSelect#exists()
		 */
		@Override
		public boolean exists() {
			return this.template.doExists(this.query, this.domainType, getTableCoordinates());
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableSelectOperation.TerminatingSelect#firstValue()
		 */
		@Override
		public T firstValue() {

			List<T> result = this.template.doSelect(this.query.limit(1), this.domainType, getTableCoordinates(),
					this.returnType);

			return ObjectUtils.isEmpty(result) ? null : result.iterator().next();
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableSelectOperation.TerminatingSelect#oneValue()
		 */
		@Override
		public T oneValue() {

			List<T> result = this.template.doSelect(this.query.limit(2), this.domainType, getTableCoordinates(),
					this.returnType);

			if (ObjectUtils.isEmpty(result)) {
				return null;
			}

			if (result.size() > 1) {
				throw new IncorrectResultSizeDataAccessException(
						String.format("Query [%s] returned non unique result.", this.query), 1);
			}

			return result.iterator().next();
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableSelectOperation.TerminatingSelect#all()
		 */
		@Override
		public List<T> all() {
			return this.template.doSelect(this.query, this.domainType, getTableCoordinates(), this.returnType);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableSelectOperation.TerminatingSelect#stream()
		 */
		@Override
		public Stream<T> stream() {
			return this.template.doStream(this.query, this.domainType, getTableCoordinates(), this.returnType);
		}

		private CqlIdentifier getTableName() {
			return this.tableName != null ? this.tableName : this.template.getTableName(this.domainType);
		}

		private Optional<CqlIdentifier> getKeyspaceName() {
			return this.keyspaceName != null ? Optional.of(this.keyspaceName) : this.template.getKeyspaceName(this.domainType);
		}


		private TableCoordinates getTableCoordinates(){
			return TableCoordinates.of(getKeyspaceName(), getTableName());
		}

	}
}
