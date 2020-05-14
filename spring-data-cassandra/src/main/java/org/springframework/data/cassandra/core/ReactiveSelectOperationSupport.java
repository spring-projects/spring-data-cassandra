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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Implementation of {@link ReactiveSelectOperation}.
 *
 * @author Mark Paluch
 * @see org.springframework.data.cassandra.core.ReactiveSelectOperation
 * @see org.springframework.data.cassandra.core.query.Query
 * @since 2.1
 */
class ReactiveSelectOperationSupport implements ReactiveSelectOperation {

	private final ReactiveCassandraTemplate template;

	public ReactiveSelectOperationSupport(ReactiveCassandraTemplate template) {
		this.template = template;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveSelectOperation#query(java.lang.Class)
	 */
	@Override
	public <T> ReactiveSelect<T> query(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ReactiveSelectSupport<>(this.template, domainType, domainType, Query.empty(), null);
	}

	static class ReactiveSelectSupport<T> implements ReactiveSelect<T> {

		private final ReactiveCassandraTemplate template;

		private final Class<?> domainType;

		private final Class<T> returnType;

		private final Query query;

		private final @Nullable CqlIdentifier tableName;

		public ReactiveSelectSupport(ReactiveCassandraTemplate template, Class<?> domainType, Class<T> returnType,
				Query query, CqlIdentifier tableName) {
			this.template = template;
			this.domainType = domainType;
			this.returnType = returnType;
			this.query = query;
			this.tableName = tableName;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ReactiveSelectOperation.SelectWithTable#inTable(org.springframework.data.cassandra.core.cql.CqlIdentifier)
		 */
		@Override
		public SelectWithProjection<T> inTable(CqlIdentifier tableName) {

			Assert.notNull(tableName, "Table name must not be null");

			return new ReactiveSelectSupport<>(this.template, this.domainType, this.returnType, this.query, tableName);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ReactiveSelectOperation.SelectWithProjection#as(java.lang.Class)
		 */
		@Override
		public <R> SelectWithQuery<R> as(Class<R> returnType) {

			Assert.notNull(returnType, "ReturnType must not be null");

			return new ReactiveSelectSupport<>(this.template, this.domainType, returnType, this.query, this.tableName);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ReactiveSelectOperation.SelectWithQuery#matching(org.springframework.data.cassandra.core.query.Query)
		 */
		@Override
		public TerminatingSelect<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null");

			return new ReactiveSelectSupport<>(this.template, this.domainType, this.returnType, query, this.tableName);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ReactiveSelectOperation.TerminatingSelect#count()
		 */
		@Override
		public Mono<Long> count() {
			return this.template.doCount(this.query, this.domainType, getTableName());
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ReactiveSelectOperation.TerminatingSelect#exists()
		 */
		@Override
		public Mono<Boolean> exists() {
			return this.template.doExists(this.query, this.domainType, getTableName());
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ReactiveSelectOperation.TerminatingSelect#first()
		 */
		@Override
		public Mono<T> first() {
			return this.template.doSelect(this.query.limit(1), this.domainType, getTableName(), this.returnType).next();
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ReactiveSelectOperation.TerminatingSelect#one()
		 */
		@Override
		public Mono<T> one() {

			Flux<T> result = this.template.doSelect(this.query.limit(2), this.domainType, getTableName(), this.returnType);

			return result.collectList() //
					.flatMap(it -> {

						if (it.isEmpty()) {
							return Mono.empty();
						}

						if (it.size() > 1) {
							return Mono.error(new IncorrectResultSizeDataAccessException(
									String.format("Query [%s] returned non unique result.", this.query), 1));
						}

						return Mono.just(it.get(0));
					});
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ReactiveSelectOperation.TerminatingSelect#all()
		 */
		@Override
		public Flux<T> all() {
			return this.template.doSelect(this.query, this.domainType, getTableName(), this.returnType);
		}

		private CqlIdentifier getTableName() {
			return this.tableName != null ? this.tableName : this.template.getTableName(this.domainType);
		}
	}
}
