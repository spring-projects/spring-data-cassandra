/*
 * Copyright 2018-present the original author or authors.
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

import reactor.core.publisher.Mono;

import org.jspecify.annotations.Nullable;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Implementation of {@link ReactiveDeleteOperation}.
 *
 * @author Mark Paluch
 * @see org.springframework.data.cassandra.core.ReactiveDeleteOperation
 * @see org.springframework.data.cassandra.core.query.Query
 * @since 2.1
 */
class ReactiveDeleteOperationSupport implements ReactiveDeleteOperation {

	private final ReactiveCassandraTemplate template;

	public ReactiveDeleteOperationSupport(ReactiveCassandraTemplate template) {
		this.template = template;
	}

	@Override
	public ReactiveDelete delete(Class<?> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ReactiveDeleteSupport(this.template, domainType, Query.empty(), null);
	}

	static class ReactiveDeleteSupport implements ReactiveDelete, TerminatingDelete {

		private final ReactiveCassandraTemplate template;

		private final Class<?> domainType;

		private final Query query;

		private final @Nullable CqlIdentifier tableName;

		public ReactiveDeleteSupport(ReactiveCassandraTemplate template, Class<?> domainType, Query query,
				@Nullable CqlIdentifier tableName) {
			this.template = template;
			this.domainType = domainType;
			this.query = query;
			this.tableName = tableName;
		}

		@Override
		public DeleteWithQuery inTable(CqlIdentifier tableName) {

			Assert.notNull(tableName, "Table name must not be null");

			return new ReactiveDeleteSupport(this.template, this.domainType, this.query, tableName);
		}

		@Override
		public TerminatingDelete matching(Query query) {

			Assert.notNull(query, "Query must not be null");

			return new ReactiveDeleteSupport(this.template, this.domainType, query, this.tableName);
		}

		public Mono<WriteResult> all() {
			return this.template.doDelete(this.query, this.domainType, getTableName());
		}

		private CqlIdentifier getTableName() {
			return this.tableName != null ? this.tableName : this.template.getTableName(this.domainType);
		}

	}

}
