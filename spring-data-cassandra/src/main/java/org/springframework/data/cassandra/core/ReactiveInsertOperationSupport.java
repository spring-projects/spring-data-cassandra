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

import reactor.core.publisher.Mono;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Implementation of {@link ReactiveInsertOperation}.
 *
 * @author Mark Paluch
 * @since 2.1
 */
class ReactiveInsertOperationSupport implements ReactiveInsertOperation {

	private final ReactiveCassandraTemplate template;

	public ReactiveInsertOperationSupport(ReactiveCassandraTemplate template) {
		this.template = template;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ReactiveInsertOperation#insert(java.lang.Class)
	 */
	@Override
	public <T> ReactiveInsert<T> insert(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ReactiveInsertSupport<>(this.template, domainType, InsertOptions.empty(), null);
	}

	static class ReactiveInsertSupport<T> implements ReactiveInsert<T> {

		private final ReactiveCassandraTemplate template;

		private final Class<T> domainType;

		private final InsertOptions insertOptions;

		private final @Nullable CqlIdentifier tableName;

		public ReactiveInsertSupport(ReactiveCassandraTemplate template, Class<T> domainType, InsertOptions insertOptions,
				CqlIdentifier tableName) {
			this.template = template;
			this.domainType = domainType;
			this.insertOptions = insertOptions;
			this.tableName = tableName;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ReactiveInsertOperation.InsertWithTable#inTable(org.springframework.data.cassandra.core.cql.CqlIdentifier)
		 */
		@Override
		public InsertWithOptions<T> inTable(CqlIdentifier tableName) {

			Assert.notNull(tableName, "Table name must not be null");

			return new ReactiveInsertSupport<>(this.template, this.domainType, this.insertOptions, tableName);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ReactiveInsertOperation.InsertWithOptions#withOptions(org.springframework.data.cassandra.core.InsertOptions)
		 */
		@Override
		public TerminatingInsert<T> withOptions(InsertOptions insertOptions) {

			Assert.notNull(insertOptions, "InsertOptions must not be null");

			return new ReactiveInsertSupport<>(this.template, this.domainType, insertOptions, this.tableName);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ReactiveInsertOperation.TerminatingInsert#one(java.lang.Object)
		 */
		@Override
		public Mono<EntityWriteResult<T>> one(T object) {

			Assert.notNull(object, "Object must not be null");

			return this.template.doInsert(object, this.insertOptions, getTableName());
		}

		private CqlIdentifier getTableName() {
			return this.tableName != null ? this.tableName : this.template.getTableName(this.domainType);
		}
	}
}
