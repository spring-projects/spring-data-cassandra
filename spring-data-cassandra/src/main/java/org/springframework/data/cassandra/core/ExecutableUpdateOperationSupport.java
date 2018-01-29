/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.cassandra.core;

import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;

import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Implementation of {@link ExecutableUpdateOperation}.
 *
 * @author Mark Paluch
 * @since 2.1
 */
@RequiredArgsConstructor
class ExecutableUpdateOperationSupport implements ExecutableUpdateOperation {

	private final @NonNull CassandraTemplate template;

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ExecutableUpdateOperation#update(java.lang.Class)
	 */
	@Override
	public <T> ExecutableUpdate<T> update(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");

		return new ExecutableUpdateSupport<>(template, domainType, Query.empty(), null, null);
	}

	@RequiredArgsConstructor
	@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
	static class ExecutableUpdateSupport<T> implements ExecutableUpdate<T>, UpdateWithTable<T>, UpdateWithQuery<T>,
			UpdateWithUpdate<T>, TerminatingUpdate<T> {

		@NonNull CassandraTemplate template;

		@NonNull Class<T> domainType;

		@NonNull Query query;

		@Nullable Update update;

		@Nullable CqlIdentifier tableName;

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableUpdateOperation.UpdateWithUpdate#apply(org.springframework.data.cassandra.core.query.Update)
		 */
		@Override
		public TerminatingUpdate<T> apply(Update update) {

			Assert.notNull(update, "Update must not be null!");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, tableName);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableUpdateOperation.UpdateWithTable#inTable(java.lang.String)
		 */
		@Override
		public UpdateWithQuery<T> inTable(String tableName) {

			Assert.hasText(tableName, "Table name must not be null or empty!");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, CqlIdentifier.of(tableName));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableUpdateOperation.UpdateWithTable#inTable(org.springframework.data.cassandra.core.cql.CqlIdentifier)
		 */
		@Override
		public UpdateWithQuery<T> inTable(CqlIdentifier tableName) {

			Assert.notNull(tableName, "Table name must not be null!");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, tableName);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableUpdateOperation.UpdateWithQuery#matching(org.springframework.data.cassandra.core.query.Query)
		 */
		@Override
		public UpdateWithUpdate<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null!");

			return new ExecutableUpdateSupport<>(template, domainType, query, update, tableName);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableUpdateOperation.TerminatingUpdate#all()
		 */
		@Override
		public WriteResult all() {
			return template.doUpdate(query, update, domainType, getTableName());
		}

		private CqlIdentifier getTableName() {
			return tableName != null ? tableName : template.getTableName(domainType);
		}
	}
}
