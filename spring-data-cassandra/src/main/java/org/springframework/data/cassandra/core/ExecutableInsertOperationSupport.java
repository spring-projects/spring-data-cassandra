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
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Implementation of {@link ExecutableInsertOperation}.
 *
 * @author Mark Paluch
 * @since 2.1
 */
@RequiredArgsConstructor
class ExecutableInsertOperationSupport implements ExecutableInsertOperation {

	private final @NonNull CassandraTemplate template;

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ExecutableInsertOperation#insert(java.lang.Class)
	 */
	@Override
	public <T> ExecutableInsert<T> insert(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");

		return new ExecutableInsertSupport<>(template, domainType, null, InsertOptions.empty());
	}

	@RequiredArgsConstructor
	@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
	static class ExecutableInsertSupport<T> implements ExecutableInsert<T> {

		@NonNull CassandraTemplate template;

		@NonNull Class<T> domainType;

		@Nullable CqlIdentifier tableName;

		@NonNull InsertOptions insertOptions;

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableInsertOperation.InsertWithTable#inTable(java.lang.String)
		 */
		@Override
		public InsertWithOptions<T> inTable(String tableName) {

			Assert.hasText(tableName, "Table name must not be null or empty");

			return new ExecutableInsertSupport<>(template, domainType, CqlIdentifier.of(tableName), insertOptions);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableInsertOperation.InsertWithTable#inTable(org.springframework.data.cassandra.core.cql.CqlIdentifier)
		 */
		@Override
		public InsertWithOptions<T> inTable(CqlIdentifier tableName) {

			Assert.notNull(tableName, "Table name must not be null");

			return new ExecutableInsertSupport<>(template, domainType, tableName, insertOptions);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableInsertOperation.InsertWithOptions#withOptions(org.springframework.data.cassandra.core.InsertOptions)
		 */
		@Override
		public TerminatingInsert<T> withOptions(InsertOptions insertOptions) {

			Assert.notNull(insertOptions, "InsertOptions must not be null");

			return new ExecutableInsertSupport<>(template, domainType, tableName, insertOptions);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableInsertOperation.TerminatingInsert#one(java.lang.Object)
		 */
		@Override
		public WriteResult one(T object) {

			Assert.notNull(object, "Object must not be null!");

			return template.doInsert(object, insertOptions, getTableName());
		}

		private CqlIdentifier getTableName() {
			return tableName != null ? tableName : template.getTableName(domainType);
		}
	}
}
