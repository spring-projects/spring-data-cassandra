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
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Implementation of {@link ExecutableDeleteOperation}.
 *
 * @author Mark Paluch
 * @since 2.1
 */
@RequiredArgsConstructor
class ExecutableDeleteOperationSupport implements ExecutableDeleteOperation {

	private final @NonNull CassandraTemplate template;

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.ExecutableDeleteOperation#remove(java.lang.Class)
	 */
	@Override
	public ExecutableDelete delete(Class<?> domainType) {

		Assert.notNull(domainType, "DomainType must not be null!");

		return new ExecutableDeleteSupport(template, domainType, Query.empty(), null);
	}

	@RequiredArgsConstructor
	@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
	static class ExecutableDeleteSupport implements ExecutableDelete, DeleteWithTable, TerminatingDelete {

		@NonNull CassandraTemplate template;

		@NonNull Class<?> domainType;

		@NonNull Query query;

		@Nullable CqlIdentifier tableName;

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableDeleteOperation.DeleteWithTable#inTable(java.lang.String)
		 */
		@Override
		public DeleteWithQuery inTable(String tableName) {

			Assert.hasText(tableName, "Table name must not be null or empty");

			return new ExecutableDeleteSupport(template, domainType, query, CqlIdentifier.of(tableName));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableDeleteOperation.DeleteWithTable#inTable(org.springframework.data.cassandra.core.cql.CqlIdentifier)
		 */
		@Override
		public DeleteWithQuery inTable(CqlIdentifier tableName) {

			Assert.notNull(tableName, "Table name must not be null");

			return new ExecutableDeleteSupport(template, domainType, query, tableName);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableDeleteOperation.DeleteWithQuery#matching(org.springframework.data.cassandra.core.query.Query)
		 */
		@Override
		public TerminatingDelete matching(Query query) {

			Assert.notNull(query, "Query must not be null!");

			return new ExecutableDeleteSupport(template, domainType, query, tableName);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.ExecutableDeleteOperation.TerminatingDelete#all()
		 */
		public WriteResult all() {
			return template.doDelete(query, domainType, getTableName());
		}

		private CqlIdentifier getTableName() {
			return tableName != null ? tableName : template.getTableName(domainType);
		}
	}
}
