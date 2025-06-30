/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.cassandra.repository.aot;

import java.util.List;

import org.springframework.data.cassandra.repository.query.ParameterBinding;

/**
 * An AOT query represented by a string.
 *
 * @author Mark Paluch
 * @since 5.0
 */
abstract class StringAotQuery extends AotQuery {

	private StringAotQuery(List<ParameterBinding> parameterBindings) {
		super(parameterBindings);
	}

	/**
	 * Creates a new {@link StringAotQuery}.
	 *
	 * @param query
	 * @param parameterBindings
	 * @param count whether to apply count projection.
	 * @param exists whether to apply exists projection.
	 * @return
	 */
	static StringAotQuery of(String query, List<ParameterBinding> parameterBindings, boolean count, boolean exists) {
		return new DeclaredAotQuery(query, parameterBindings, count, exists);
	}

	/**
	 * Creates a new {@link NamedStringAotQuery named string query}.
	 *
	 * @param queryName
	 * @param query
	 * @param parameterBindings
	 * @param count whether to apply count projection.
	 * @param exists whether to apply exists projection.
	 * @return
	 */
	static StringAotQuery named(String queryName, String query, List<ParameterBinding> parameterBindings, boolean count,
			boolean exists) {
		return new NamedStringAotQuery(queryName, query, parameterBindings, count, exists);
	}

	/**
	 * @return the query string used to execute the query.
	 */
	public abstract String getQueryString();

	@Override
	public String toString() {
		return getQueryString();
	}

	/**
	 * Declared Cassandra query.
	 */
	static class DeclaredAotQuery extends StringAotQuery {

		private final String query;
		private final boolean count;
		private final boolean exists;

		DeclaredAotQuery(String query, List<ParameterBinding> parameterBindings, boolean count, boolean exists) {

			super(parameterBindings);

			this.query = query;
			this.count = count;
			this.exists = exists;
		}

		@Override
		public boolean isCount() {
			return count;
		}

		@Override
		public boolean isExists() {
			return exists;
		}

		@Override
		public String getQueryString() {
			return query;
		}

	}

	/**
	 * Named Cassandra query.
	 */
	static class NamedStringAotQuery extends DeclaredAotQuery {

		private final String queryName;

		NamedStringAotQuery(String queryName, String query, List<ParameterBinding> parameterBindings, boolean count,
				boolean exists) {

			super(query, parameterBindings, count, exists);

			this.queryName = queryName;
		}

		public String getQueryName() {
			return queryName;
		}

	}

}
