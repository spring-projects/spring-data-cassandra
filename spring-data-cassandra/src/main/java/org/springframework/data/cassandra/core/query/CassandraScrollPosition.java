/*
 * Copyright 2023-present the original author or authors.
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
package org.springframework.data.cassandra.core.query;

import java.nio.ByteBuffer;

import org.springframework.data.cassandra.core.query.CassandraScrollPosition.Initial;
import org.springframework.data.cassandra.core.query.CassandraScrollPosition.PagingState;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Cassandra-specific implementation of {@link ScrollPosition} using
 * {@link com.datastax.oss.driver.api.core.cql.PagingState}.
 *
 * @author Mark Paluch
 * @since 4.2
 */
public abstract sealed class CassandraScrollPosition implements ScrollPosition permits Initial,PagingState {

	/**
	 * Returns an initial {@link CassandraScrollPosition}.
	 *
	 * @return an initial {@link CassandraScrollPosition}.
	 */
	public static CassandraScrollPosition initial() {
		return Initial.INSTANCE;
	}

	/**
	 * Creates a continuation {@link CassandraScrollPosition} given {@code pagingState}.
	 *
	 * @return a continuation {@link CassandraScrollPosition} given {@code pagingState}.
	 */
	public static CassandraScrollPosition of(ByteBuffer pagingState) {

		Assert.notNull(pagingState, "PagingState must not be null");

		return new PagingState(pagingState);
	}

	/**
	 * Returns the underlying binary representation of the paging state as read-only buffer if the scroll position is not
	 * {@link #initial()}.
	 *
	 * @return the underlying binary representation of the paging state.
	 * @throws IllegalStateException if the scroll position {@link #initial()} (i.e. the scroll position isn't associated
	 *           with a continuation).
	 */
	public abstract ByteBuffer getPagingState();

	static final class Initial extends CassandraScrollPosition {

		private final static Initial INSTANCE = new Initial();

		@Override
		public boolean isInitial() {
			return true;
		}

		@Override
		public ByteBuffer getPagingState() {
			throw new IllegalStateException("Initial scroll position does not provide a PagingState");
		}
	}

	static final class PagingState extends CassandraScrollPosition {

		private final ByteBuffer pagingState;

		PagingState(ByteBuffer pagingState) {
			this.pagingState = pagingState;
		}

		@Override
		public boolean isInitial() {
			return false;
		}

		@Override
		public ByteBuffer getPagingState() {
			return pagingState.asReadOnlyBuffer();
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			PagingState that = (PagingState) o;

			return ObjectUtils.nullSafeEquals(pagingState, that.pagingState);
		}

		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHashCode(pagingState);
		}
	}
}
