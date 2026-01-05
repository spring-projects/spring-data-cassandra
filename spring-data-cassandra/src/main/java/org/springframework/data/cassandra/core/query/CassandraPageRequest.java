/*
 * Copyright 2017-present the original author or authors.
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
import java.util.Objects;

import org.jspecify.annotations.Nullable;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.lang.CheckReturnValue;
import org.springframework.lang.Contract;
import org.springframework.util.Assert;

/**
 * Cassandra-specific {@link PageRequest} implementation providing access to {@link ByteBuffer paging state}. This class
 * allows creation of the first page request and represents through Cassandra paging is based on the progress of fetched
 * pages and allows forward-only navigation. Accessing a particular page requires fetching of all pages until the
 * desired page is reached.
 * <p>
 * The fetching progress is represented as {@link ByteBuffer paging state}. Query results are associated with a
 * {@link com.datastax.oss.driver.api.core.cql.ExecutionInfo#getPagingState paging state} that is used on the next query
 * as input parameter to continue page fetching.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class CassandraPageRequest extends PageRequest {

	private final CassandraScrollPosition scrollPosition;

	private final boolean nextAllowed;

	private CassandraPageRequest(int page, int size, Sort sort, CassandraScrollPosition scrollPosition,
			boolean nextAllowed) {

		super(page, size, sort);

		Assert.notNull(scrollPosition, "ScrollPosition must not be null");

		this.scrollPosition = scrollPosition;
		this.nextAllowed = nextAllowed;
	}

	/**
	 * Creates a new unsorted {@link PageRequest}.
	 *
	 * @param page zero-based page index.
	 * @param size the size of the page to be returned.
	 * @throws IllegalArgumentException for page requests other than the first page.
	 */
	public static CassandraPageRequest of(int page, int size) {

		Assert.isTrue(page == 0,
				"Cannot create a Cassandra page request for an indexed page other than the first page (0)");

		return of(page, size, Sort.unsorted());
	}

	/**
	 * Creates a new {@link PageRequest} with sort parameters applied.
	 *
	 * @param page zero-based page index.
	 * @param size the size of the page to be returned.
	 * @param sort must not be {@literal null}.
	 * @throws IllegalArgumentException for page requests other than the first page.
	 */
	public static CassandraPageRequest of(int page, int size, Sort sort) {

		Assert.isTrue(page == 0,
				"Cannot create a Cassandra page request for an indexed page other than the first page (0)");

		return new CassandraPageRequest(page, size, sort, CassandraScrollPosition.initial(), false);
	}

	/**
	 * Creates a new {@link PageRequest} with sort direction and properties applied.
	 *
	 * @param page zero-based page index.
	 * @param size the size of the page to be returned.
	 * @param direction must not be {@literal null}.
	 * @param properties must not be {@literal null}.
	 * @throws IllegalArgumentException for page requests other than the first page.
	 */
	public static CassandraPageRequest of(int page, int size, Direction direction, String... properties) {

		Assert.isTrue(page == 0,
				"Cannot create a Cassandra page request for an indexed page other than the first page (0)");

		return of(page, size, Sort.by(direction, properties));
	}

	/**
	 * Creates a {@link PageRequest} with sort direction and properties applied.
	 *
	 * @param current the current {@link Pageable}, must not be {@literal null}.
	 * @param pagingState the paging state associated with the current {@link Pageable}. Can be {@literal null} if there
	 *          is no paging state associated.
	 */
	public static CassandraPageRequest of(Pageable current, @Nullable ByteBuffer pagingState) {
		return new CassandraPageRequest(current.getPageNumber(), current.getPageSize(), current.getSort(),
				pagingState != null ? CassandraScrollPosition.of(pagingState) : CassandraScrollPosition.initial(),
				pagingState != null);
	}

	/**
	 * Creates a new unsorted {@link PageRequest} for the first page.
	 *
	 * @param size the size of the page to be returned.
	 */
	public static CassandraPageRequest first(int size) {
		return of(0, size, Sort.unsorted());
	}

	/**
	 * Creates a new {@link PageRequest} with sort parameters applied for the first page.
	 *
	 * @param size the size of the page to be returned.
	 * @param sort must not be {@literal null}.
	 */
	public static CassandraPageRequest first(int size, Sort sort) {
		return new CassandraPageRequest(0, size, sort, CassandraScrollPosition.initial(), false);
	}

	/**
	 * Creates a new {@link PageRequest} with sort direction and properties applied for the first page.
	 *
	 * @param size the size of the page to be returned.
	 * @param direction must not be {@literal null}.
	 * @param properties must not be {@literal null}.
	 */
	public static CassandraPageRequest first(int size, Direction direction, String... properties) {
		return first(size, Sort.by(direction, properties));
	}

	/**
	 * Validate the {@link Pageable} whether it can be used for querying. Valid pageables are either:
	 * <ul>
	 * <li>Unpaged</li>
	 * <li>Request the first page</li>
	 * <li>{@link CassandraPageRequest} with a {@link ByteBuffer paging state}</li>
	 * </ul>
	 *
	 * @param pageable
	 * @throws IllegalArgumentException if the {@link Pageable} is not valid.
	 */
	public static void validatePageable(Pageable pageable) {

		if (pageable.isUnpaged() || pageable.getPageNumber() == 0) {
			return;
		}

		if (pageable instanceof CassandraPageRequest) {

			CassandraPageRequest pageRequest = (CassandraPageRequest) pageable;

			if (pageRequest.getPagingState() != null) {
				return;
			}
		}

		throw new IllegalArgumentException(
				"Paging queries for pages other than the first one require a CassandraPageRequest with a valid paging state");
	}

	/**
	 * @return the {@link ByteBuffer paging state} for the current {@link CassandraPageRequest} or {@literal null} if the
	 *         current {@link Pageable} represents the last page.
	 */
	@Nullable
	public ByteBuffer getPagingState() {

		if (this.scrollPosition.isInitial()) {
			return null;
		}

		return this.scrollPosition.getPagingState();
	}

	/**
	 * Returns whether there's a next {@link Pageable} we can access from the current one. Will return {@literal false} in
	 * case the current {@link Pageable} already refers to the next page.
	 *
	 * @return {@literal true } if there's a next {@link Pageable} we can access from the current one.
	 */
	public boolean hasNext() {
		return (getPagingState() != null && this.nextAllowed);
	}

	@Override
	public CassandraPageRequest next() {

		Assert.state(hasNext(), "Cannot create a next page request without a PagingState");

		return new CassandraPageRequest(getPageNumber() + 1, getPageSize(), getSort(), this.scrollPosition, false);
	}

	/**
	 * Create a new {@link CassandraPageRequest} associated with {@link Sort} sort order.
	 *
	 * @param sort must not be {@literal null}.
	 * @return a new {@link CassandraPageRequest} associated with the given {@link Sort}.
	 * @since 2.1.13
	 */
	@Contract("_ -> new")
	@CheckReturnValue
	public CassandraPageRequest withSort(Sort sort) {

		Assert.notNull(sort, "Sort must not be null");

		return new CassandraPageRequest(this.getPageNumber(), this.getPageSize(), sort, this.scrollPosition,
				this.nextAllowed);
	}

	@Override
	public PageRequest previous() {

		Assert.state(getPageNumber() < 2, "Cannot navigate to an intermediate page");

		return super.previous();
	}

	/**
	 * Returns the underlying {@link CassandraScrollPosition}.
	 *
	 * @return the underlying {@link CassandraScrollPosition}.
	 * @since 4.2
	 */
	public CassandraScrollPosition getScrollPosition() {
		return this.scrollPosition;
	}

	@Override
	public boolean equals(@Nullable Object obj) {

		if (this == obj) {
			return true;
		}
		if (!(obj instanceof CassandraPageRequest)) {
			return false;
		}
		if (!super.equals(obj)) {
			return false;
		}

		CassandraPageRequest that = (CassandraPageRequest) obj;

		if (nextAllowed != that.nextAllowed) {
			return false;
		}

		return Objects.equals(this.scrollPosition, that.scrollPosition);
	}

	@Override
	public int hashCode() {

		int result = super.hashCode();

		result = 31 * result + (scrollPosition != null ? scrollPosition.hashCode() : 0);
		result = 31 * result + (nextAllowed ? 1 : 0);

		return result;
	}

	@Override
	public String toString() {
		return String.format("Cassandra page request [number: %d, size %d, sort: %s, paging state: %s]", getPageNumber(),
				getPageSize(), getSort(), getPagingState());
	}
}
