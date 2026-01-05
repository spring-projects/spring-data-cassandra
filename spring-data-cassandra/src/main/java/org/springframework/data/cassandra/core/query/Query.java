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

import static org.springframework.util.ObjectUtils.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.jspecify.annotations.Nullable;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.lang.CheckReturnValue;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.cql.PagingState;

/**
 * Query object representing {@link CriteriaDefinition}s, {@link Columns}, {@link Sort}, {@link ByteBuffer paging state}
 * and {@link QueryOptions} for a CQL query. {@link Query} is created with a fluent API creating immutable objects.
 *
 * @author Mark Paluch
 * @see org.springframework.data.cassandra.core.query.Filter
 * @see org.springframework.data.domain.Sort
 * @since 2.0
 */
public class Query implements Filter {

	private static final Query EMPTY = new Query(Collections.emptyList(), Columns.empty(), Sort.unsorted(),
			CassandraScrollPosition.initial(), Optional.empty(), Limit.unlimited(), false);

	private final boolean allowFiltering;

	private final Columns columns;

	private final List<CriteriaDefinition> criteriaDefinitions;

	private final Limit limit;

	private final CassandraScrollPosition scrollPosition;

	private final Optional<QueryOptions> queryOptions;

	private final Sort sort;

	private Query(List<CriteriaDefinition> criteriaDefinitions, Columns columns, Sort sort,
			CassandraScrollPosition pagingState, Optional<QueryOptions> queryOptions, Limit limit, boolean allowFiltering) {

		Assert.notNull(limit, "Limit must not be null");

		this.criteriaDefinitions = criteriaDefinitions;
		this.columns = columns;
		this.sort = sort;
		this.scrollPosition = pagingState;
		this.queryOptions = queryOptions;
		this.limit = limit;
		this.allowFiltering = allowFiltering;
	}

	/**
	 * Static factory method to create an empty {@link Query}.
	 *
	 * @return a new, empty {@link Query}.
	 */
	public static Query empty() {
		return EMPTY;
	}

	/**
	 * Static factory method to create a {@link Query} for the given column selection.
	 *
	 * @return a new {@link Query} selecting {@link Columns}.
	 * @since 4.5
	 */
	public static Query select(Columns columns) {
		return EMPTY.columns(columns);
	}

	/**
	 * Static factory method to create a {@link Query} using the provided {@link CriteriaDefinition}.
	 *
	 * @param criteriaDefinitions must not be {@literal null}.
	 * @return the {@link Query} for {@link CriteriaDefinition}s.
	 */
	public static Query query(CriteriaDefinition... criteriaDefinitions) {

		Assert.notNull(criteriaDefinitions, "CriteriaDefinitions must not be null");

		return query(Arrays.asList(criteriaDefinitions));
	}

	/**
	 * Static factory method to create a {@link Query} using the provided {@link CriteriaDefinition}.
	 *
	 * @param criteriaDefinitions must not be {@literal null}.
	 * @return the {@link Query} for {@link CriteriaDefinition}s.
	 */
	public static Query query(Iterable<? extends CriteriaDefinition> criteriaDefinitions) {

		Assert.notNull(criteriaDefinitions, "CriteriaDefinitions must not be null");

		List<CriteriaDefinition> collect = StreamSupport.stream(criteriaDefinitions.spliterator(), false)
				.collect(Collectors.toList());

		return new Query(collect, Columns.empty(), Sort.unsorted(), CassandraScrollPosition.initial(), Optional.empty(),
				Limit.unlimited(), false);
	}

	/**
	 * Add the given {@link CriteriaDefinition} to the current {@link Query}.
	 *
	 * @param criteriaDefinition must not be {@literal null}.
	 * @return a new {@link Query} object containing the former settings with {@link CriteriaDefinition} applied.
	 */
	@CheckReturnValue
	public Query and(CriteriaDefinition criteriaDefinition) {

		Assert.notNull(criteriaDefinition, "Criteria must not be null");

		List<CriteriaDefinition> criteriaDefinitions = new ArrayList<>(this.criteriaDefinitions.size() + 1);

		criteriaDefinitions.addAll(this.criteriaDefinitions);

		if (!criteriaDefinitions.contains(criteriaDefinition)) {
			criteriaDefinitions.add(criteriaDefinition);
		}

		return new Query(criteriaDefinitions, this.columns, this.sort, this.scrollPosition, this.queryOptions, this.limit,
				this.allowFiltering);
	}

	@Override
	public Iterable<CriteriaDefinition> getCriteriaDefinitions() {
		return Collections.unmodifiableCollection(criteriaDefinitions);
	}

	/**
	 * Add {@link Columns} to the {@link Query} instance. Existing definitions are merged or overwritten for overriding
	 * {@link ColumnName}s in {@code columns}.
	 *
	 * @param columns must not be {@literal null}.
	 * @return a new {@link Query} object containing the former settings with {@link Columns} applied.
	 */
	@CheckReturnValue
	public Query columns(Columns columns) {

		Assert.notNull(columns, "Columns must not be null");

		if (columns.equals(this.columns)) {
			return this;
		}

		return new Query(this.criteriaDefinitions, this.columns.and(columns), this.sort, this.scrollPosition,
				this.queryOptions, this.limit, this.allowFiltering);
	}

	/**
	 * @return the query {@link Columns}.
	 */
	public Columns getColumns() {
		return this.columns;
	}

	/**
	 * Add a {@link Sort} to the {@link Query} instance.
	 *
	 * @param sort must not be {@literal null}.
	 * @return a new {@link Query} object containing the former settings with {@link Sort} applied.
	 */
	@CheckReturnValue
	public Query sort(Sort sort) {

		Assert.notNull(sort, "Sort must not be null");

		for (Order order : sort) {
			if (order.isIgnoreCase()) {
				throw new IllegalArgumentException(String.format("Given sort contained an Order for %s with ignore case;"
						+ " Apache Cassandra does not support sorting ignoring case currently", order.getProperty()));
			}
		}

		Sort sortToUse = this.sort;
		if (this.sort.isUnsorted()) {
			sortToUse = sort;
		} else {
			if (sortToUse instanceof VectorSort || sort instanceof VectorSort) {
				throw new InvalidDataAccessApiUsageException("Cannot concatenate multiple VectorSort instances");
			}
			sortToUse = this.sort.and(sort);
		}

		return new Query(this.criteriaDefinitions, this.columns, sortToUse, this.scrollPosition,
				this.queryOptions, this.limit, this.allowFiltering);
	}

	/**
	 * @return the query {@link Sort} object.
	 */
	public Sort getSort() {
		return this.sort;
	}

	/**
	 * Create a {@link Query} initialized with a {@link PageRequest} to fetch the first page of results or advance in
	 * paging along with sorting. Reads (and overrides, if set) {@link Pageable#getPageSize() page size} into
	 * {@code QueryOptions#getPageSize()} and sets {@code pagingState} and {@link Sort}.
	 *
	 * @param pageable must not be {@literal null}.
	 * @return a new {@link Query} object containing the former settings with {@link PageRequest} applied.
	 * @see CassandraPageRequest
	 */
	@CheckReturnValue
	public Query pageRequest(Pageable pageable) {

		Assert.notNull(pageable, "Pageable must not be null");

		CassandraPageRequest.validatePageable(pageable);

		if (pageable.isUnpaged()) {
			return this;
		}

		CassandraScrollPosition scrollPosition = getScrollPosition();

		if (pageable instanceof CassandraPageRequest cpr) {
			scrollPosition = cpr.getScrollPosition();
		}

		QueryOptions queryOptions = this.queryOptions.map(QueryOptions::mutate).orElse(QueryOptions.builder())
				.pageSize(pageable.getPageSize()).build();

		return new Query(this.criteriaDefinitions, this.columns, this.sort.and(pageable.getSort()), scrollPosition,
				Optional.of(queryOptions), this.limit, this.allowFiltering);
	}

	/**
	 * Set the {@code paging state} to skip rows.
	 *
	 * @param scrollPosition must not be {@literal null}.
	 * @return a new {@link Query} object containing the former settings with paging state applied.
	 */
	@CheckReturnValue
	public Query pagingState(CassandraScrollPosition scrollPosition) {

		Assert.notNull(scrollPosition, "CassandraScrollPosition must not be null");

		return new Query(this.criteriaDefinitions, this.columns, this.sort, scrollPosition, this.queryOptions, this.limit,
				this.allowFiltering);
	}

	/**
	 * Set the {@link PagingState paging state} to skip rows.
	 *
	 * @param pagingState must not be {@literal null}.
	 * @return a new {@link Query} object containing the former settings with {@link PagingState paging state} applied.
	 * @since 5.0
	 */
	@CheckReturnValue
	public Query pagingState(PagingState pagingState) {
		return pagingState(CassandraScrollPosition.of(pagingState));
	}

	/**
	 * Set the {@link ByteBuffer paging state} to skip rows.
	 *
	 * @param pagingState must not be {@literal null}.
	 * @return a new {@link Query} object containing the former settings with {@link ByteBuffer paging state} applied.
	 */
	@CheckReturnValue
	public Query pagingState(ByteBuffer pagingState) {

		Assert.notNull(pagingState, "PagingState must not be null");

		return new Query(this.criteriaDefinitions, this.columns, this.sort, CassandraScrollPosition.of(pagingState),
				this.queryOptions, this.limit, this.allowFiltering);
	}

	/**
	 * @return the optional {@link ByteBuffer paging state}.
	 */
	public Optional<ByteBuffer> getPagingState() {

		if (this.scrollPosition.isInitial()) {
			return Optional.empty();
		}

		return Optional.of(this.scrollPosition.getPagingState());
	}

	/**
	 * Set the {@link QueryOptions}.
	 *
	 * @param queryOptions must not be {@literal null}.
	 * @return a new {@link Query} object containing the former settings with {@link QueryOptions} applied.
	 */
	@CheckReturnValue
	public Query queryOptions(QueryOptions queryOptions) {

		Assert.notNull(queryOptions, "QueryOptions must not be null");

		return new Query(this.criteriaDefinitions, this.columns, this.sort, this.scrollPosition, Optional.of(queryOptions),
				this.limit, this.allowFiltering);
	}

	/**
	 * @return the optional {@link QueryOptions}.
	 */
	public Optional<QueryOptions> getQueryOptions() {
		return this.queryOptions;
	}

	/**
	 * Limit the number of returned rows to {@code limit}.
	 *
	 * @param limit
	 * @return a new {@link Query} object containing the former settings with {@code limit} applied.
	 */
	@CheckReturnValue
	public Query limit(long limit) {
		return new Query(this.criteriaDefinitions, this.columns, this.sort, this.scrollPosition, this.queryOptions,
				Limit.of(Math.toIntExact(limit)), this.allowFiltering);
	}

	/**
	 * Limit the number of returned rows to {@link Limit}.
	 *
	 * @param limit
	 * @return a new {@link Query} object containing the former settings with {@code limit} applied.
	 */
	@CheckReturnValue
	public Query limit(Limit limit) {
		return new Query(this.criteriaDefinitions, this.columns, this.sort, this.scrollPosition, this.queryOptions, limit,
				this.allowFiltering);
	}

	/**
	 * @return the maximum number of rows to be returned.
	 */
	public long getLimit() {
		return this.limit.isLimited() ? this.limit.max() : 0;
	}

	/**
	 * @return {@code true} if the query is limited.
	 */
	public boolean isLimited() {
		return this.limit.isLimited();
	}

	/**
	 * Allow filtering with {@code this} {@link Query}.
	 *
	 * @return a new {@link Query} object containing the former settings with {@code allowFiltering} applied.
	 */
	@CheckReturnValue
	public Query withAllowFiltering() {
		return new Query(this.criteriaDefinitions, this.columns, this.sort, this.scrollPosition, this.queryOptions,
				this.limit, true);
	}

	/**
	 * @return {@literal true} to allow filtering.
	 */
	public boolean isAllowFiltering() {
		return this.allowFiltering;
	}

	/**
	 * @return the paging state as {@link CassandraScrollPosition}.
	 * @since 4.2
	 */
	private CassandraScrollPosition getScrollPosition() {
		return scrollPosition;
	}

	@Override
	public boolean equals(@Nullable Object obj) {

		if (this == obj) {
			return true;
		}

		if (obj == null || !getClass().equals(obj.getClass())) {
			return false;
		}

		return querySettingsEquals((Query) obj);
	}

	/**
	 * Tests whether the settings of the given {@link Query} are equal to this query.
	 *
	 * @param that
	 * @return
	 */
	protected boolean querySettingsEquals(Query that) {

		boolean criteriaEqual = this.criteriaDefinitions.equals(that.criteriaDefinitions);
		boolean columnsEqual = nullSafeEquals(this.columns, that.columns);
		boolean sortEqual = nullSafeEquals(this.sort, that.sort);
		boolean pagingStateEqual = nullSafeEquals(this.scrollPosition, that.scrollPosition);
		boolean queryOptionsEqual = nullSafeEquals(this.queryOptions, that.queryOptions);
		boolean limitEqual = this.limit == that.limit;
		boolean allowFilteringEqual = this.allowFiltering == that.allowFiltering;

		return criteriaEqual && columnsEqual && sortEqual && pagingStateEqual && queryOptionsEqual && limitEqual
				&& allowFilteringEqual;
	}

	@Override
	public int hashCode() {
		int result = 17;
		result += 31 * criteriaDefinitions.hashCode();
		result += 31 * nullSafeHashCode(columns);
		result += 31 * nullSafeHashCode(sort);
		result += 31 * nullSafeHashCode(scrollPosition);
		result += 31 * nullSafeHashCode(queryOptions);
		result += 31 * nullSafeHashCode(limit);
		result += (allowFiltering ? 0 : 1);
		return result;
	}

	@Override
	public String toString() {

		String query = StreamSupport.stream(this.spliterator(), false).map(SerializationUtils::serializeToCqlSafely)
				.collect(Collectors.joining(" AND "));

		return String.format("Query: %s, Columns: %s, Sort: %s, Limit: %d", query, getColumns(), getSort(), getLimit());
	}
}
