/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.cassandra.core.query;

import static org.springframework.util.ObjectUtils.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.springframework.cassandra.core.QueryOptions;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.util.Assert;

import com.datastax.driver.core.PagingState;

/**
 * Query object representing {@link CriteriaDefinition}s, {@link Columns}, {@link Sort}, {@link PagingState} and
 * {@link QueryOptions} for a CQL query. {@link Query} is created with a fluent API creating immutable objects.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class Query implements Filter {

	private final List<CriteriaDefinition> criteriaDefinitions;

	private final Columns columns;

	private final Sort sort;

	private final Optional<PagingState> pagingState;

	private final Optional<QueryOptions> queryOptions;

	private final Optional<Long> limit;

	private final boolean allowFiltering;

	private Query(List<CriteriaDefinition> criteriaDefinitions, Columns columns, Sort sort,
			Optional<PagingState> pagingState, Optional<QueryOptions> queryOptions, Optional<Long> limit,
			boolean allowFiltering) {

		this.criteriaDefinitions = criteriaDefinitions;
		this.columns = columns;
		this.sort = sort;
		this.pagingState = pagingState;
		this.queryOptions = queryOptions;
		this.limit = limit;
		this.allowFiltering = allowFiltering;
	}

	/**
	 * Static factory method to create an empty {@link Query}
	 *
	 * @return the new {@link Query}.
	 */
	public static Query empty() {
		return new Query(Collections.emptyList(), Columns.empty(), Sort.unsorted(), Optional.empty(), Optional.empty(),
				Optional.empty(), false);
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

		return new Query(collect, Columns.empty(), Sort.unsorted(), Optional.empty(), Optional.empty(), Optional.empty(),
				false);
	}

	/**
	 * Add the given {@link CriteriaDefinition} to the current {@link Query}.
	 *
	 * @param criteriaDefinition must not be {@literal null}.
	 * @return a new {@link Query} object containing the former settings with {@link CriteriaDefinition} applied.
	 */
	public Query and(CriteriaDefinition criteriaDefinition) {

		Assert.notNull(criteriaDefinition, "Criteria must not be null");

		List<CriteriaDefinition> criteriaDefinitions = new ArrayList<>(this.criteriaDefinitions.size() + 1);
		criteriaDefinitions.addAll(this.criteriaDefinitions);

		if (!criteriaDefinitions.contains(criteriaDefinition)) {
			criteriaDefinitions.add(criteriaDefinition);
		}

		return new Query(criteriaDefinitions, columns, sort, pagingState, queryOptions, limit, allowFiltering);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.query.Filter#getCriteriaDefinitions()
	 */
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
	public Query columns(Columns columns) {

		Assert.notNull(columns, "Columns must not be null");

		return new Query(criteriaDefinitions, this.columns.and(columns), sort, pagingState, queryOptions, limit,
				allowFiltering);
	}

	/**
	 * @return the query {@link Columns}.
	 */
	public Columns getColumns() {
		return columns;
	}

	/**
	 * Add a {@link Sort} to the {@link Query} instance.
	 *
	 * @param sort must not be {@literal null}.
	 * @return a new {@link Query} object containing the former settings with {@link Sort} applied.
	 */
	public Query sort(Sort sort) {

		Assert.notNull(sort, "Sort must not be null");

		for (Order order : sort) {
			if (order.isIgnoreCase()) {
				throw new IllegalArgumentException(String.format("Given sort contained an Order for %s with ignore case! "
						+ "Apache Cassandra does not support sorting ignoring case currently!", order.getProperty()));
			}
		}

		return new Query(criteriaDefinitions, columns, this.sort.and(sort), pagingState, queryOptions, limit,
				allowFiltering);
	}

	/**
	 * @return the query {@link Sort} object.
	 */
	public Sort getSort() {
		return sort;
	}

	/**
	 * Set the {@link PagingState} to skip rows.
	 *
	 * @param pagingState must not be {@literal null}.
	 * @return a new {@link Query} object containing the former settings with {@link PagingState} applied.
	 */
	public Query pagingState(PagingState pagingState) {

		Assert.notNull(pagingState, "PagingState must not be null");

		return new Query(criteriaDefinitions, columns, sort, Optional.of(pagingState), queryOptions, limit, allowFiltering);
	}

	/**
	 * @return the optional {@link PagingState}.
	 */
	public Optional<PagingState> getPagingState() {
		return pagingState;
	}

	/**
	 * Set the {@link QueryOptions}.
	 *
	 * @param queryOptions must not be {@literal null}.
	 * @return a new {@link Query} object containing the former settings with {@link QueryOptions} applied.
	 */
	public Query queryOptions(QueryOptions queryOptions) {

		Assert.notNull(queryOptions, "QueryOptions must not be null");

		return new Query(criteriaDefinitions, columns, sort, pagingState, Optional.of(queryOptions), limit, allowFiltering);
	}

	/**
	 * @return the optional {@link QueryOptions}.
	 */
	public Optional<QueryOptions> getQueryOptions() {
		return queryOptions;
	}

	/**
	 * Limit the number of returned rows to {@code limit}.
	 *
	 * @param limit
	 * @return a new {@link Query} object containing the former settings with {@code limit} applied.
	 */
	public Query limit(long limit) {
		return new Query(criteriaDefinitions, columns, sort, pagingState, queryOptions, Optional.of(limit), allowFiltering);
	}

	/**
	 * @return the maximum number of rows to be returned.
	 */
	public long getLimit() {
		return this.limit.orElse(0L);
	}

	/**
	 * Allow filtering with {@code this} {@link Query}.
	 *
	 * @return a new {@link Query} object containing the former settings with {@code allowFiltering} applied.
	 */
	public Query withAllowFiltering() {

		return new Query(criteriaDefinitions, columns, sort, pagingState, queryOptions, limit, true);
	}

	/**
	 * @return {@literal true} to allow filtering.
	 */
	public boolean isAllowFiltering() {
		return allowFiltering;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

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
		boolean pagingStateEqual = nullSafeEquals(this.pagingState, that.pagingState);
		boolean queryOptionsEqual = nullSafeEquals(this.queryOptions, that.queryOptions);
		boolean limitEqual = this.limit == that.limit;
		boolean allowFilteringEqual = this.allowFiltering == that.allowFiltering;

		return criteriaEqual && columnsEqual && sortEqual && pagingStateEqual && queryOptionsEqual && limitEqual
				&& allowFilteringEqual;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {

		int result = 17;

		result += 31 * criteriaDefinitions.hashCode();
		result += 31 * nullSafeHashCode(columns);
		result += 31 * nullSafeHashCode(sort);
		result += 31 * nullSafeHashCode(pagingState);
		result += 31 * nullSafeHashCode(queryOptions);
		result += 31 * nullSafeHashCode(limit);
		result += (allowFiltering ? 0 : 1);

		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {

		String query = StreamSupport.stream(this.spliterator(), false) //
				.map(SerializationUtils::serializeToCqlSafely) //
				.collect(Collectors.joining(" AND "));

		return String.format("Query: %s, Columns: %s, Sort: %s, Limit: %d", query, getColumns(), getSort(), getLimit());
	}
}
