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
 * {@link QueryOptions} for a CQL query.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class Query implements Filter {

	private final List<CriteriaDefinition> criteriaDefinitions = new ArrayList<>();

	private Columns columns = Columns.empty();

	private Sort sort;

	private PagingState pagingState;

	private QueryOptions queryOptions;

	private int limit;

	private boolean allowFiltering;

	public Query() {}

	/**
	 * Static factory method to create a {@link Query} using the provided {@link CriteriaDefinition}.
	 *
	 * @param criteriaDefinitions must not be {@literal null}.
	 * @return the {@link Query} for {@link CriteriaDefinition}s.
	 */
	public static Query from(CriteriaDefinition... criteriaDefinitions) {

		Assert.notNull(criteriaDefinitions, "CriteriaDefinitions must not be null");

		return from(Arrays.asList(criteriaDefinitions));
	}

	/**
	 * Static factory method to create a {@link Query} using the provided {@link CriteriaDefinition}.
	 *
	 * @param criteriaDefinitions must not be {@literal null}.
	 * @return the {@link Query} for {@link CriteriaDefinition}s.
	 */
	public static Query from(Iterable<? extends CriteriaDefinition> criteriaDefinitions) {

		Assert.notNull(criteriaDefinitions, "CriteriaDefinitions must not be null");

		Query query = new Query();

		for (CriteriaDefinition criteriaDefinition : criteriaDefinitions) {
			query.with(criteriaDefinition);
		}

		return query;
	}

	/**
	 * Add the given {@link CriteriaDefinition} to the current {@link Query}.
	 *
	 * @param criteriaDefinition must not be {@literal null}.
	 * @return {@literal this} {@link Query}.
	 */
	public Query with(CriteriaDefinition criteriaDefinition) {

		Assert.notNull(criteriaDefinition, "Criteria must not be null");

		if (criteriaDefinition instanceof ChainedCriteria) {

			for (CriteriaDefinition definition : (ChainedCriteria) criteriaDefinition) {
				if (!this.criteriaDefinitions.contains(definition)) {
					this.criteriaDefinitions.add(definition);
				}
			}
		}

		if (!this.criteriaDefinitions.contains(criteriaDefinition)) {
			this.criteriaDefinitions.add(criteriaDefinition);
		}

		return this;
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
	 * @return {@code this} {@link Query}.
	 */
	public Query with(Columns columns) {

		Assert.notNull(columns, "Columns must not be null");

		this.columns = this.columns.and(columns);

		return this;
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
	 * @return {@code this} {@link Query}.
	 */
	public Query with(Sort sort) {

		Assert.notNull(sort, "Sort must not be null");

		for (Order order : sort) {
			if (order.isIgnoreCase()) {
				throw new IllegalArgumentException(String.format("Given sort contained an Order for %s with ignore case! "
						+ "Apache Cassandra does not support sorting ignoring case currently!", order.getProperty()));
			}
		}

		if (this.sort == null) {
			this.sort = sort;
		} else {
			this.sort = this.sort.and(sort);
		}

		return this;
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
	 * @return {@code this} {@link Query}.
	 */
	public Query with(PagingState pagingState) {

		Assert.notNull(pagingState, "PagingState must not be null");

		this.pagingState = pagingState;
		return this;
	}

	/**
	 * @return the optional {@link PagingState}.
	 */
	public Optional<PagingState> getPagingState() {
		return Optional.ofNullable(pagingState);
	}

	/**
	 * Set the {@link QueryOptions}.
	 *
	 * @param queryOptions must not be {@literal null}.
	 * @return {@code this} {@link Query}.
	 */
	public Query with(QueryOptions queryOptions) {

		Assert.notNull(queryOptions, "QueryOptions must not be null");

		this.queryOptions = queryOptions;
		return this;
	}

	/**
	 * @return the optional {@link QueryOptions}.
	 */
	public Optional<QueryOptions> getQueryOptions() {
		return Optional.ofNullable(queryOptions);
	}

	/**
	 * Limit the number of returned rows to {@code limit}.
	 *
	 * @param limit
	 * @return {@code this} {@link Query}.
	 */
	public Query limit(int limit) {

		this.limit = limit;
		return this;
	}

	/**
	 * @return the maximum number of rows to be returned.
	 */
	public int getLimit() {
		return this.limit;
	}

	/**
	 * Allow filtering with {@code this} {@link Query}.
	 *
	 * @return {@code this} {@link Query}.
	 */
	public Query withAllowFiltering() {

		this.allowFiltering = true;
		return this;
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
		result += 31 * limit;
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
