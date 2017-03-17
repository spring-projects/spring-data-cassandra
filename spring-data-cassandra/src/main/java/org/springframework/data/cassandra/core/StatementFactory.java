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
package org.springframework.data.cassandra.core;

import java.util.ArrayList;
import java.util.List;

import org.springframework.cassandra.core.QueryOptionsUtil;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.convert.QueryMapper;
import org.springframework.data.cassandra.core.query.Columns.Column;
import org.springframework.data.cassandra.core.query.Columns.FunctionCall;
import org.springframework.data.cassandra.core.query.CriteriaDefinition;
import org.springframework.data.cassandra.core.query.CriteriaDefinition.Predicate;
import org.springframework.data.cassandra.core.query.Filter;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.util.Assert;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Ordering;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Select.SelectionOrAlias;
import com.datastax.driver.core.querybuilder.Update;

/**
 * Statement factory to render {@link Statement} from {@link Query} and {@link Update} objects.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class StatementFactory {

	private final QueryMapper queryMapper;

	/**
	 * Create {@link StatementFactory} given {@link QueryMapper}.
	 *
	 * @param updateMapper must not be {@literal null}.
	 */
	public StatementFactory(QueryMapper queryMapper) {

		Assert.notNull(queryMapper, "QueryMapper must not be null");

		this.queryMapper = queryMapper;
	}

	/**
	 * Create a {@literal SELECT} statement by mapping {@link Query} to {@link Select}.
	 *
	 * @param query
	 * @param entity
	 * @return
	 */
	public RegularStatement select(Query query, CassandraPersistentEntity<?> entity) {

		List<Column> selectors = queryMapper.getMappedSelectors(query.getColumns(), entity);
		Filter filter = queryMapper.getMappedObject(query, entity);
		Sort sort = query.getSort() != null ? queryMapper.getMappedSort(query.getSort(), entity) : null;

		Select select = select(selectors, entity.getTableName(), filter, sort);

		query.getQueryOptions().ifPresent(queryOptions -> QueryOptionsUtil.addQueryOptions(select, queryOptions));

		if (query.getLimit() > 0) {
			select.limit(query.getLimit());
		}

		if (query.isAllowFiltering()) {
			select.allowFiltering();
		}

		query.getPagingState().ifPresent(select::setPagingState);

		return select;
	}

	private static Select select(List<Column> selectors, CqlIdentifier from, Filter filter, Sort sort) {

		Select select;

		if (selectors.isEmpty()) {
			select = QueryBuilder.select().all().from(from.toCql());
		} else {
			Selection selection = QueryBuilder.select();
			selectors.forEach(selector -> {

				SelectionOrAlias column;

				if (selector instanceof FunctionCall) {

					Object[] objects = ((FunctionCall) selector).getParameters().stream().map(o -> {

						if (o instanceof Column) {
							return QueryBuilder.column(((Column) o).getExpression());
						}

						return o;

					}).toArray();

					column = selection.fcall(selector.getExpression(), objects);
				} else {
					column = selection.column(selector.getExpression());
				}

				selector.getAlias().map(CqlIdentifier::toCql).ifPresent(column::as);
			});
			select = selection.from(from.toCql());
		}

		for (CriteriaDefinition criteriaDefinition : filter) {
			select.where(toClause(criteriaDefinition));
		}

		if (sort != null) {

			List<Ordering> orderings = new ArrayList<>();
			for (Order order : sort) {

				if (order.isAscending()) {
					orderings.add(QueryBuilder.asc(order.getProperty()));
				} else {
					orderings.add(QueryBuilder.desc(order.getProperty()));
				}
			}

			if (!orderings.isEmpty()) {
				select.orderBy(orderings.toArray(new Ordering[orderings.size()]));
			}
		}

		return select;
	}

	/**
	 * Create a {@literal DELETE} statement by mapping {@link Query} to {@link Delete}.
	 *
	 * @param query
	 * @param entity
	 * @return
	 */
	public RegularStatement delete(Query query, CassandraPersistentEntity<?> entity) {

		List<String> columnNames = queryMapper.getMappedColumnNames(query.getColumns(), entity);
		Filter filter = queryMapper.getMappedObject(query, entity);

		Delete delete = delete(columnNames, entity.getTableName(), filter);

		query.getQueryOptions().ifPresent(queryOptions -> QueryOptionsUtil.addQueryOptions(delete, queryOptions));

		query.getPagingState().ifPresent(delete::setPagingState);

		return delete;
	}

	private static Delete delete(List<String> columnNames, CqlIdentifier from, Filter filter) {

		Delete select;

		if (columnNames.isEmpty()) {
			select = QueryBuilder.delete().all().from(from.toCql());
		} else {
			Delete.Selection selection = QueryBuilder.delete();
			columnNames.forEach(selection::column);
			select = selection.from(from.toCql());
		}

		for (CriteriaDefinition criteriaDefinition : filter) {
			select.where(toClause(criteriaDefinition));
		}

		return select;
	}

	private static Clause toClause(CriteriaDefinition criteriaDefinition) {

		Predicate predicate = criteriaDefinition.getPredicate();
		String columnName = criteriaDefinition.getColumnName().toCql();

		switch (predicate.getOperator()) {
			case "=":
				return QueryBuilder.eq(columnName, predicate.getValue());

			case ">":
				return QueryBuilder.gt(columnName, predicate.getValue());

			case ">=":
				return QueryBuilder.gte(columnName, predicate.getValue());

			case "<":
				return QueryBuilder.lt(columnName, predicate.getValue());

			case "<=":
				return QueryBuilder.lte(columnName, predicate.getValue());

			case "IN":

				if (predicate.getValue() instanceof List) {
					return QueryBuilder.in(columnName, (List<?>) predicate.getValue());
				}

				if (predicate.getValue().getClass().isArray()) {
					return QueryBuilder.in(columnName, (Object[]) predicate.getValue());
				}

				return QueryBuilder.in(columnName, predicate.getValue());

			case "LIKE":
				return QueryBuilder.like(columnName, predicate.getValue());

			case "CONTAINS":
				return QueryBuilder.contains(columnName, predicate.getValue());

			case "CONTAINS KEY":
				return QueryBuilder.containsKey(columnName, predicate.getValue());
		}

		throw new IllegalArgumentException(
				String.format("Criteria %s %s %s not supported", columnName, predicate.getOperator(), predicate.getValue()));
	}
}
