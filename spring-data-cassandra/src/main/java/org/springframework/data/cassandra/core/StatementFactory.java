/*
 * Copyright 2017-2018 the original author or authors.
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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.springframework.data.cassandra.core.convert.QueryMapper;
import org.springframework.data.cassandra.core.convert.UpdateMapper;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.cql.QueryOptionsUtil;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.query.Columns.ColumnSelector;
import org.springframework.data.cassandra.core.query.Columns.FunctionCall;
import org.springframework.data.cassandra.core.query.Columns.Selector;
import org.springframework.data.cassandra.core.query.CriteriaDefinition;
import org.springframework.data.cassandra.core.query.CriteriaDefinition.Predicate;
import org.springframework.data.cassandra.core.query.Filter;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.Update;
import org.springframework.data.cassandra.core.query.Update.AddToMapOp;
import org.springframework.data.cassandra.core.query.Update.AddToOp;
import org.springframework.data.cassandra.core.query.Update.AddToOp.Mode;
import org.springframework.data.cassandra.core.query.Update.AssignmentOp;
import org.springframework.data.cassandra.core.query.Update.IncrOp;
import org.springframework.data.cassandra.core.query.Update.RemoveOp;
import org.springframework.data.cassandra.core.query.Update.SetAtIndexOp;
import org.springframework.data.cassandra.core.query.Update.SetAtKeyOp;
import org.springframework.data.cassandra.core.query.Update.SetOp;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.util.Assert;

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Ordering;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Select.SelectionOrAlias;
import com.google.common.primitives.Ints;

/**
 * Statement factory to render {@link Statement} from {@link Query} and {@link Update} objects.
 *
 * @author Mark Paluch
 * @author John Blum
 * @see Query
 * @see Update
 * @since 2.0
 */
public class StatementFactory {

	private final QueryMapper queryMapper;

	private final UpdateMapper updateMapper;

	/**
	 * Create {@link StatementFactory} given {@link UpdateMapper}.
	 *
	 * @param updateMapper must not be {@literal null}.
	 */
	public StatementFactory(UpdateMapper updateMapper) {
		this(updateMapper, updateMapper);
	}

	/**
	 * Create {@link StatementFactory} given {@link QueryMapper} and {@link UpdateMapper}.
	 *
	 * @param queryMapper must not be {@literal null}.
	 * @param updateMapper must not be {@literal null}.
	 */
	public StatementFactory(QueryMapper queryMapper, UpdateMapper updateMapper) {

		Assert.notNull(queryMapper, "QueryMapper must not be null");
		Assert.notNull(updateMapper, "UpdateMapper must not be null");

		this.queryMapper = queryMapper;
		this.updateMapper = updateMapper;
	}

	/**
	 * Returns the {@link QueryMapper} used to map {@link Query} to CQL-specific data types.
	 *
	 * @return the {@link QueryMapper} used to map {@link Query} to CQL-specific data types.
	 * @see QueryMapper
	 */
	protected QueryMapper getQueryMapper() {
		return this.queryMapper;
	}

	/**
	 * Returns the {@link UpdateMapper} used to map {@link Update} to CQL-specific data types.
	 *
	 * @return the {@link UpdateMapper} used to map {@link Update} to CQL-specific data types.
	 * @see UpdateMapper
	 */
	protected UpdateMapper getUpdateMapper() {
		return this.updateMapper;
	}

	/**
	 * Create a {@literal COUNT} statement by mapping {@link Query} to {@link Select}.
	 *
	 * @param query user-defined count {@link Query} to execute; must not be {@literal null}.
	 * @param entity {@link CassandraPersistentEntity entity} to count; must not be {@literal null}.
	 * @return the rendered {@link RegularStatement}.
	 * @since 2.1
	 */
	public RegularStatement count(Query query, CassandraPersistentEntity<?> entity) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entity, "Entity must not be null");

		return count(query, entity, entity.getTableName());
	}

	/**
	 * Create a {@literal COUNT} statement by mapping {@link Query} to {@link Select}.
	 *
	 * @param query user-defined count {@link Query} to execute; must not be {@literal null}.
	 * @param entity {@link CassandraPersistentEntity entity} to count; must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return the rendered {@link RegularStatement}.
	 * @since 2.1
	 */
	public RegularStatement count(Query query, CassandraPersistentEntity<?> entity, CqlIdentifier tableName) {

		Filter filter = getQueryMapper().getMappedObject(query, entity);

		List<Selector> selectors = Collections.singletonList(FunctionCall.from("COUNT", 1L));

		return createSelect(query, entity, filter, selectors, tableName);
	}

	/**
	 * Create a {@literal SELECT} statement by mapping {@link Query} to {@link Select}.
	 *
	 * @param query must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return the rendered {@link RegularStatement}.
	 */
	public RegularStatement select(Query query, CassandraPersistentEntity<?> entity) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entity, "Entity must not be null");

		return select(query, entity, entity.getTableName());
	}

	/**
	 * Create a {@literal SELECT} statement by mapping {@link Query} to {@link Select}.
	 *
	 * @param query must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return the rendered {@link RegularStatement}.
	 * @since 2.1
	 */
	public RegularStatement select(Query query, CassandraPersistentEntity<?> entity, CqlIdentifier tableName) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(entity, "Table name must not be null");

		Filter filter = getQueryMapper().getMappedObject(query, entity);

		List<Selector> selectors = getQueryMapper().getMappedSelectors(query.getColumns(), entity);

		return createSelect(query, entity, filter, selectors, tableName);
	}

	private Select createSelect(Query query, CassandraPersistentEntity<?> entity, Filter filter,
			List<Selector> selectors, CqlIdentifier tableName) {

		Sort sort = Optional.of(query.getSort()).map(querySort -> getQueryMapper().getMappedSort(querySort, entity))
				.orElse(Sort.unsorted());

		Select select = createSelectAndOrder(selectors, tableName, filter, sort);

		query.getQueryOptions().ifPresent(queryOptions -> QueryOptionsUtil.addQueryOptions(select, queryOptions));

		if (query.getLimit() > 0) {
			select.limit(Ints.checkedCast(query.getLimit()));
		}

		if (query.isAllowFiltering()) {
			select.allowFiltering();
		}

		query.getPagingState().ifPresent(select::setPagingState);

		return select;
	}

	private static Select createSelectAndOrder(List<Selector> selectors, CqlIdentifier from, Filter filter, Sort sort) {

		Select select;

		if (selectors.isEmpty()) {
			select = QueryBuilder.select().all().from(from.toCql());
		} else {

			Selection selection = QueryBuilder.select();

			selectors.forEach(selector ->
				selector.getAlias().map(CqlIdentifier::toCql).ifPresent(getSelection(selection, selector)::as));
			select = selection.from(from.toCql());
		}

		for (CriteriaDefinition criteriaDefinition : filter) {
			select.where(toClause(criteriaDefinition));
		}

		if (sort.isSorted()) {

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

	private static SelectionOrAlias getSelection(Selection selection, Selector selector) {

		if (selector instanceof FunctionCall) {

			Object[] objects = ((FunctionCall) selector).getParameters().stream().map(param -> {

				if (param instanceof ColumnSelector) {
					return QueryBuilder.column(((ColumnSelector) param).getExpression());
				}

				return param;

			}).toArray();

			return selection.fcall(selector.getExpression(), objects);
		}

		return selection.column(selector.getExpression());
	}

	/**
	 * Create an {@literal UPDATE} statement by mapping {@link Query} to {@link Update}.
	 *
	 * @param query must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return the rendered {@link RegularStatement}.
	 */
	public RegularStatement update(Query query, Update update, CassandraPersistentEntity<?> entity) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");
		Assert.notNull(entity, "Entity must not be null");

		return update(query, update, entity, entity.getTableName());
	}

	/**
	 * Create an {@literal UPDATE} statement by mapping {@link Query} to {@link Update}.
	 *
	 * @param query must not be {@literal null}.
	 * @param updateObj must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return the rendered {@link RegularStatement}.
	 * @since 2.1
	 */
	RegularStatement update(Query query, Update updateObj, CassandraPersistentEntity<?> entity, CqlIdentifier tableName) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(updateObj, "Update must not be null");
		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(tableName, "Table name must not be null");

		Filter filter = getQueryMapper().getMappedObject(query, entity);

		Update mappedUpdate = getUpdateMapper().getMappedObject(updateObj, entity);

		com.datastax.driver.core.querybuilder.Update update = update(tableName, mappedUpdate, filter);

		query.getQueryOptions().ifPresent(queryOptions -> {
			if (queryOptions instanceof WriteOptions) {
				QueryOptionsUtil.addWriteOptions(update, (WriteOptions) queryOptions);
			} else {
				QueryOptionsUtil.addQueryOptions(update, queryOptions);
			}
		});

		query.getPagingState().ifPresent(update::setPagingState);

		return update;
	}

	private static com.datastax.driver.core.querybuilder.Update update(CqlIdentifier table, Update mappedUpdate,
			Filter filter) {

		com.datastax.driver.core.querybuilder.Update update = QueryBuilder.update(table.toCql());

		for (AssignmentOp assignmentOp : mappedUpdate.getUpdateOperations()) {
			update.with(getAssignment(assignmentOp));
		}

		for (CriteriaDefinition criteriaDefinition : filter) {
			update.where(toClause(criteriaDefinition));
		}

		return update;
	}

	private static Assignment getAssignment(AssignmentOp assignmentOp) {

		if (assignmentOp instanceof SetOp) {
			return getAssignment((SetOp) assignmentOp);
		}

		if (assignmentOp instanceof RemoveOp) {
			return getAssignment((RemoveOp) assignmentOp);
		}

		if (assignmentOp instanceof IncrOp) {
			return getAssignment((IncrOp) assignmentOp);
		}

		if (assignmentOp instanceof AddToOp) {
			return getAssignment((AddToOp) assignmentOp);
		}

		if (assignmentOp instanceof AddToMapOp) {
			return getAssignment((AddToMapOp) assignmentOp);
		}

		throw new IllegalArgumentException(String.format("UpdateOp %s not supported", assignmentOp));
	}

	private static Assignment getAssignment(IncrOp incrOp) {

		return incrOp.getValue().intValue() > 0
				? QueryBuilder.incr(incrOp.getColumnName().toCql(), Math.abs(incrOp.getValue().intValue()))
				: QueryBuilder.decr(incrOp.getColumnName().toCql(), Math.abs(incrOp.getValue().intValue()));
	}

	private static Assignment getAssignment(SetOp updateOp) {

		if (updateOp instanceof SetAtIndexOp) {
			SetAtIndexOp op = (SetAtIndexOp) updateOp;
			return QueryBuilder.setIdx(op.getColumnName().toCql(), op.getIndex(), op.getValue());
		}

		if (updateOp instanceof SetAtKeyOp) {
			SetAtKeyOp op = (SetAtKeyOp) updateOp;
			return QueryBuilder.put(op.getColumnName().toCql(), op.getKey(), op.getValue());
		}

		return QueryBuilder.set(updateOp.getColumnName().toCql(), updateOp.getValue());
	}

	private static Assignment getAssignment(RemoveOp updateOp) {

		if (updateOp.getValue() instanceof Set) {
			return QueryBuilder.removeAll(updateOp.getColumnName().toCql(), (Set) updateOp.getValue());
		}

		if (updateOp.getValue() instanceof List) {
			return QueryBuilder.discardAll(updateOp.getColumnName().toCql(), (List) updateOp.getValue());
		}

		return QueryBuilder.remove(updateOp.getColumnName().toCql(), updateOp.getValue());
	}

	@SuppressWarnings("unchecked")
	private static Assignment getAssignment(AddToOp updateOp) {

		if (updateOp.getValue() instanceof Set) {
			return QueryBuilder.addAll(updateOp.getColumnName().toCql(), (Set) updateOp.getValue());
		}

		return Mode.PREPEND.equals(updateOp.getMode())
				? QueryBuilder.prependAll(updateOp.getColumnName().toCql(), (List) updateOp.getValue())
				: QueryBuilder.appendAll(updateOp.getColumnName().toCql(), (List) updateOp.getValue());
	}

	private static Assignment getAssignment(AddToMapOp updateOp) {
		return QueryBuilder.putAll(updateOp.getColumnName().toCql(), updateOp.getValue());
	}

	/**
	 * Create a {@literal DELETE} statement by mapping {@link Query} to {@link Delete}.
	 *
	 * @param query must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return the rendered {@link RegularStatement}.
	 */
	public RegularStatement delete(Query query, CassandraPersistentEntity<?> entity) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entity, "Entity must not be null");

		return delete(query, entity, entity.getTableName());
	}

	/**
	 * Create a {@literal DELETE} statement by mapping {@link Query} to {@link Delete}.
	 *
	 * @param query must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return the rendered {@link RegularStatement}.
	 * @see 2.1
	 */
	public RegularStatement delete(Query query, CassandraPersistentEntity<?> entity, CqlIdentifier tableName) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entity, "Entity must not be null");
		Assert.notNull(tableName, "Table name must not be null");

		Filter filter = getQueryMapper().getMappedObject(query, entity);

		List<String> columnNames = getQueryMapper().getMappedColumnNames(query.getColumns(), entity);

		Delete delete = delete(columnNames, tableName, filter);

		query.getQueryOptions().ifPresent(queryOptions -> {
			if (queryOptions instanceof WriteOptions) {
				QueryOptionsUtil.addWriteOptions(delete, (WriteOptions) queryOptions);
			} else {
				QueryOptionsUtil.addQueryOptions(delete, queryOptions);
			}
		});

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

		switch (predicate.getOperator().toString()) {

			case "=":
				return QueryBuilder.eq(columnName, predicate.getValue());

			case "!=":
				return QueryBuilder.ne(columnName, predicate.getValue());

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

				if (predicate.getValue() != null && predicate.getValue().getClass().isArray()) {
					return QueryBuilder.in(columnName, (Object[]) predicate.getValue());
				}

				return QueryBuilder.in(columnName, predicate.getValue());

			case "LIKE":
				return QueryBuilder.like(columnName, predicate.getValue());

			case "IS NOT NULL":
				return QueryBuilder.notNull(columnName);

			case "CONTAINS":
				Assert.state(predicate.getValue() != null,
						() -> String.format("CONTAINS value for column %s is null", columnName));
				return QueryBuilder.contains(columnName, predicate.getValue());

			case "CONTAINS KEY":
				Assert.state(predicate.getValue() != null,
						() -> String.format("CONTAINS KEY value for column %s is null", columnName));
				return QueryBuilder.containsKey(columnName, predicate.getValue());
		}

		throw new IllegalArgumentException(String.format("Criteria %s %s %s not supported",
				columnName, predicate.getOperator(), predicate.getValue()));
	}
}
