/*
 * Copyright 2018-present the original author or authors.
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
package org.springframework.data.cassandra.core;

import java.util.List;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.cassandra.core.cql.QueryExtractorDelegate;
import org.springframework.data.cassandra.core.cql.RowMapper;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.domain.Slice;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Implementation of {@link ExecutableSelectOperation}.
 *
 * @author Mark Paluch
 * @see org.springframework.data.cassandra.core.ExecutableSelectOperation
 * @see org.springframework.data.cassandra.core.query.Query
 * @since 2.1
 */
class ExecutableSelectOperationSupport implements ExecutableSelectOperation {

	private final CassandraTemplate template;
	private final QueryOperations queryOperations;

	public ExecutableSelectOperationSupport(CassandraTemplate template, QueryOperations queryOperations) {
		this.template = template;
		this.queryOperations = queryOperations;
	}

	@Override
	public <T> ExecutableSelect<T> query(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ExecutableSelectSupport<>(domainType, domainType, QueryResultConverter.entity(),
				Query.empty(), null);
	}

	@Override
	public UntypedSelect query(String cql) {

		Assert.hasText(cql, "CQL must not be empty");

		return new UntypedSelectSupport(SimpleStatement.newInstance(cql));
	}

	@Override
	public UntypedSelect query(Statement<?> statement) {

		Assert.notNull(statement, "Statement must not be null");

		return new UntypedSelectSupport(statement);
	}

	class UntypedSelectSupport implements UntypedSelect {

		private final Statement<?> statement;

		private UntypedSelectSupport(Statement<?> statement) {
			this.statement = statement;
		}

		@Override
		public <T> TerminatingResults<T> as(Class<T> resultType) {

			Assert.notNull(resultType, "Result type must not be null");

				return new TypedSelectSupport<>(statement, resultType);
			}

			@Override
			public <T> TerminatingResults<T> map(RowMapper<T> mapper) {

				Assert.notNull(mapper, "RowMapper must not be null");

				return new TerminatingSelectResultSupport<>(statement, mapper);
			}

			@Override
			public long count() {

				List<Row> rows = template.select(statement, Row.class);

				if (rows.size() == 1) {

					Object object = rows.get(0).getObject(0);

					return ((Number) object).longValue();
				}

				return 0;
			}
	}

	class TypedSelectSupport<T> extends TerminatingSelectResultSupport<T, T> implements TerminatingResults<T> {

		private final Class<T> domainType;

		TypedSelectSupport(Statement<?> statement, Class<T> domainType) {
			super(statement,
					ResultSet.class.isAssignableFrom(domainType) ? null : queryOperations.getRowMapper(domainType, statement));

			this.domainType = domainType;
		}

		@Override
		public <R> TerminatingResults<R> map(QueryResultConverter<? super T, ? extends R> converter) {

			Assert.notNull(converter, "Mapping function must not be null");

			return new TerminatingSelectResultSupport<>(this.statement, this.domainType, converter);
		}

	}

	class TerminatingSelectResultSupport<S, T> implements TerminatingResults<T> {

		final Statement<?> statement;

		final @Nullable RowMapper<T> rowMapper;

		TerminatingSelectResultSupport(Statement<?> statement,
				@Nullable RowMapper<T> rowMapper) {
			this.statement = statement;
			this.rowMapper = rowMapper;
		}

		TerminatingSelectResultSupport(Statement<?> statement, Class<S> domainType,
				QueryResultConverter<? super S, ? extends T> mappingFunction) {
			this(statement, queryOperations.getRowMapper(domainType, statement, mappingFunction));
		}

		@Override
		public <R> TerminatingResults<R> map(QueryResultConverter<? super T, ? extends R> converter) {

			return new TerminatingSelectResultSupport<>(this.statement, (row, rowNum) -> {

				return converter.mapRow(row, () -> {
					return this.rowMapper.mapRow(row, rowNum);
				});
			});
		}

		@Override
		public @Nullable T firstValue() {

			List<T> result = template.getCqlOperations().query(this.statement, this.rowMapper);

			return ObjectUtils.isEmpty(result) ? null : result.iterator().next();
		}

		@Override
		public @Nullable T oneValue() {

			if (this.rowMapper == null) {
				return (T) template.queryForResultSet(this.statement);
			}

			List<T> result = template.getCqlOperations().query(this.statement, this.rowMapper);

			if (ObjectUtils.isEmpty(result)) {
				return null;
			}

			if (result.size() > 1) {
				throw new IncorrectResultSizeDataAccessException(
						String.format("Query [%s] returned non unique result", QueryExtractorDelegate.getCql(this.statement)), 1);
			}

			return result.iterator().next();
		}

		@Override
		public List<T> all() {
			return template.getCqlOperations().query(this.statement, this.rowMapper);
		}

		@Override
		public Slice<T> slice() {
			return template.doSlice(this.statement, this.rowMapper);
		}

		@Override
		public Stream<T> stream() {
			return template.getCqlOperations().queryForStream(this.statement, this.rowMapper);
		}

	}

	class ExecutableSelectSupport<S, T> implements ExecutableSelect<T> {

		private final Class<?> domainType;

		private final Class<S> returnType;

		private final QueryResultConverter<? super S, ? extends T> mappingFunction;

		private final Query query;

		private final @Nullable CqlIdentifier tableName;

		public ExecutableSelectSupport(Class<?> domainType, Class<S> returnType,
				QueryResultConverter<? super S, ? extends T> mappingFunction, Query query,
				@Nullable CqlIdentifier tableName) {

			this.domainType = domainType;
			this.returnType = returnType;
			this.mappingFunction = mappingFunction;
			this.query = query;
			this.tableName = tableName;
		}

		@Override
		public <R> TerminatingResults<R> map(QueryResultConverter<? super T, ? extends R> converter) {

			Assert.notNull(converter, "Mapping function name must not be null");

			return new ExecutableSelectSupport<>(this.domainType, this.returnType,
					this.mappingFunction.andThen(converter), this.query, tableName);
		}

		@Override
		public SelectWithProjection<T> inTable(CqlIdentifier tableName) {

			Assert.notNull(tableName, "Table name must not be null");

			return new ExecutableSelectSupport<>(this.domainType, this.returnType, this.mappingFunction,
					this.query, tableName);
		}

		@Override
		public <R> SelectWithQuery<R> as(Class<R> returnType) {

			Assert.notNull(returnType, "ReturnType must not be null");

			return new ExecutableSelectSupport<>(this.domainType, returnType, QueryResultConverter.entity(),
					this.query, this.tableName);
		}

		@Override
		public TerminatingSelect<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null");

			return new ExecutableSelectSupport<>(this.domainType, this.returnType, this.mappingFunction, query,
					this.tableName);
		}

		@Override
		public long count() {
			return template.doCount(this.query, this.domainType, getTableName());
		}

		@Override
		public boolean exists() {
			return template.doExists(this.query, this.domainType, getTableName());
		}

		@Override
		public @Nullable T firstValue() {

			List<T> result = prepare(this.query.limit(1)).select(template::doQuery);

			return ObjectUtils.isEmpty(result) ? null : result.iterator().next();
		}

		@Override
		public @Nullable T oneValue() {

			if (this.returnType.equals(ResultSet.class)) {
				return (T) template.doSelectResultSet(this.query.limit(2), this.domainType, getTableName());
			}

			List<T> result = prepare(this.query.limit(2)).select(template::doQuery);

			if (ObjectUtils.isEmpty(result)) {
				return null;
			}

			if (result.size() > 1) {
				throw new IncorrectResultSizeDataAccessException(1, result.size());

			}

			return result.iterator().next();
		}

		@Override
		public List<T> all() {
			return prepare(this.query).select(template::doQuery);
		}

		@Override
		public Slice<T> slice() {
			return prepare(this.query).select(template::doSlice);
		}

		@Override
		public Stream<T> stream() {
			return prepare(this.query).select(template::doStream);
		}

		@SuppressWarnings("unchecked")
		public QueryOperations.TerminalSelect<T> prepare(Query query) {
			return (QueryOperations.TerminalSelect<T>) queryOperations.select(this.domainType, getTableName())
					.project(this.returnType, this.mappingFunction).matching(query);
		}

		private CqlIdentifier getTableName() {
			return this.tableName != null ? this.tableName : template.getTableName(this.domainType);
		}

	}


}
