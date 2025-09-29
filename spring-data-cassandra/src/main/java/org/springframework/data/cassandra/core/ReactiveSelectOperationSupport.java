/*
 * Copyright 2018-2025 the original author or authors.
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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.util.List;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;

import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.data.cassandra.core.cql.QueryExtractorDelegate;
import org.springframework.data.cassandra.core.cql.RowMapper;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;

/**
 * Implementation of {@link ReactiveSelectOperation}.
 *
 * @author Mark Paluch
 * @see org.springframework.data.cassandra.core.ReactiveSelectOperation
 * @see org.springframework.data.cassandra.core.query.Query
 * @since 2.1
 */
class ReactiveSelectOperationSupport implements ReactiveSelectOperation {

	private final ReactiveCassandraTemplate template;
	private final QueryOperations queryOperations;

	public ReactiveSelectOperationSupport(ReactiveCassandraTemplate template, QueryOperations queryOperations) {
		this.template = template;
		this.queryOperations = queryOperations;
	}

	@Override
	public <T> ReactiveSelect<T> query(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ReactiveSelectSupport<>(domainType, domainType, QueryResultConverter.entity(),
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

			return new TerminatingSelectResultSupport<>(this.statement, mapper);
		}
	}

	class TypedSelectSupport<T> extends TerminatingSelectResultSupport<T, T> implements TerminatingResults<T> {

		private final Class<T> domainType;

		TypedSelectSupport(Statement<?> statement, Class<T> domainType) {
			super(statement, queryOperations.getRowMapper(domainType, statement));
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

		final RowMapper<T> rowMapper;

		TerminatingSelectResultSupport(Statement<?> statement, RowMapper<T> rowMapper) {
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
		public Mono<T> first() {
			return template.getReactiveCqlOperations().query(this.statement, this.rowMapper).next();
		}

		@Override
		public Mono<T> one() {

			Flux<T> result = template.getReactiveCqlOperations().query(this.statement, this.rowMapper);

			return result.collectList() //
					.handle((objects, sink) -> handleOne(objects, sink, () -> QueryExtractorDelegate.getCql(this.statement)));
		}

		@Override
		public Flux<T> all() {
			return template.getReactiveCqlOperations().query(this.statement, this.rowMapper);
		}

	}

	class ReactiveSelectSupport<S, T> implements ReactiveSelect<T> {

		private final Class<?> domainType;

		private final Class<S> returnType;

		private final QueryResultConverter<? super S, ? extends T> mappingFunction;

		private final Query query;

		private final @Nullable CqlIdentifier tableName;

		public ReactiveSelectSupport(Class<?> domainType, Class<S> returnType,
				QueryResultConverter<? super S, ? extends T> mappingFunction, Query query, @Nullable CqlIdentifier tableName) {
			this.domainType = domainType;
			this.returnType = returnType;
			this.mappingFunction = mappingFunction;
			this.query = query;
			this.tableName = tableName;
		}

		@Override
		public <R> TerminatingResults<R> map(QueryResultConverter<? super T, ? extends R> converter) {

			Assert.notNull(converter, "Mapping function name must not be null");

			return new ReactiveSelectSupport<>(this.domainType, this.returnType,
					this.mappingFunction.andThen(converter), this.query, tableName);
		}

		@Override
		public SelectWithProjection<T> inTable(CqlIdentifier tableName) {

			Assert.notNull(tableName, "Table name must not be null");

			return new ReactiveSelectSupport<>(this.domainType, this.returnType, this.mappingFunction,
					this.query, tableName);
		}

		@Override
		public <R> SelectWithQuery<R> as(Class<R> returnType) {

			Assert.notNull(returnType, "ReturnType must not be null");

			return new ReactiveSelectSupport<>(this.domainType, returnType, QueryResultConverter.entity(),
					this.query, this.tableName);
		}

		@Override
		public TerminatingSelect<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null");

			return new ReactiveSelectSupport<>(this.domainType, this.returnType, this.mappingFunction, query,
					this.tableName);
		}

		@Override
		public Mono<Long> count() {
			return template.doCount(this.query, this.domainType, getTableName());
		}

		@Override
		public Mono<Boolean> exists() {
			return template.doExists(this.query, this.domainType, getTableName());
		}

		@Override
		public Mono<T> first() {

			Flux<T> one = prepare(this.query.limit(1)).select(template::doQuery);

			return one.next();
		}

		@Override
		public Mono<T> one() {

			Flux<T> result = prepare(this.query.limit(2)).select(template::doQuery);

			return result.collectList() //
					.handle((objects, sink) -> handleOne(objects, sink, this.query::toString));
		}

		@Override
		public Flux<T> all() {
			return prepare(this.query).select(template::doQuery);
		}

		@SuppressWarnings("unchecked")
		public QueryOperations.TerminalSelect<T> prepare(Query query) {
			return (QueryOperations.TerminalSelect<T>) queryOperations.select(this.domainType, getTableName())
					.project(this.returnType, this.mappingFunction).matching(query);
		}

		private CqlIdentifier getTableName() {
			return this.tableName != null ? this.tableName : queryOperations.getTableName(this.domainType);
		}

	}

	private static <T> void handleOne(List<T> objects, SynchronousSink<T> sink, Supplier<@Nullable String> query) {

		if (objects.isEmpty()) {
			return;
		}

		if (objects.size() == 1) {
			sink.next(objects.get(0));
			return;
		}

		sink.error(new IncorrectResultSizeDataAccessException(
				String.format("Query [%s] returned non unique result", query.get()), 1));
	}

}
