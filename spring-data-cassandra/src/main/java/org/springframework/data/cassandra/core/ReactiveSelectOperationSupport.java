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

	public ReactiveSelectOperationSupport(ReactiveCassandraTemplate template) {
		this.template = template;
	}

	@Override
	public <T> ReactiveSelect<T> query(Class<T> domainType) {

		Assert.notNull(domainType, "DomainType must not be null");

		return new ReactiveSelectSupport<>(this.template, domainType, domainType, QueryResultConverter.entity(),
				Query.empty(), null);
	}

	@Override
	public UntypedSelect query(String cql) {

		Assert.hasText(cql, "CQL must not be empty");

		return new UntypedSelectSupport(this.template, SimpleStatement.newInstance(cql));
	}

	@Override
	public UntypedSelect query(Statement<?> statement) {

		Assert.notNull(statement, "Statement must not be null");

		return new UntypedSelectSupport(this.template, statement);
	}

	private record UntypedSelectSupport(ReactiveCassandraTemplate template,
			Statement<?> statement) implements UntypedSelect {

		@Override
		public <T> TerminatingResults<T> as(Class<T> resultType) {

			Assert.notNull(resultType, "Result type must not be null");

			return new TypedSelectSupport<>(template, statement, resultType);
		}

		@Override
		public <T> TerminatingResults<T> map(RowMapper<T> mapper) {

			Assert.notNull(mapper, "RowMapper must not be null");

			return new TerminatingSelectResultSupport<>(template, statement, mapper);
		}

	}

	static class TypedSelectSupport<T> extends TerminatingSelectResultSupport<T, T> implements TerminatingResults<T> {

		private final Class<T> domainType;

		TypedSelectSupport(ReactiveCassandraTemplate template, Statement<?> statement, Class<T> domainType) {
			super(template, statement,
					template.getRowMapper(domainType, EntityQueryUtils.getTableName(statement), QueryResultConverter.entity()));

			this.domainType = domainType;
		}

		@Override
		public <R> TerminatingResults<R> map(QueryResultConverter<? super T, ? extends R> converter) {

			Assert.notNull(converter, "Mapping function must not be null");

			return new TerminatingSelectResultSupport<>(this.template, this.statement, this.domainType, converter);
		}

	}

	static class TerminatingSelectResultSupport<S, T> implements TerminatingResults<T> {

		final ReactiveCassandraTemplate template;

		final Statement<?> statement;

		final RowMapper<T> rowMapper;

		TerminatingSelectResultSupport(ReactiveCassandraTemplate template, Statement<?> statement, RowMapper<T> rowMapper) {
			this.template = template;
			this.statement = statement;
			this.rowMapper = rowMapper;
		}

		TerminatingSelectResultSupport(ReactiveCassandraTemplate template, Statement<?> statement, Class<S> domainType,
				QueryResultConverter<? super S, ? extends T> mappingFunction) {
			this(template, statement,
					template.getRowMapper(domainType, EntityQueryUtils.getTableName(statement), mappingFunction));
		}

		@Override
		public <R> TerminatingResults<R> map(QueryResultConverter<? super T, ? extends R> converter) {

			return new TerminatingSelectResultSupport<>(this.template, this.statement, (row, rowNum) -> {

				return converter.mapRow(row, () -> {
					return this.rowMapper.mapRow(row, rowNum);
				});
			});
		}

		@Override
		public Mono<T> first() {
			return this.template.getReactiveCqlOperations().query(this.statement, this.rowMapper).next();
		}

		@Override
		public Mono<T> one() {

			Flux<T> result = this.template.getReactiveCqlOperations().query(this.statement, this.rowMapper);

			return result.collectList() //
					.handle((objects, sink) -> handleOne(objects, sink, () -> QueryExtractorDelegate.getCql(this.statement)));
		}

		@Override
		public Flux<T> all() {
			return this.template.getReactiveCqlOperations().query(this.statement, this.rowMapper);
		}

	}

	static class ReactiveSelectSupport<S, T> implements ReactiveSelect<T> {

		private final ReactiveCassandraTemplate template;

		private final Class<?> domainType;

		private final Class<S> returnType;

		private final QueryResultConverter<? super S, ? extends T> mappingFunction;

		private final Query query;

		private final @Nullable CqlIdentifier tableName;

		public ReactiveSelectSupport(ReactiveCassandraTemplate template, Class<?> domainType, Class<S> returnType,
				QueryResultConverter<? super S, ? extends T> mappingFunction, Query query, @Nullable CqlIdentifier tableName) {
			this.template = template;
			this.domainType = domainType;
			this.returnType = returnType;
			this.mappingFunction = mappingFunction;
			this.query = query;
			this.tableName = tableName;
		}

		@Override
		public <R> TerminatingResults<R> map(QueryResultConverter<? super T, ? extends R> converter) {

			Assert.notNull(converter, "Mapping function name must not be null");

			return new ReactiveSelectSupport<>(this.template, this.domainType, this.returnType,
					this.mappingFunction.andThen(converter), this.query, tableName);
		}

		@Override
		public SelectWithProjection<T> inTable(CqlIdentifier tableName) {

			Assert.notNull(tableName, "Table name must not be null");

			return new ReactiveSelectSupport<>(this.template, this.domainType, this.returnType, this.mappingFunction,
					this.query, tableName);
		}

		@Override
		public <R> SelectWithQuery<R> as(Class<R> returnType) {

			Assert.notNull(returnType, "ReturnType must not be null");

			return new ReactiveSelectSupport<>(this.template, this.domainType, returnType, QueryResultConverter.entity(),
					this.query, this.tableName);
		}

		@Override
		public TerminatingSelect<T> matching(Query query) {

			Assert.notNull(query, "Query must not be null");

			return new ReactiveSelectSupport<>(this.template, this.domainType, this.returnType, this.mappingFunction, query,
					this.tableName);
		}

		@Override
		public Mono<Long> count() {
			return this.template.doCount(this.query, this.domainType, getTableName());
		}

		@Override
		public Mono<Boolean> exists() {
			return this.template.doExists(this.query, this.domainType, getTableName());
		}

		@Override
		public Mono<T> first() {

			Flux<T> one = this.template.doSelect(this.query.limit(1), this.domainType, getTableName(), this.returnType,
					this.mappingFunction);

			return one.next();
		}

		@Override
		public Mono<T> one() {

			Flux<T> result = this.template.doSelect(this.query.limit(2), this.domainType, getTableName(), this.returnType,
					this.mappingFunction);

			return result.collectList() //
					.handle((objects, sink) -> handleOne(objects, sink, this.query::toString));
		}

		@Override
		public Flux<T> all() {
			return this.template.doSelect(this.query, this.domainType, getTableName(), this.returnType, this.mappingFunction);
		}

		private CqlIdentifier getTableName() {
			return this.tableName != null ? this.tableName : this.template.getTableName(this.domainType);
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
