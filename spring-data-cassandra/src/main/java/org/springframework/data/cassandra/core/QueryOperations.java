/*
 * Copyright 2019-2025 the original author or authors.
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

import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.cql.RowMapper;
import org.springframework.data.cassandra.core.cql.util.StatementBuilder;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.event.AfterConvertEvent;
import org.springframework.data.cassandra.core.mapping.event.AfterLoadEvent;
import org.springframework.data.cassandra.core.mapping.event.CassandraMappingEvent;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.util.Lazy;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.querybuilder.select.Select;

/**
 * QueryOperations centralizes common operations required before an operation is actually ready to be executed. This
 * involves mapping {@link Query queries} into their respective Cassandra representation and computing execution options
 *
 * @author Mark Paluch
 * @since 5.0
 */
class QueryOperations {

	private final CassandraConverter converter;

	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	private final StatementFactory statementFactory;

	private final EntityLifecycleEventDelegate eventDelegate;

	QueryOperations(CassandraConverter converter, StatementFactory statementFactory,
			EntityLifecycleEventDelegate eventDelegate) {

		this.converter = converter;
		this.mappingContext = converter.getMappingContext();
		this.statementFactory = statementFactory;
		this.eventDelegate = eventDelegate;
	}

	/**
	 * Returns the {@link MappingContext} used by this entity data access operations class to access mapping meta-data
	 * used to store (map) object to Cassandra tables.
	 *
	 * @return the {@link MappingContext} used by this entity data access operations class.
	 * @see org.springframework.data.cassandra.core.mapping.CassandraMappingContext
	 */
	public CassandraPersistentEntity<?> getRequiredPersistentEntity(Class<?> entityClass) {
		return mappingContext.getRequiredPersistentEntity(ClassUtils.getUserClass(entityClass));
	}

	public CqlIdentifier getTableName(Class<?> entityClass) {
		return getRequiredPersistentEntity(entityClass).getTableName();
	}

	/**
	 * Start creating a select operation for the given entity class.
	 *
	 * @param entityClass the queried entity class.
	 * @return the selection operation to continue building the query.
	 * @param <T> the entity type to infer the {@link RowMapper} type.
	 */
	public <T> Selection<T> select(Class<T> entityClass) {

		Assert.notNull(entityClass, "Entity class must not be null");

		return new DefaultSelection<>(entityClass);
	}

	/**
	 * Start creating a select operation for the given entity class using {@link CqlIdentifier table name}.
	 *
	 * @param entityClass the queried entity class.
	 * @param tableName the table name to use.
	 * @return the selection operation to continue building the query.
	 * @param <T> the entity type to infer the {@link RowMapper} type.
	 */
	public <T> Selection<T> select(Class<T> entityClass, CqlIdentifier tableName) {

		Assert.notNull(entityClass, "Entity class must not be null");
		Assert.notNull(tableName, "Table name class must not be null");

		return new DefaultSelection<>(entityClass, tableName);
	}

	/**
	 * Obtain a {@link RowMapper} for the given {@code domainClass} and try to resolve the table name from the given
	 * {@link Statement}. The returned row mapper emits {@link AfterLoadEvent} and {@link AfterConvertEvent} events when
	 * mapping results.
	 *
	 * @param domainClass the queried entity class.
	 * @param statement statement to extract the table name from.
	 * @return the row mapper to be used.
	 * @param <T> result type.
	 */
	public <T> RowMapper<T> getRowMapper(Class<T> domainClass, Statement<?> statement) {
		return getRowMapper(domainClass, statement, QueryResultConverter.entity());
	}

	/**
	 * Obtain a {@link RowMapper} for the given {@code domainClass} and try to resolve the table name from the given
	 * {@link Statement} considering {@link QueryResultConverter}.The returned row mapper emits {@link AfterLoadEvent} and
	 * {@link AfterConvertEvent} events when mapping results.
	 *
	 * @param domainClass the queried entity class.
	 * @param statement statement to extract the table name from.
	 * @param mappingFunction the mapping function to apply.
	 * @return the row mapper to be used.
	 * @param <T> result type.
	 * @param <R> type returned by the mapping function.
	 */
	public <T, R> RowMapper<R> getRowMapper(Class<T> domainClass, Statement<?> statement,
			QueryResultConverter<? super T, ? extends R> mappingFunction) {
		return getRowMapper(domainClass, EntityQueryUtils.getTableName(statement), mappingFunction);
	}

	/**
	 * Obtain a {@link RowMapper} for the given {@code domainClass} and {@code tableName} considering
	 * {@link QueryResultConverter}. The returned row mapper emits {@link AfterLoadEvent} and {@link AfterConvertEvent}
	 * events when mapping results.
	 *
	 * @param domainClass the queried entity class.
	 * @param tableName the table name in use.
	 * @param mappingFunction the mapping function to apply.
	 * @return the row mapper to be used.
	 * @param <T> result type.
	 * @param <R> type returned by the mapping function.
	 */
	protected <T, R> RowMapper<R> getRowMapper(Class<T> domainClass, CqlIdentifier tableName,
			QueryResultConverter<? super T, ? extends R> mappingFunction) {
		return getRowMapper(statementFactory.getEntityProjection(domainClass), tableName, mappingFunction);
	}

	@SuppressWarnings("unchecked")
	private <T, R> RowMapper<R> getRowMapper(EntityProjection<T, ?> projection, CqlIdentifier tableName,
			QueryResultConverter<? super T, ? extends R> mappingFunction) {

		Function<Row, T> mapper = getMapper(projection, tableName);

		return mappingFunction == QueryResultConverter.entity() ? (row, rowNum) -> (R) mapper.apply(row)
				: (row, rowNum) -> {
					Lazy<T> reader = Lazy.of(() -> mapper.apply(row));
					return mappingFunction.mapRow(row, reader::get);
				};
	}

	/**
	 * Obtain a {@link Function} to map {@link Row}s into entities of the given {@code domainClass} considering the table
	 * name from {@link Statement}. The returned function emits {@link AfterLoadEvent} and {@link AfterConvertEvent}
	 * events when mapping results.
	 *
	 * @param domainClass the queried entity class.
	 * @param statement statement to extract the table name from.
	 * @return the mapping function to map rows to entities.
	 * @param <T> entity type.
	 */
	public <T> Function<Row, T> getMapper(Class<T> domainClass, Statement<?> statement) {
		return getMapper(statementFactory.getEntityProjection(domainClass), EntityQueryUtils.getTableName(statement));
	}

	private <T> Function<Row, T> getMapper(EntityProjection<T, ?> projection, CqlIdentifier tableName) {

		Class<T> targetType = projection.getMappedType().getType();

		return row -> {

			maybeEmitEvent(() -> new AfterLoadEvent<>(row, targetType, tableName));

			T result = converter.project(projection, row);

			// while it should not be possible, we safe-guard against null results here.
			if (result != null) {
				maybeEmitEvent(() -> new AfterConvertEvent<>(row, result, tableName));
			}

			return result;
		};
	}

	private <E extends CassandraMappingEvent<T>, T> void maybeEmitEvent(Supplier<E> event) {
		this.eventDelegate.publishEvent(event);
	}

	/**
	 * Functional interface to create a select operation.
	 *
	 * @param <T>
	 */
	interface Selection<T> {

		/**
		 * Apply a projection to the selection operation. The returned {@link Projection} allows to continue the selection
		 * operation.
		 *
		 * @param returnType the return type.
		 * @param mappingFunction the mapping function to be applied to rows and the intermediate return type.
		 * @return the projection to continue building the query.
		 * @param <P> intermediate type used to map the {@link Row} before applying the {@code mappingFunction}.
		 * @param <R> the result type.
		 */
		<P, R> Projection<R> project(Class<P> returnType, QueryResultConverter<? super P, ? extends R> mappingFunction);

		/**
		 * Provide a {@link Query} to define the query.
		 *
		 * @param query must not be {@literal null}.
		 * @return the terminal selection operation to continue building the query.
		 */
		TerminalSelectExists<T> matching(Query query);

		/**
		 * Provide an identifier to query by id. The id property name is obtained from the mapping metadata.
		 *
		 * @param identifier must not be {@literal null}.
		 * @return the terminal selection operation to continue building the query.
		 */
		TerminalSelectExists<T> matchingId(Object identifier);

	}

	/**
	 * Functional interface to continue a selection operation with a projection.
	 *
	 * @param <T>
	 */
	interface Projection<T> {

		/**
		 * Provide a {@link Query} to define the query.
		 *
		 * @param query must not be {@literal null}.
		 * @return the terminal selection operation to continue building the query.
		 */
		TerminalSelect<T> matching(Query query);

	}

	/**
	 * Functional interface to continue a selection operation.
	 *
	 * @param <T>
	 */
	interface TerminalSelectExists<T> extends TerminalSelect<T>, TerminalExists {

	}

	/**
	 * Functional interface to continue a selection operation.
	 */
	interface TerminalExists {

		/**
		 * Apply the given function to the {@link Statement} to check for existence. The statement ensures a limited query.
		 *
		 * @param function function accepting the statement.
		 * @return return value of the function.
		 * @param <V> the result type.
		 */
		<V> V exists(Function<Statement<?>, V> function);

	}

	/**
	 * Fluent interface to apply the built {@link Statement} and {@link RowMapper} to a function that produces a result.
	 *
	 * @param <T>
	 */
	interface TerminalSelect<T> {

		/**
		 * Apply the given function to the {@link Statement} and {@link RowMapper}.
		 *
		 * @param function function accepting the statement.
		 * @return return value of the function.
		 * @param <V> the result type.
		 */
		<V> V select(BiFunction<Statement<?>, RowMapper<T>, V> function);

	}

	/**
	 * Default implementation of {@link Selection}.
	 *
	 * @param <T>
	 */
	class DefaultSelection<T> implements Selection<T>, TerminalSelectExists<T> {

		private final Class<T> entityClass;
		private final CassandraPersistentEntity<?> persistentEntity;
		private final CqlIdentifier tableName;
		private Query query = Query.empty();

		public DefaultSelection(Class<T> entityClass) {
			this.entityClass = entityClass;
			this.persistentEntity = mappingContext.getRequiredPersistentEntity(entityClass);
			this.tableName = persistentEntity.getTableName();
		}

		public DefaultSelection(Class<T> entityClass, CqlIdentifier tableName) {
			this.entityClass = entityClass;
			this.persistentEntity = mappingContext.getRequiredPersistentEntity(entityClass);
			this.tableName = tableName;
		}

		@Override
		public TerminalSelectExists<T> matching(Query query) {
			this.query = query;
			return this;
		}

		@Override
		public TerminalSelectExists<T> matchingId(Object identifier) {

			return new TerminalSelectExists<>() {

				@Override
				public <V> V exists(Function<Statement<?>, V> function) {
					SimpleStatement statement = statementFactory.selectExists(identifier, persistentEntity, tableName).build();
					return function.apply(statement);
				}

				@Override
				public <V> V select(BiFunction<Statement<?>, RowMapper<T>, V> function) {
					SimpleStatement statement = statementFactory.selectOneById(identifier, persistentEntity, tableName).build();
					RowMapper<T> rowMapper = getRowMapper(entityClass, tableName, QueryResultConverter.entity());
					return function.apply(statement, rowMapper);
				}
			};
		}

		@Override
		public <P, R> Projection<R> project(Class<P> returnType,
				QueryResultConverter<? super P, ? extends R> mappingFunction) {
			return new DefaultProjection<>(this, returnType, mappingFunction);
		}

		@Override
		public <V> V exists(Function<Statement<?>, V> function) {

			StatementBuilder<Select> builder = statementFactory.selectExists(query,
					statementFactory.getEntityProjection(this.entityClass), persistentEntity, tableName);

			return function.apply(builder.build());
		}

		@Override
		public <V> V select(BiFunction<Statement<?>, RowMapper<T>, V> function) {

			StatementBuilder<Select> select = statementFactory.select(query, persistentEntity, tableName);
			RowMapper<T> mapper = getRowMapper(entityClass, tableName, QueryResultConverter.entity());

			return function.apply(select.build(), mapper);
		}

	}

	class DefaultProjection<E, R, T> implements Projection<R>, TerminalSelect<R> {

		private final DefaultSelection<E> selection;
		private final Class<T> returnType;
		private final QueryResultConverter<? super T, ? extends R> resultConverter;

		public DefaultProjection(DefaultSelection<E> selection, Class<T> returnType,
				QueryResultConverter<? super T, ? extends R> resultConverter) {
			this.selection = selection;
			this.returnType = returnType;
			this.resultConverter = resultConverter;
		}

		@Override
		public TerminalSelect<R> matching(Query query) {
			selection.matching(query);
			return this;
		}

		@Override
		public <V> V select(BiFunction<Statement<?>, RowMapper<R>, V> function) {

			EntityProjection<T, E> projection = statementFactory.getEntityProjection(selection.entityClass, returnType);
			StatementBuilder<Select> select = statementFactory.select(selection.query, projection, selection.persistentEntity,
					selection.tableName);
			RowMapper<R> mapper = getRowMapper(projection, selection.tableName, resultConverter);

			return function.apply(select.build(), mapper);
		}

	}

}
