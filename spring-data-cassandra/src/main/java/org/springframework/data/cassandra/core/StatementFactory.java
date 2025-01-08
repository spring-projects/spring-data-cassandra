/*
 * Copyright 2017-2025 the original author or authors.
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

import java.beans.PropertyDescriptor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.QueryMapper;
import org.springframework.data.cassandra.core.convert.UpdateMapper;
import org.springframework.data.cassandra.core.convert.Where;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.cql.QueryOptionsUtil;
import org.springframework.data.cassandra.core.cql.QueryOptionsUtil.CqlStatementOptionsAccessor;
import org.springframework.data.cassandra.core.cql.WriteOptions;
import org.springframework.data.cassandra.core.cql.util.StatementBuilder;
import org.springframework.data.cassandra.core.cql.util.TermFactory;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.PersistentPropertyTranslator;
import org.springframework.data.cassandra.core.query.Columns;
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
import org.springframework.data.convert.EntityWriter;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.projection.EntityProjection;
import org.springframework.data.projection.ProjectionInformation;
import org.springframework.data.util.Predicates;
import org.springframework.data.util.ProxyUtils;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.BindMarker;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.condition.Condition;
import com.datastax.oss.driver.api.querybuilder.condition.ConditionBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.delete.DeleteSelection;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.relation.ColumnRelationBuilder;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.api.querybuilder.update.Assignment;
import com.datastax.oss.driver.api.querybuilder.update.OngoingAssignment;
import com.datastax.oss.driver.api.querybuilder.update.UpdateStart;
import com.datastax.oss.driver.api.querybuilder.update.UpdateWithAssignments;

/**
 * Factory to render {@link com.datastax.oss.driver.api.core.cql.Statement} objects from {@link Query} and
 * {@link Update} objects.
 *
 * @author Mark Paluch
 * @author John Blum
 * @author Sam Lightfoot
 * @see com.datastax.oss.driver.api.core.cql.Statement
 * @see org.springframework.data.cassandra.core.query.Query
 * @see org.springframework.data.cassandra.core.query.Update
 * @since 2.0
 */
public class StatementFactory {

	private final CassandraConverter cassandraConverter;

	private final QueryMapper queryMapper;

	private final UpdateMapper updateMapper;

	private KeyspaceProvider keyspaceProvider = KeyspaceProviders.ENTITY_KEYSPACE;

	/**
	 * Create {@link StatementFactory} given {@link CassandraConverter}.
	 *
	 * @param converter must not be {@literal null}.
	 * @since 3.0
	 */
	public StatementFactory(CassandraConverter converter) {

		Assert.notNull(converter, "CassandraConverter must not be null");

		this.cassandraConverter = converter;

		UpdateMapper updateMapper = new UpdateMapper(converter);
		this.queryMapper = updateMapper;
		this.updateMapper = updateMapper;
	}

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

		this.cassandraConverter = queryMapper.getConverter();
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
	 * Sets the {@link KeyspaceProvider} to determine the {@link CqlIdentifier keyspace} for a
	 * {@link CassandraPersistentEntity entity}-related statement.
	 *
	 * @param keyspaceProvider the keyspace provider to use, must not be {@literal null}.
	 * @since 4.4
	 */
	public void setKeyspaceProvider(KeyspaceProvider keyspaceProvider) {

		Assert.notNull(keyspaceProvider, "KeyspaceProvider must not be null");

		this.keyspaceProvider = keyspaceProvider;
	}

	/**
	 * Create a {@literal COUNT} statement by mapping {@link Query} to {@link Select}.
	 *
	 * @param query user-defined count {@link Query} to execute; must not be {@literal null}.
	 * @param entity {@link CassandraPersistentEntity entity} to count; must not be {@literal null}.
	 * @return the select builder.
	 * @since 2.1
	 */
	public StatementBuilder<Select> count(Query query, CassandraPersistentEntity<?> entity) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		return count(query, entity, entity.getTableName());
	}

	/**
	 * Create a {@literal COUNT} statement by mapping {@link Query} to {@link Select}.
	 *
	 * @param query user-defined count {@link Query} to execute; must not be {@literal null}.
	 * @param entity {@link CassandraPersistentEntity entity} to count; must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return the select builder.
	 * @since 2.1
	 */
	public StatementBuilder<Select> count(Query query, CassandraPersistentEntity<?> entity, CqlIdentifier tableName) {

		Filter filter = getQueryMapper().getMappedObject(query, entity);

		List<Selector> selectors = Collections.singletonList(FunctionCall.from("COUNT", 1L));

		return createSelect(query, entity, filter, selectors, tableName);
	}

	/**
	 * Create an {@literal SELECT} statement by mapping {@code id} to {@literal SELECT … WHERE} considering
	 * {@link UpdateOptions}.
	 *
	 * @param id must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return the select builder.
	 */
	public StatementBuilder<Select> selectOneById(Object id, CassandraPersistentEntity<?> entity,
			CqlIdentifier tableName) {

		Where where = new Where();

		cassandraConverter.write(id, where, entity);

		return StatementBuilder
				.of(QueryBuilder.selectFrom(getKeyspace(entity, tableName), tableName).all().limit(1),
						cassandraConverter.getCodecRegistry())
				.bind((statement, factory) -> statement.where(toRelations(where, factory)));
	}

	/**
	 * Create a {@literal SELECT} statement by mapping {@link Query} to {@link Select}.
	 *
	 * @param query must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return the select builder.
	 */
	public StatementBuilder<Select> select(Query query, CassandraPersistentEntity<?> entity) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		return select(query, entity, entity.getTableName());
	}

	/**
	 * Create a {@literal SELECT} statement by mapping {@link Query} to {@link Select}.
	 *
	 * @param query must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return the select builder.
	 * @since 2.1
	 */
	public StatementBuilder<Select> select(Query query, CassandraPersistentEntity<?> entity, CqlIdentifier tableName) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");
		Assert.notNull(entity, "Table name must not be null");

		Filter filter = getQueryMapper().getMappedObject(query, entity);

		List<Selector> selectors = getQueryMapper().getMappedSelectors(query.getColumns(), entity);

		return createSelect(query, entity, filter, selectors, tableName);
	}

	/**
	 * Creates a Query Object for an insert.
	 *
	 * @param objectToInsert the object to save, must not be {@literal null}.
	 * @param options optional {@link WriteOptions} to apply to the {@link Insert} statement, may be {@literal null}.
	 * @return the select builder.
	 * @since 3.0
	 */
	public StatementBuilder<RegularInsert> insert(Object objectToInsert, WriteOptions options) {

		Assert.notNull(objectToInsert, "Object to builder must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		CassandraPersistentEntity<?> entity = cassandraConverter.getMappingContext()
				.getRequiredPersistentEntity(objectToInsert.getClass());
		return insert(objectToInsert, options, entity, entity.getTableName());
	}

	/**
	 * Creates a Query Object for an insert.
	 *
	 * @param tableName the table name, must not be empty and not {@literal null}.
	 * @param objectToInsert the object to save, must not be {@literal null}.
	 * @param options optional {@link WriteOptions} to apply to the {@link Insert} statement, may be {@literal null}.
	 * @param entity the {@link CassandraPersistentEntity} to write insert values.
	 * @return the select builder.
	 */
	public StatementBuilder<RegularInsert> insert(Object objectToInsert, WriteOptions options,
			CassandraPersistentEntity<?> entity, CqlIdentifier tableName) {

		Assert.notNull(tableName, "TableName must not be null");
		Assert.notNull(objectToInsert, "Object to insert must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		boolean insertNulls;

		if (options instanceof InsertOptions insertOptions) {
			insertNulls = insertOptions.isInsertNulls();
		} else {
			insertNulls = false;
		}

		Map<CqlIdentifier, Object> object = new LinkedHashMap<>();
		cassandraConverter.write(objectToInsert, object, entity);

		StatementBuilder<RegularInsert> builder = StatementBuilder
				.of(QueryBuilder.insertInto(getKeyspace(entity, tableName), tableName).valuesByIds(Collections.emptyMap()),
						cassandraConverter.getCodecRegistry())
				.bind((statement, factory) -> {

					Map<CqlIdentifier, Term> values = createTerms(insertNulls, object, factory);
					CqlStatementOptionsAccessor<Insert> accessor = factory.ifBoundOrInline(
							bindings -> CqlStatementOptionsAccessor.ofInsert(bindings, statement),
							() -> CqlStatementOptionsAccessor.ofInsert(statement));
					RegularInsert afterOptions = (RegularInsert) addInsertOptions(accessor, options);

					return afterOptions.valuesByIds(values);
				});

		builder.transform(statement -> QueryOptionsUtil.addQueryOptions(statement, options));

		return builder;
	}

	private static Map<CqlIdentifier, Term> createTerms(boolean insertNulls, Map<CqlIdentifier, Object> object,
			TermFactory factory) {

		Map<CqlIdentifier, Term> values = new LinkedHashMap<>(object.size());

		object.forEach((cqlIdentifier, o) -> {

			if (o == null && !insertNulls) {
				return;
			}
			values.put(cqlIdentifier, factory.create(o));
		});
		return values;
	}

	/**
	 * Create an {@literal UPDATE} statement by mapping {@link Query} to {@link Update}.
	 *
	 * @param query must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return the update builder.
	 */
	public StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update(Query query, Update update,
			CassandraPersistentEntity<?> entity) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		return update(query, update, entity, entity.getTableName());
	}

	/**
	 * Create an {@literal UPDATE} statement by mapping {@link Query} to {@link Update}.
	 *
	 * @param query must not be {@literal null}.
	 * @param update must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return the update builder.
	 * @since 2.1
	 */
	StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update(Query query, Update update,
			CassandraPersistentEntity<?> entity, CqlIdentifier tableName) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(update, "Update must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");
		Assert.notNull(tableName, "Table name must not be null");

		Filter filter = getQueryMapper().getMappedObject(query, entity);

		Update mappedUpdate = getUpdateMapper().getMappedObject(update, entity);

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> builder = update(entity, tableName,
				mappedUpdate, filter,
				query.getQueryOptions().filter(WriteOptions.class::isInstance).map(WriteOptions.class::cast));

		query.getQueryOptions().filter(UpdateOptions.class::isInstance).map(UpdateOptions.class::cast)
				.map(UpdateOptions::getIfCondition)
				.ifPresent(criteriaDefinitions -> applyUpdateIfCondition(builder, criteriaDefinitions));

		query.getQueryOptions().ifPresent(
				options -> builder.transform(statementBuilder -> QueryOptionsUtil.addQueryOptions(statementBuilder, options)));

		return builder;
	}

	/**
	 * Create an {@literal UPDATE} statement by mapping {@code objectToUpdate} to {@link Update} considering
	 * {@link UpdateOptions}.
	 *
	 * @param objectToUpdate must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @return the update builder.
	 * @since 3.0
	 */
	public StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update(Object objectToUpdate,
			WriteOptions options) {

		Assert.notNull(objectToUpdate, "Object to builder must not be null");
		Assert.notNull(options, "WriteOptions must not be null");

		CassandraPersistentEntity<?> entity = cassandraConverter.getMappingContext()
				.getRequiredPersistentEntity(objectToUpdate.getClass());

		return update(objectToUpdate, options, entity, entity.getTableName());
	}

	/**
	 * Create an {@literal UPDATE} statement by mapping {@code objectToUpdate} to {@link Update} considering
	 * {@link UpdateOptions}.
	 *
	 * @param objectToUpdate must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return the update builder.
	 */
	public StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update(Object objectToUpdate,
			WriteOptions options, CassandraPersistentEntity<?> entity, CqlIdentifier tableName) {

		Assert.notNull(tableName, "TableName must not be null");
		Assert.notNull(objectToUpdate, "Object to builder must not be null");
		Assert.notNull(options, "WriteOptions must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		Where where = new Where();
		cassandraConverter.write(objectToUpdate, where, entity);

		Map<CqlIdentifier, Object> object = new LinkedHashMap<>();
		cassandraConverter.write(objectToUpdate, object, entity);
		where.forEach((cqlIdentifier, o) -> object.remove(cqlIdentifier));

		StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> builder = StatementBuilder
				.of(QueryBuilder.update(getKeyspace(entity, tableName), tableName).set().where(),
						cassandraConverter.getCodecRegistry())
				.bind((statement, factory) -> {

					CqlStatementOptionsAccessor<UpdateStart> accessor = factory.ifBoundOrInline(
							bindings -> CqlStatementOptionsAccessor.ofUpdate(bindings, (UpdateStart) statement),
							() -> CqlStatementOptionsAccessor.ofUpdate((UpdateStart) statement));
					com.datastax.oss.driver.api.querybuilder.update.Update statementToUse = addUpdateOptions(accessor, options);

					return ((UpdateWithAssignments) statementToUse).set(toAssignments(object, factory))
							.where(toRelations(where, factory));
				});

		Optional.of(options).filter(UpdateOptions.class::isInstance).map(UpdateOptions.class::cast)
				.map(UpdateOptions::getIfCondition)
				.ifPresent(criteriaDefinitions -> applyUpdateIfCondition(builder, criteriaDefinitions));

		builder.transform(statement -> QueryOptionsUtil.addQueryOptions(statement, options));

		return builder;
	}

	/**
	 * Create an {@literal DELETE} statement by mapping {@code id} to {@literal SELECT … WHERE} considering
	 * {@link UpdateOptions}.
	 *
	 * @param id must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return the delete builder.
	 */
	public StatementBuilder<Delete> deleteById(Object id, CassandraPersistentEntity<?> entity, CqlIdentifier tableName) {

		Where where = new Where();

		cassandraConverter.write(id, where, entity);

		return StatementBuilder
				.of(QueryBuilder.deleteFrom(getKeyspace(entity, tableName), tableName).where(),
						cassandraConverter.getCodecRegistry())
				.bind((statement, factory) -> statement.where(toRelations(where, factory)));
	}

	/**
	 * Create a {@literal DELETE} statement by mapping {@link Query} to {@link Delete}.
	 *
	 * @param query must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return the delete builder.
	 */
	public StatementBuilder<Delete> delete(Query query, CassandraPersistentEntity<?> entity) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		return delete(query, entity, entity.getTableName());
	}

	/**
	 * Create a {@literal DELETE} statement by mapping {@link Query} to {@link Delete}.
	 *
	 * @param query must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return the delete builder.
	 * @see 2.1
	 */
	public StatementBuilder<Delete> delete(Query query, CassandraPersistentEntity<?> entity, CqlIdentifier tableName) {

		Assert.notNull(query, "Query must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");
		Assert.notNull(tableName, "Table name must not be null");

		Filter filter = getQueryMapper().getMappedObject(query, entity);
		List<CqlIdentifier> columnNames = getQueryMapper().getMappedColumnNames(query.getColumns(), entity);

		StatementBuilder<Delete> builder = delete(columnNames, entity, tableName, filter,
				query.getQueryOptions().filter(WriteOptions.class::isInstance).map(WriteOptions.class::cast));

		query.getQueryOptions().filter(DeleteOptions.class::isInstance).map(DeleteOptions.class::cast)
				.map(DeleteOptions::getIfCondition)
				.ifPresent(criteriaDefinitions -> applyDeleteIfCondition(builder, criteriaDefinitions));

		query.getQueryOptions()
				.ifPresent(options -> builder.transform(statement -> QueryOptionsUtil.addQueryOptions(statement, options)));

		return builder;
	}

	/**
	 * Create an {@literal DELETE} statement by mapping {@code entity} to {@link Delete DELETE … WHERE} considering
	 * {@link WriteOptions}.
	 *
	 * @param entity must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 * @param entityWriter must not be {@literal null}.
	 * @param tableName must not be {@literal null}.
	 * @return the delete builder.
	 */
	public StatementBuilder<Delete> delete(Object entity, QueryOptions options, EntityWriter<Object, Object> entityWriter,
			CqlIdentifier tableName) {

		Assert.notNull(tableName, "TableName must not be null");
		Assert.notNull(entity, "Object to builder must not be null");
		Assert.notNull(entityWriter, "EntityWriter must not be null");

		Where where = new Where();
		entityWriter.write(entity, where);
		BasicCassandraPersistentEntity<?> persistentEntity = cassandraConverter.getMappingContext()
				.getRequiredPersistentEntity(ProxyUtils.getUserClass(entity.getClass()));

		StatementBuilder<Delete> builder = StatementBuilder
				.of(QueryBuilder.deleteFrom(getKeyspace(persistentEntity, tableName), tableName).where(),
						cassandraConverter.getCodecRegistry())
				.bind((statement, factory) -> {

					Delete statementToUse;
					if (options instanceof WriteOptions wo) {

						CqlStatementOptionsAccessor<DeleteSelection> accessor = factory.ifBoundOrInline(
								bindings -> CqlStatementOptionsAccessor.ofDelete(bindings, (DeleteSelection) statement),
								() -> CqlStatementOptionsAccessor.ofDelete((DeleteSelection) statement));
						statementToUse = addDeleteOptions(accessor, wo);
					} else {
						statementToUse = statement;
					}

					return statementToUse.where(toRelations(where, factory));
				});

		Optional.of(options).filter(DeleteOptions.class::isInstance).map(DeleteOptions.class::cast)
				.map(DeleteOptions::getIfCondition)
				.ifPresent(criteriaDefinitions -> applyDeleteIfCondition(builder, criteriaDefinitions));

		builder.transform(statement -> QueryOptionsUtil.addQueryOptions(statement, options));

		return builder;
	}

	/**
	 * Compute the {@link Columns} to include type if the {@code returnType} is a {@literal DTO projection} or a
	 * {@literal closed interface projection}.
	 *
	 * @param columns must not be {@literal null}.
	 * @param domainType must not be {@literal null}.
	 * @param returnType must not be {@literal null}.
	 * @return {@link Columns} with columns to be included.
	 * @since 2.2
	 */
	Columns computeColumnsForProjection(EntityProjection<?, ?> projection, Columns columns,
			CassandraPersistentEntity<?> domainType, Class<?> returnType) {

		if (!columns.isEmpty() || ClassUtils.isAssignable(domainType.getType(), returnType)) {
			return columns;
		}

		if (projection.getMappedType().getType().isInterface()) {
			projection.forEach(propertyPath -> columns.include(propertyPath.getPropertyPath().getSegment()));
		} else {

			// DTO projections use merged metadata between domain type and result type
			PersistentPropertyTranslator translator = PersistentPropertyTranslator.create(domainType,
					Predicates.negate(CassandraPersistentProperty::hasExplicitColumnName));

			CassandraPersistentEntity<?> entity = getQueryMapper().getConverter().getMappingContext()
					.getRequiredPersistentEntity(projection.getMappedType());
			for (CassandraPersistentProperty property : entity) {
				columns.include(translator.translate(property).getColumnName());
			}
		}

		Columns projectedColumns = Columns.empty();

		if (returnType.isInterface()) {

			ProjectionInformation projectionInformation = cassandraConverter.getProjectionFactory()
					.getProjectionInformation(returnType);

			if (projectionInformation.isClosed()) {

				for (PropertyDescriptor inputProperty : projectionInformation.getInputProperties()) {
					projectedColumns = projectedColumns.include(inputProperty.getName());
				}
			}
		} else {
			for (PersistentProperty<?> property : domainType) {
				projectedColumns = projectedColumns.include(property.getName());
			}
		}

		return projectedColumns;
	}

	private StatementBuilder<Select> createSelect(Query query, CassandraPersistentEntity<?> entity, Filter filter,
			List<Selector> selectors, CqlIdentifier tableName) {

		Sort sort = Optional.of(query.getSort()).map(querySort -> getQueryMapper().getMappedSort(querySort, entity))
				.orElse(Sort.unsorted());

		StatementBuilder<Select> select = createSelectAndOrder(selectors, entity, tableName, filter, sort);

		if (query.isAllowFiltering()) {
			select.apply(Select::allowFiltering);
		}

		select.onBuild(statementBuilder -> query.getPagingState().ifPresent(statementBuilder::setPagingState));

		if (query.getLimit() > 0) {

			int limit = Math.toIntExact(query.getLimit());
			select.bind((statement, factory) -> factory.ifBoundOrInline(bindings -> statement.limit(bindings.bind(limit)),
					() -> statement.limit(limit)));
		}

		query.getQueryOptions()
				.ifPresent(it -> select.transform(statement -> QueryOptionsUtil.addQueryOptions(statement, it)));

		return select;
	}

	@Nullable
	private CqlIdentifier getKeyspace(CassandraPersistentEntity<?> entity, CqlIdentifier tableName) {
		return keyspaceProvider.getKeyspace(entity, tableName);
	}

	private StatementBuilder<Select> createSelectAndOrder(List<Selector> selectors, CassandraPersistentEntity<?> entity,
			CqlIdentifier from, Filter filter, Sort sort) {

		Select select;

		if (selectors.isEmpty()) {
			select = QueryBuilder.selectFrom(getKeyspace(entity, from), from).all();
		} else {

			List<com.datastax.oss.driver.api.querybuilder.select.Selector> mappedSelectors = new ArrayList<>(
					selectors.size());
			for (Selector selector : selectors) {
				com.datastax.oss.driver.api.querybuilder.select.Selector orElseGet = selector.getAlias()
						.map(it -> getSelection(selector).as(it)).orElseGet(() -> getSelection(selector));
				mappedSelectors.add(orElseGet);
			}

			select = QueryBuilder.selectFrom(getKeyspace(entity, from), from).selectors(mappedSelectors);
		}

		StatementBuilder<Select> builder = StatementBuilder.of(select, cassandraConverter.getCodecRegistry());

		builder.bind((statement, factory) -> {
			return statement.where(getRelations(filter, factory));
		});

		if (sort.isSorted()) {

			builder.apply((statement) -> {

				Select statementToUse = statement;

				for (Sort.Order order : sort) {
					statementToUse = statementToUse.orderBy(order.getProperty(),
							order.isAscending() ? ClusteringOrder.ASC : ClusteringOrder.DESC);
				}

				return statementToUse;
			});
		}

		return builder;
	}

	private static List<Relation> getRelations(Filter filter, TermFactory factory) {
		List<Relation> relations = new ArrayList<>();
		for (CriteriaDefinition criteriaDefinition : filter) {
			relations.add(toClause(criteriaDefinition, factory));
		}
		return relations;
	}

	private static com.datastax.oss.driver.api.querybuilder.select.Selector getSelection(Selector selector) {

		if (selector instanceof FunctionCall) {

			com.datastax.oss.driver.api.querybuilder.select.Selector[] arguments = ((FunctionCall) selector).getParameters()
					.stream().map(param -> {

						if (param instanceof ColumnSelector) {

							return com.datastax.oss.driver.api.querybuilder.select.Selector
									.column(((ColumnSelector) param).getExpression());
						}
						return new SimpleSelector(param.toString());

					}).toArray(com.datastax.oss.driver.api.querybuilder.select.Selector[]::new);

			return com.datastax.oss.driver.api.querybuilder.select.Selector.function(selector.getExpression(), arguments);
		}

		return com.datastax.oss.driver.api.querybuilder.select.Selector
				.column(CqlIdentifier.fromInternal(selector.getExpression()));
	}

	private StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update(
			CassandraPersistentEntity<?> entity, CqlIdentifier table, Update mappedUpdate, Filter filter,
			Optional<WriteOptions> optionalOptions) {

		UpdateStart updateStart = QueryBuilder.update(getKeyspace(entity, table), table);

		return StatementBuilder
				.of((com.datastax.oss.driver.api.querybuilder.update.Update) updateStart, cassandraConverter.getCodecRegistry())
				.bind((statement, factory) -> {

					com.datastax.oss.driver.api.querybuilder.update.Update statementToUse;
					WriteOptions options = optionalOptions.orElse(null);
					if (options != null) {
						CqlStatementOptionsAccessor<UpdateStart> accessor = factory.ifBoundOrInline(
								bindings -> CqlStatementOptionsAccessor.ofUpdate(bindings, (UpdateStart) statement),
								() -> CqlStatementOptionsAccessor.ofUpdate((UpdateStart) statement));
						statementToUse = addUpdateOptions(accessor, options);
					} else {
						statementToUse = statement;
					}

					List<Assignment> assignments = mappedUpdate.getUpdateOperations().stream()
							.map(assignmentOp -> getAssignment(assignmentOp, factory)).collect(Collectors.toList());

					return (com.datastax.oss.driver.api.querybuilder.update.Update) ((OngoingAssignment) statementToUse)
							.set(assignments);

				}).bind((statement, factory) -> {
					return statement.where(getRelations(filter, factory));
				});
	}

	static Iterable<Relation> toRelations(Where where, TermFactory factory) {

		List<Relation> relations = new ArrayList<>();

		where.forEach((cqlIdentifier, termValue) -> relations
				.add(Relation.column(cqlIdentifier).isEqualTo(factory.create(termValue))));

		return relations;
	}

	static Iterable<Assignment> toAssignments(Map<CqlIdentifier, Object> object, TermFactory factory) {

		List<Assignment> assignments = new ArrayList<>();

		object.forEach(
				(cqlIdentifier, termValue) -> assignments.add(Assignment.setColumn(cqlIdentifier, factory.create(termValue))));

		return assignments;
	}

	private static void applyUpdateIfCondition(
			StatementBuilder<com.datastax.oss.driver.api.querybuilder.update.Update> update, Filter criteriaDefinitions) {

		update.bind((statement, factory) -> {

			List<Condition> conditions = criteriaDefinitions.stream().map(it -> toCondition(it, factory))
					.collect(Collectors.toList());

			return statement.if_(conditions);
		});
	}

	private static void applyDeleteIfCondition(
			StatementBuilder<com.datastax.oss.driver.api.querybuilder.delete.Delete> delete, Filter criteriaDefinitions) {
		delete.bind((statement, factory) -> {

			List<Condition> conditions = criteriaDefinitions.stream().map(it -> toCondition(it, factory))
					.collect(Collectors.toList());
			return statement.if_(conditions);
		});
	}

	private static Assignment getAssignment(AssignmentOp assignmentOp, TermFactory termFactory) {

		if (assignmentOp instanceof SetOp) {
			return getAssignment((SetOp) assignmentOp, termFactory);
		}

		if (assignmentOp instanceof RemoveOp) {
			return getAssignment((RemoveOp) assignmentOp, termFactory);
		}

		if (assignmentOp instanceof IncrOp) {
			return getAssignment((IncrOp) assignmentOp, termFactory);
		}

		if (assignmentOp instanceof AddToOp) {
			return getAssignment((AddToOp) assignmentOp, termFactory);
		}

		if (assignmentOp instanceof AddToMapOp) {
			return getAssignment((AddToMapOp) assignmentOp, termFactory);
		}

		throw new IllegalArgumentException(String.format("UpdateOp %s not supported", assignmentOp));
	}

	private static Assignment getAssignment(IncrOp incrOp, TermFactory termFactory) {

		return incrOp.getValue().longValue() > 0
				? Assignment.increment(incrOp.toCqlIdentifier(), termFactory.create(Math.abs(incrOp.getValue().longValue())))
				: Assignment.decrement(incrOp.toCqlIdentifier(), termFactory.create(Math.abs(incrOp.getValue().longValue())));
	}

	private static Assignment getAssignment(SetOp updateOp, TermFactory termFactory) {

		if (updateOp instanceof SetAtIndexOp) {

			return Assignment.setListValue(updateOp.toCqlIdentifier(),
					termFactory.create(((SetAtIndexOp) updateOp).getIndex()), termFactory.create(updateOp.getValue()));
		}

		if (updateOp instanceof SetAtKeyOp) {
			SetAtKeyOp op = (SetAtKeyOp) updateOp;
			return Assignment.setMapValue(op.toCqlIdentifier(), termFactory.create(op.getKey()),
					termFactory.create(op.getValue()));
		}

		return Assignment.setColumn(updateOp.toCqlIdentifier(), termFactory.create(updateOp.getValue()));
	}

	@SuppressWarnings("unchecked")
	private static Assignment getAssignment(RemoveOp removeOp, TermFactory termFactory) {

		if (removeOp.getValue() instanceof Set) {

			Collection<Object> collection = (Collection<Object>) removeOp.getValue();

			return new RemoveCollectionElementsAssignment(removeOp.toCqlIdentifier(), termFactory.create(collection));
		}

		if (removeOp.getValue() instanceof List) {

			Collection<Object> collection = (Collection<Object>) removeOp.getValue();

			return new RemoveCollectionElementsAssignment(removeOp.toCqlIdentifier(), termFactory.create(collection));
		}

		return Assignment.remove(removeOp.toCqlIdentifier(), termFactory.create(removeOp.getValue()));
	}

	private static Assignment getAssignment(AddToOp updateOp, TermFactory termFactory) {

		if (updateOp.getValue() instanceof Set) {
			return Assignment.append(updateOp.toCqlIdentifier(), termFactory.create(updateOp.getValue()));
		}

		return Mode.PREPEND.equals(updateOp.getMode())
				? Assignment.prepend(updateOp.toCqlIdentifier(), termFactory.create(updateOp.getValue()))
				: Assignment.append(updateOp.getColumnName().toCql(), termFactory.create(updateOp.getValue()));
	}

	private static Assignment getAssignment(AddToMapOp updateOp, TermFactory termFactory) {
		return Assignment.append(updateOp.toCqlIdentifier(), termFactory.create(updateOp.getValue()));
	}

	private StatementBuilder<Delete> delete(List<CqlIdentifier> columnNames, CassandraPersistentEntity<?> entity,
			CqlIdentifier from, Filter filter, Optional<WriteOptions> optionsOptional) {

		DeleteSelection select = QueryBuilder.deleteFrom(getKeyspace(entity, from), from);

		for (CqlIdentifier columnName : columnNames) {
			select = select.column(columnName);
		}

		return StatementBuilder.of(select.where(), cassandraConverter.getCodecRegistry()).bind((statement, factory) -> {

			WriteOptions options = optionsOptional.orElse(null);
			Delete statementToUse;
			if (options != null) {
				CqlStatementOptionsAccessor<DeleteSelection> accessor = factory.ifBoundOrInline(
						bindings -> CqlStatementOptionsAccessor.ofDelete(bindings, (DeleteSelection) statement),
						() -> CqlStatementOptionsAccessor.ofDelete((DeleteSelection) statement));
				statementToUse = addDeleteOptions(accessor, options);
			} else {
				statementToUse = statement;
			}

			return statementToUse.where(getRelations(filter, factory));
		});
	}

	/**
	 * Add common {@link WriteOptions} options to {@link Insert} CQL statements.
	 *
	 * @param insert {@link Insert} CQL statement, must not be {@literal null}.
	 * @param writeOptions write options (e.g. consistency level) to add to the CQL statement.
	 * @return the given {@link Insert}.
	 * @since 2.1
	 */
	static Insert addInsertOptions(CqlStatementOptionsAccessor<Insert> insert, WriteOptions writeOptions) {

		Assert.notNull(insert, "Insert must not be null");

		Insert insertToUse = QueryOptionsUtil.addWriteOptions(insert, writeOptions);

		if (writeOptions instanceof InsertOptions insertOptions) {

			if (insertOptions.isIfNotExists()) {
				insertToUse = insertToUse.ifNotExists();
			}
		}

		return insertToUse;
	}

	/**
	 * Add common {@link WriteOptions} options to {@link com.datastax.oss.driver.api.querybuilder.update.Update} CQL
	 * statements.
	 *
	 * @param update {@link com.datastax.oss.driver.api.querybuilder.update.Update} CQL statement, must not be
	 *          {@literal null}.
	 * @param writeOptions write options (e.g. consistency level) to add to the CQL statement.
	 * @return the given {@link com.datastax.oss.driver.api.querybuilder.update.Update}.
	 * @see QueryOptionsUtil#addWriteOptions(com.datastax.oss.driver.api.querybuilder.update.Update, WriteOptions)
	 * @since 2.1
	 */
	static com.datastax.oss.driver.api.querybuilder.update.Update addUpdateOptions(
			CqlStatementOptionsAccessor<UpdateStart> update, WriteOptions writeOptions) {

		Assert.notNull(update, "Update must not be null");

		com.datastax.oss.driver.api.querybuilder.update.Update updateToUse = (com.datastax.oss.driver.api.querybuilder.update.Update) QueryOptionsUtil
				.addWriteOptions(update, writeOptions);

		if (writeOptions instanceof UpdateOptions updateOptions) {

			if (updateOptions.isIfExists()) {
				updateToUse = updateToUse.ifExists();
			}
		}

		return updateToUse;
	}

	/**
	 * Add common {@link WriteOptions} options to {@link Delete} CQL statements.
	 *
	 * @param delete {@link Delete} CQL statement, must not be {@literal null}.
	 * @param writeOptions write options (e.g. consistency level) to add to the CQL statement.
	 * @return the given {@link Delete}.
	 * @since 2.1
	 */
	static Delete addDeleteOptions(CqlStatementOptionsAccessor<DeleteSelection> delete, WriteOptions writeOptions) {

		Assert.notNull(delete, "Delete must not be null");

		Delete deleteToUse = (Delete) QueryOptionsUtil.addWriteOptions(delete, writeOptions);

		if (writeOptions instanceof DeleteOptions deleteOptions) {

			if (deleteOptions.isIfExists()) {
				deleteToUse = deleteToUse.where().ifExists();
			}
		}

		return deleteToUse;
	}

	private static Relation toClause(CriteriaDefinition criteriaDefinition, TermFactory factory) {

		CqlIdentifier columnName = criteriaDefinition.getColumnName().getCqlIdentifier()
				.orElseGet(() -> CqlIdentifier.fromInternal(criteriaDefinition.getColumnName().toCql()));

		Predicate predicate = criteriaDefinition.getPredicate();

		CriteriaDefinition.Operators predicateOperator = CriteriaDefinition.Operators
				.from(predicate.getOperator().toString()).orElseThrow(
						() -> new IllegalArgumentException(String.format("Unknown operator [%s]", predicate.getOperator())));

		ColumnRelationBuilder<Relation> column = Relation.column(columnName);
		Object value = predicate.getValue();

		switch (predicateOperator) {

			case EQ:
				return column.isEqualTo(factory.create(value));

			case NE:
				return column.isNotEqualTo(factory.create(value));

			case GT:
				return column.isGreaterThan(factory.create(value));

			case GTE:
				return column.isGreaterThanOrEqualTo(factory.create(value));

			case LT:
				return column.isLessThan(factory.create(value));

			case LTE:
				return column.isLessThanOrEqualTo(factory.create(value));

			case IN:

				if (isCollectionLike(value)) {

					if (factory.canBindCollection()) {
						Term term = factory.create(value);
						return term instanceof BindMarker ? column.in((BindMarker) term) : column.in(term);
					}

					return column.in(toLiterals(value));
				}

				return column.in(factory.create(value));

			case LIKE:
				return column.like(factory.create(value));

			case IS_NOT_NULL:
				return column.isNotNull();

			case CONTAINS:

				Assert.state(value != null, () -> String.format("CONTAINS value for column %s is null", columnName));

				return column.contains(factory.create(value));

			case CONTAINS_KEY:

				Assert.state(value != null, () -> String.format("CONTAINS KEY value for column %s is null", columnName));

				return column.containsKey(factory.create(value));
		}

		throw new IllegalArgumentException(
				String.format("Criteria %s %s %s not supported", columnName, predicate.getOperator(), value));
	}

	private static Condition toCondition(CriteriaDefinition criteriaDefinition, TermFactory factory) {

		String columnName = criteriaDefinition.getColumnName().toCql();

		Predicate predicate = criteriaDefinition.getPredicate();

		CriteriaDefinition.Operators predicateOperator = CriteriaDefinition.Operators
				.from(predicate.getOperator().toString()).orElseThrow(
						() -> new IllegalArgumentException(String.format("Unknown operator [%s]", predicate.getOperator())));

		ConditionBuilder<Condition> column = Condition.column(columnName);
		Object value = predicate.getValue();

		switch (predicateOperator) {

			case EQ:
				return column.isEqualTo(factory.create(value));

			case NE:
				return column.isNotEqualTo(factory.create(value));

			case GT:
				return column.isGreaterThan(factory.create(value));

			case GTE:
				return column.isGreaterThanOrEqualTo(factory.create(value));

			case LT:
				return column.isLessThan(factory.create(value));

			case LTE:
				return column.isLessThanOrEqualTo(factory.create(value));

			case IN:

				if (isCollectionLike(value)) {

					if (factory.canBindCollection()) {
						Term term = factory.create(value);
						return term instanceof BindMarker ? column.in((BindMarker) term) : column.in(term);
					}

					return column.in(toLiterals(value));
				}

				return column.in(factory.create(value));
		}

		throw new IllegalArgumentException(
				String.format("Criteria %s %s %s not supported for IF Conditions", columnName, predicate.getOperator(), value));
	}

	static List<Term> toLiterals(@Nullable Object arrayOrList) {
		return toLiterals(arrayOrList, QueryBuilder::literal);
	}

	static List<Term> toLiterals(@Nullable Object arrayOrList, Function<Object, Term> termFactory) {

		if (arrayOrList instanceof List) {

			List<?> list = (List<?>) arrayOrList;
			List<Term> literals = new ArrayList<>(list.size());
			for (Object o : list) {
				literals.add(termFactory.apply(o));
			}

			return literals;
		}

		if (arrayOrList != null && arrayOrList.getClass().isArray()) {

			Object[] array = (Object[]) arrayOrList;
			List<Term> literals = new ArrayList<>(array.length);
			for (Object o : array) {
				literals.add(termFactory.apply(o));
			}

			return literals;
		}

		return Collections.emptyList();
	}

	private static boolean isCollectionLike(@Nullable Object value) {
		return value instanceof List || (value != null && value.getClass().isArray());
	}

	static class SimpleSelector implements com.datastax.oss.driver.api.querybuilder.select.Selector {

		private final String selector;

		SimpleSelector(String selector) {
			this.selector = selector;
		}

		@NonNull
		@Override
		public com.datastax.oss.driver.api.querybuilder.select.Selector as(@NonNull CqlIdentifier alias) {
			throw new UnsupportedOperationException();
		}

		@Nullable
		@Override
		public CqlIdentifier getAlias() {
			return null;
		}

		@Override
		public void appendTo(@NonNull StringBuilder builder) {
			builder.append(selector);
		}
	}

	private static class RemoveCollectionElementsAssignment implements Assignment {

		private final CqlIdentifier columnId;
		private final Term value;

		protected RemoveCollectionElementsAssignment(CqlIdentifier columnId, Term value) {
			this.columnId = columnId;
			this.value = value;
		}

		@Override
		public void appendTo(StringBuilder builder) {
			builder.append(String.format("%1$s=%1$s-%2$s", columnId.asCql(true), buildRightOperand()));
		}

		private String buildRightOperand() {
			StringBuilder builder = new StringBuilder();
			value.appendTo(builder);
			return builder.toString();
		}

		@Override
		public boolean isIdempotent() {
			return value.isIdempotent();
		}

		public Term getValue() {
			return value;
		}

	}

	/**
	 * Function interface to determine a {@link CqlIdentifier keyspace} for a given {@link CassandraPersistentEntity} and
	 * {@code tableName}. Classes implementing this interface can choose to return a keyspace or {@code null} to use the
	 * default keyspace.
	 *
	 * @since 4.4
	 */
	@FunctionalInterface
	public interface KeyspaceProvider {

		/**
		 * Determine a {@link CqlIdentifier keyspace} for a given {@link CassandraPersistentEntity} and {@code tableName}.
		 *
		 * @param entity the persistent entity for which the operation is applied.
		 * @param tableName the table of the operation.
		 * @return a {@link CqlIdentifier keyspace} to use a dedicated keyspace for a
		 *         {@link com.datastax.oss.driver.api.core.cql.Statement} or {@code null} to use the default session
		 *         keyspace.
		 */
		@Nullable
		CqlIdentifier getKeyspace(CassandraPersistentEntity<?> entity, CqlIdentifier tableName);

	}

	/**
	 * Implementations of {@link KeyspaceProvider}.
	 */
	enum KeyspaceProviders implements KeyspaceProvider {

		/**
		 * Derive the keyspace from the given {@link CassandraPersistentEntity}.
		 */
		ENTITY_KEYSPACE {
			@Nullable
			@Override
			public CqlIdentifier getKeyspace(CassandraPersistentEntity<?> entity, CqlIdentifier tableName) {
				return entity.getKeyspace();
			}
		},

		/**
		 * Use the session's keyspace.
		 */
		EMPTY_KEYSPACE {
			@Nullable
			@Override
			public CqlIdentifier getKeyspace(CassandraPersistentEntity<?> entity, CqlIdentifier tableName) {
				return null;
			}
		}
	}

}
