/*
 * Copyright 2017-2020 the original author or authors.
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
package org.springframework.data.cassandra.core.convert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.EmbeddedEntityOperations;
import org.springframework.data.cassandra.core.query.ColumnName;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Columns.ColumnSelector;
import org.springframework.data.cassandra.core.query.Columns.FunctionCall;
import org.springframework.data.cassandra.core.query.Columns.Selector;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.CriteriaDefinition;
import org.springframework.data.cassandra.core.query.CriteriaDefinition.Predicate;
import org.springframework.data.cassandra.core.query.Filter;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Map {@link org.springframework.data.cassandra.core.query.Query} to CQL-specific data types.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @see ColumnName
 * @see Columns
 * @see Criteria
 * @see Filter
 * @see Sort
 * @since 2.0
 */
public class QueryMapper {

	private final CassandraConverter converter;

	private final CassandraMappingContext mappingContext;

	/**
	 * Creates a new {@link QueryMapper} with the given {@link CassandraConverter}.
	 *
	 * @param converter must not be {@literal null}.
	 */
	public QueryMapper(CassandraConverter converter) {

		Assert.notNull(converter, "CassandraConverter must not be null");

		this.converter = converter;
		this.mappingContext = converter.getMappingContext();
	}

	/**
	 * Returns the configured {@link CassandraConverter} used to convert object values into Cassandra column typed values.
	 *
	 * @return the configured {@link CassandraConverter}.
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter
	 */
	public CassandraConverter getConverter() {
		return this.converter;
	}

	/**
	 * Returns the configured {@link MappingContext} containing mapping meta-data (persistent entities and properties)
	 * used to store (map) objects to Cassandra tables (rows/columns).
	 *
	 * @return the configured {@link MappingContext}.
	 */
	protected CassandraMappingContext getMappingContext() {
		return this.mappingContext;
	}

	/**
	 * Map a {@link Filter} with a {@link CassandraPersistentEntity type hint}. Filter mapping translates property names
	 * to column names and maps {@link Predicate} values to simple Cassandra values.
	 *
	 * @param filter must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return the mapped {@link Filter}.
	 */
	public Filter getMappedObject(Filter filter, CassandraPersistentEntity<?> entity) {

		Assert.notNull(filter, "Filter must not be null");
		Assert.notNull(entity, "Entity must not be null");

		List<CriteriaDefinition> result = new ArrayList<>();

		for (CriteriaDefinition criteriaDefinition : filter) {

			Field field = createPropertyField(entity, criteriaDefinition.getColumnName());

			field.getProperty().filter(CassandraPersistentProperty::isCompositePrimaryKey).ifPresent(it -> {
				throw new IllegalArgumentException(
						"Cannot use composite primary key directly. Reference a property of the composite primary key");
			});

			field.getProperty().filter(it -> it.getOrdinal() != null).ifPresent(it -> {
				throw new IllegalArgumentException(
						String.format("Cannot reference tuple value elements, property [%s]", field.getMappedKey()));
			});

			Predicate predicate = criteriaDefinition.getPredicate();

			Object value = predicate.getValue();
			ColumnType typeDescriptor = getColumnType(field, value, predicate.getOperator());

			Object mappedValue = value != null ? getConverter().convertToColumnType(value, typeDescriptor) : null;

			Predicate mappedPredicate = new Predicate(predicate.getOperator(), mappedValue);

			result.add(Criteria.of(field.getMappedKey(), mappedPredicate));
		}

		return Filter.from(result);
	}

	/**
	 * Map {@link Columns} with a {@link CassandraPersistentEntity type hint} to {@link ColumnSelector}s.
	 *
	 * @param columns must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return the mapped {@link Selector}s.
	 */
	public List<Selector> getMappedSelectors(Columns columns, CassandraPersistentEntity<?> entity) {

		Assert.notNull(columns, "Columns must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		if (columns.isEmpty()) {
			return Collections.emptyList();
		}

		List<Selector> selectors = new ArrayList<>();

		for (ColumnName column : columns) {

			Field field = createPropertyField(entity, column);

			columns.getSelector(column).ifPresent(selector -> getCqlIdentifier(column, field)
					.ifPresent(cqlIdentifier -> selectors.add(getMappedSelector(selector, cqlIdentifier))));
		}

		if (columns.isEmpty()) {
			addColumns(entity, selectors);
		}

		return selectors;
	}

	private void addColumns(CassandraPersistentEntity<?> entity, List<Selector> selectors) {

		entity.doWithProperties((PropertyHandler<CassandraPersistentProperty>) property -> {

			if (property.isCompositePrimaryKey()) {

				CassandraPersistentEntity<?> primaryKeyEntity = mappingContext.getRequiredPersistentEntity(property);
				addColumns(primaryKeyEntity, selectors);
			} else {
				selectors.add(ColumnSelector.from(property.getRequiredColumnName()));
			}
		});
	}

	private Selector getMappedSelector(Selector selector, CqlIdentifier cqlIdentifier) {

		if (selector instanceof ColumnSelector) {

			ColumnSelector columnSelector = (ColumnSelector) selector;

			ColumnSelector mappedColumnSelector = ColumnSelector.from(cqlIdentifier);

			return columnSelector.getAlias().map(mappedColumnSelector::as).orElse(mappedColumnSelector);
		}

		if (selector instanceof FunctionCall) {

			FunctionCall functionCall = (FunctionCall) selector;

			List<Object> mappedParameters = functionCall.getParameters().stream().map(obj -> {

				if (obj instanceof Selector) {
					return getMappedSelector((Selector) obj, cqlIdentifier);
				}

				return obj;
			}) //
					.collect(Collectors.toList());

			FunctionCall mappedFunctionCall = FunctionCall.from(functionCall.getExpression(), mappedParameters.toArray());

			return functionCall.getAlias() //
					.map(mappedFunctionCall::as) //
					.orElse(mappedFunctionCall);
		}

		throw new IllegalArgumentException(String.format("Selector [%s] not supported", selector));
	}

	/**
	 * Map {@link Columns} with a {@link CassandraPersistentEntity type hint} to column names for included columns.
	 * Function call selectors or other {@link org.springframework.data.cassandra.core.query.Columns.Selector} types are
	 * not included.
	 *
	 * @param columns must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return the mapped column names.
	 */
	public List<CqlIdentifier> getMappedColumnNames(Columns columns, CassandraPersistentEntity<?> entity) {

		Assert.notNull(columns, "Columns must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		if (columns.isEmpty()) {
			return Collections.emptyList();
		}

		List<CqlIdentifier> columnNames = new ArrayList<>();

		Set<PersistentProperty<?>> seen = new HashSet<>();

		for (ColumnName column : columns) {

			Field field = createPropertyField(entity, column);

			field.getProperty().ifPresent(seen::add);

			columns.getSelector(column).filter(selector -> selector instanceof ColumnSelector)
					.ifPresent(columnSelector -> getCqlIdentifier(column, field).ifPresent(columnNames::add));
		}

		if (columns.isEmpty()) {

			entity.doWithProperties((PropertyHandler<CassandraPersistentProperty>) property -> {

				if (property.isCompositePrimaryKey()) {
					return;
				}

				if (seen.add(property)) {
					columnNames.add(property.getRequiredColumnName());
				}
			});
		}

		return columnNames;
	}

	public Sort getMappedSort(Sort sort, CassandraPersistentEntity<?> entity) {

		Assert.notNull(sort, "Sort must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		if (!sort.iterator().hasNext()) {
			return sort;
		}

		List<Order> mappedOrders = new ArrayList<>();

		for (Order order : sort) {

			ColumnName columnName = ColumnName.from(order.getProperty());

			Field field = createPropertyField(entity, columnName);

			Order mappedOrder = getCqlIdentifier(columnName, field)
					.map(cqlIdentifier -> new Order(order.getDirection(), cqlIdentifier.toString())).orElse(order);

			mappedOrders.add(mappedOrder);
		}

		return Sort.by(mappedOrders);
	}

	private Optional<CqlIdentifier> getCqlIdentifier(ColumnName column, Field field) {

		try {
			if (field.getProperty().isPresent()) {

				return field.getProperty().map(cassandraPersistentProperty -> {

					if (cassandraPersistentProperty.isCompositePrimaryKey()) {
						throw new IllegalArgumentException(
								"Cannot use composite primary key directly. Reference a property of the composite primary key");
					}

					return cassandraPersistentProperty.getRequiredColumnName();
				});
			}

			if (column.getColumnName().isPresent()) {
				return column.getColumnName().map(CqlIdentifier::fromCql);
			}

			return column.getCqlIdentifier();

		} catch (IllegalStateException cause) {
			throw new IllegalArgumentException(cause.getMessage(), cause);
		}
	}

	Field createPropertyField(@Nullable CassandraPersistentEntity<?> entity, ColumnName key) {

		return Optional.ofNullable(entity).<Field> map(e -> new MetadataBackedField(key, e, getMappingContext()))
				.orElseGet(() -> new Field(key));
	}

	ColumnType getColumnType(Field field, @Nullable Object value, @Nullable CriteriaDefinition.Operator operator) {

		ColumnType typeDescriptor;
		if (field.getProperty().isPresent()) {
			typeDescriptor = converter.getColumnTypeResolver().resolve(field.getProperty().get());
		} else {

			typeDescriptor = converter.getColumnTypeResolver().resolve(value);
		}

		if (field.getProperty().isPresent()) {

			CassandraPersistentProperty property = field.getProperty().get();

			if (property.isCollectionLike()) {
				if (operator == CriteriaDefinition.Operators.CONTAINS) {
					typeDescriptor = typeDescriptor.getRequiredComponentType();
				}
			}

			if (property.isMapLike()) {

				if (operator == CriteriaDefinition.Operators.CONTAINS_KEY) {
					typeDescriptor = typeDescriptor.getRequiredComponentType();
				}

				if (operator == CriteriaDefinition.Operators.CONTAINS) {
					typeDescriptor = typeDescriptor.getRequiredMapValueType();
				}
			}
		}

		if (value instanceof Collection && operator == CriteriaDefinition.Operators.IN) {
			typeDescriptor = ColumnType.listOf(typeDescriptor);
		}

		return typeDescriptor;
	}

	/**
	 * Value object to represent a field and its meta-information.
	 *
	 * @author Mark Paluch
	 */
	protected static class Field {

		protected final ColumnName name;

		/**
		 * Creates a new {@link Field} without meta-information but the given name.
		 *
		 * @param name must not be {@literal null} or empty.
		 */
		Field(ColumnName name) {
			Assert.notNull(name, "Name must not be null!");
			this.name = name;
		}

		/**
		 * Returns a new {@link Field} with the given name.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @return a new {@link Field} with the given name.
		 */
		public Field with(ColumnName name) {
			return new Field(name);
		}

		/**
		 * Returns the underlying {@link CassandraPersistentProperty} backing the field. For path traversals this will be
		 * the property that represents the value to handle. This means it'll be the leaf property for plain paths or the
		 * association property in case we refer to an association somewhere in the path.
		 */
		public Optional<CassandraPersistentProperty> getProperty() {
			return Optional.empty();
		}

		/**
		 * Returns the key to be used in the mapped document eventually.
		 */
		public ColumnName getMappedKey() {
			return name;
		}
	}

	/**
	 * Extension of {@link Field} to be backed with mapping metadata.
	 *
	 * @author Mark Paluch
	 */
	protected static class MetadataBackedField extends Field {

		private final CassandraPersistentEntity<?> entity;

		private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

		private final Optional<PersistentPropertyPath<CassandraPersistentProperty>> path;

		private final @Nullable CassandraPersistentProperty property;

		private final Optional<CassandraPersistentProperty> optionalProperty;

		/**
		 * Creates a new {@link MetadataBackedField} with the given name, {@link CassandraPersistentEntity} and
		 * {@link MappingContext}.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @param entity must not be {@literal null}.
		 * @param mappingContext must not be {@literal null}.
		 */
		public MetadataBackedField(ColumnName name, CassandraPersistentEntity<?> entity,
				MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext) {

			this(name, entity, mappingContext, null);
		}

		/**
		 * Creates a new {@link MetadataBackedField} with the given name, {@link CassandraPersistentProperty} and
		 * {@link MappingContext} with the given {@link CassandraPersistentProperty}.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @param entity must not be {@literal null}.
		 * @param mappingContext must not be {@literal null}.
		 * @param property may be {@literal null}.
		 */
		public MetadataBackedField(ColumnName name, CassandraPersistentEntity<?> entity,
				MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext,
				@Nullable CassandraPersistentProperty property) {

			super(name);

			Assert.notNull(entity, "CassandraPersistentEntity must not be null");

			this.entity = entity;
			this.mappingContext = mappingContext;
			this.path = getPath(name.toCql());
			this.property = path.map(PersistentPropertyPath::getLeafProperty).orElse(property);
			this.optionalProperty = Optional.ofNullable(this.property);
		}

		/**
		 * Returns the {@link PersistentPropertyPath} for the given {@code pathExpression}.
		 *
		 * @param pathExpression {@link String} containing the path expression to evaluate
		 * @return the {@link PersistentPropertyPath} for the given {@code pathExpression}.
		 */
		private Optional<PersistentPropertyPath<CassandraPersistentProperty>> getPath(String pathExpression) {

			try {
				PropertyPath propertyPath = PropertyPath.from(pathExpression.replaceAll("\\.\\d", ""),
						this.entity.getTypeInformation());

				PersistentPropertyPath<CassandraPersistentProperty> persistentPropertyPath = this.mappingContext
						.getPersistentPropertyPath(propertyPath);

				return Optional.of(persistentPropertyPath);
			} catch (PropertyReferenceException e) {
				return Optional.empty();
			}
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.convert.QueryMapper.Field#with(org.springframework.data.cassandra.core.query.ColumnName)
		 */
		@Override
		public MetadataBackedField with(ColumnName name) {
			return new MetadataBackedField(name, entity, mappingContext, property);
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.cassandra.core.convert.QueryMapper.Field#getProperty()
		 */
		@Override
		public Optional<CassandraPersistentProperty> getProperty() {
			return this.optionalProperty;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#getTargetKey()
		 */
		@Override
		public ColumnName getMappedKey() {

			if (!path.isPresent()) {
				return name;
			}

			boolean embedded = false;
			CassandraPersistentEntity<?> parentEntity = null;
			CassandraPersistentProperty leafProperty = null;
			for (CassandraPersistentProperty p : path.get()) {

				leafProperty = p;
				if (embedded) {

					embedded = false;
					leafProperty = parentEntity.getPersistentProperty(p.getName());
					parentEntity = null;
				}
				if (p.isEmbedded()) {
					embedded = true;
					parentEntity = new EmbeddedEntityOperations(mappingContext).getEntity(p);
				}
			}

			return ColumnName.from(leafProperty.getColumnName());
		}
	}
}
