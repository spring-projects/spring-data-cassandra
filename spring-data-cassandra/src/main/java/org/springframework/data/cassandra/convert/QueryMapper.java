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
package org.springframework.data.cassandra.convert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.cassandra.core.query.ColumnName;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Columns.Column;
import org.springframework.data.cassandra.core.query.Columns.Selectors;
import org.springframework.data.cassandra.core.query.Columns.Column;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.CriteriaDefinition;
import org.springframework.data.cassandra.core.query.CriteriaDefinition.Predicate;
import org.springframework.data.cassandra.core.query.Filter;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mapping.PersistentProperty;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.PropertyPath;
import org.springframework.data.mapping.PropertyReferenceException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.PersistentPropertyPath;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

/**
 * Map {@link org.springframework.data.cassandra.core.query.Query} to CQL.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class QueryMapper {

	private final CassandraConverter converter;

	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

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
	 * Map a {@link Filter} with a {@link CassandraPersistentEntity type hint}. Filter mapping translates property names
	 * to column names and maps {@link Predicate} values to simple Cassandra values.
	 *
	 * @param filter must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return the mapped {@link Filter}.
	 */
	public Filter getMappedObject(Filter filter, CassandraPersistentEntity<?> entity) {

		Assert.notNull(filter, "Filter must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		List<CriteriaDefinition> result = new ArrayList<>();

		for (CriteriaDefinition criteriaDefinition : filter) {

			Field field = createPropertyField(entity, criteriaDefinition.getColumnName(), mappingContext);

			Predicate predicate = criteriaDefinition.getPredicate();

			TypeInformation<?> typeInformation = field.getProperty() == null ? null
					: field.getProperty().getTypeInformation();

			Optional<Object> mappedValue = converter.convertToCassandraColumn(Optional.ofNullable(predicate.getValue()),
					typeInformation);

			Predicate mappedPredicate = new Predicate(predicate.getOperator(), mappedValue.orElse(null));

			result.add(new Criteria(field.getMappedKey(), mappedPredicate));
		}

		return Filter.from(result);
	}

	/**
	 * Map {@link Columns} with a {@link CassandraPersistentEntity type hint} to {@link Column}s.
	 *
	 * @param columns must not be {@literal null}.
	 * @param entity must not be {@literal null}.
	 * @return the mapped {@link Selector}s.
	 */
	public List<Column> getMappedSelectors(Columns columns, CassandraPersistentEntity<?> entity) {

		Assert.notNull(columns, "Columns must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		if (columns.isEmpty()) {
			return Collections.emptyList();
		}

		List<Column> selectors = new ArrayList<>();

		Set<PersistentProperty<?>> seen = new HashSet<>();

		for (ColumnName column : columns) {

			Field field = createPropertyField(entity, column, mappingContext);

			if (field.getProperty() != null) {
				seen.add(field.getProperty());
			}

			if (columns.isExcluded(column)) {
				continue;
			}

			columns.getColumnExpression(column).ifPresent(columnExpression -> {
				getCqlIdentifier(column, field).ifPresent(cqlIdentifier -> {
					selectors.add(columnExpression.evaluate(cqlIdentifier));
				});
			});
		}

		entity.doWithProperties((PropertyHandler<CassandraPersistentProperty>) property -> {

			if (seen.add(property)) {

				if (property.isCompositePrimaryKey()) {
					for (CqlIdentifier cqlIdentifier : property.getColumnNames()) {
						selectors.add(Column.of(cqlIdentifier.toCql()));
					}
				} else {
					selectors.add(Column.of(property.getColumnName().toCql()));
				}
			}
		});

		return selectors;
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
	public List<String> getMappedColumnNames(Columns columns, CassandraPersistentEntity<?> entity) {

		Assert.notNull(columns, "Columns must not be null");
		Assert.notNull(entity, "CassandraPersistentEntity must not be null");

		if (columns.isEmpty()) {
			return Collections.emptyList();
		}

		List<String> columnNames = new ArrayList<>();

		Set<PersistentProperty<?>> seen = new HashSet<>();

		for (ColumnName column : columns) {

			Field field = createPropertyField(entity, column, mappingContext);

			if (field.getProperty() != null) {
				seen.add(field.getProperty());
			}

			if (columns.isExcluded(column)) {
				continue;
			}

			columns.getColumnExpression(column) //
					.filter(Selectors.Include::equals) //
					.ifPresent(columnExpression -> {
						getCqlIdentifier(column, field).ifPresent(cqlIdentifier -> {
							columnNames.add(columnExpression.evaluate(cqlIdentifier).getExpression());
						});
					});
		}

		if (columns.hasExclusions()) {
			entity.doWithProperties((PropertyHandler<CassandraPersistentProperty>) property -> {

				if (property.isCompositePrimaryKey()) {
					return;
				}

				if (seen.add(property)) {
					columnNames.add(property.getColumnName().toCql());
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
			Field field = createPropertyField(entity, columnName, mappingContext);

			Order mappedOrder = getCqlIdentifier(columnName, field)
					.map(cqlIdentifier -> new Order(order.getDirection(), cqlIdentifier.toCql())).orElse(order);
			mappedOrders.add(mappedOrder);
		}

		return new Sort(mappedOrders);
	}

	private Optional<CqlIdentifier> getCqlIdentifier(ColumnName column, Field field) {

		try {

			if (field.getProperty() != null) {
				return Optional.of(field.getProperty().getColumnName());
			}

			if (column.getColumnName().isPresent()) {
				return column.getColumnName().map(CqlIdentifier::cqlId);
			}

			return column.getCqlIdentifier();

		} catch (IllegalStateException e) {
			throw new IllegalArgumentException(e.getMessage(), e);
		}
	}

	/**
	 * @param entity
	 * @param key
	 * @param mappingContext
	 * @return
	 */
	protected Field createPropertyField(CassandraPersistentEntity<?> entity, ColumnName key,
			MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext) {
		return entity == null ? new Field(key) : new MetadataBackedField(key, entity, mappingContext);
	}

	/**
	 * Value object to represent a field and its meta-information.
	 *
	 * @author Mark Paluch
	 */
	protected static class Field {

		protected final ColumnName name;

		/**
		 * Creates a new {@link DocumentField} without meta-information but the given name.
		 *
		 * @param name must not be {@literal null} or empty.
		 */
		public Field(ColumnName name) {

			Assert.notNull(name, "Name must not be null!");
			this.name = name;
		}

		/**
		 * Returns a new {@link DocumentField} with the given name.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @return
		 */
		public Field with(ColumnName name) {
			return new Field(name);
		}

		/**
		 * Returns the underlying {@link MongoPersistentProperty} backing the field. For path traversals this will be the
		 * property that represents the value to handle. This means it'll be the leaf property for plain paths or the
		 * association property in case we refer to an association somewhere in the path.
		 *
		 * @return
		 */
		public CassandraPersistentProperty getProperty() {
			return null;
		}

		/**
		 * Returns the key to be used in the mapped document eventually.
		 *
		 * @return
		 */
		public ColumnName getMappedKey() {
			return name;
		}

		public TypeInformation<?> getTypeHint() {
			return ClassTypeInformation.OBJECT;
		}
	}

	/**
	 * Extension of {@link DocumentField} to be backed with mapping metadata.
	 *
	 * @author Mark Paluch
	 */
	protected static class MetadataBackedField extends Field {

		private final CassandraPersistentEntity<?> entity;
		private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;
		private final CassandraPersistentProperty property;
		private final PersistentPropertyPath<CassandraPersistentProperty> path;

		/**
		 * Creates a new {@link MetadataBackedField} with the given name, {@link MongoPersistentEntity} and
		 * {@link MappingContext}.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @param entity must not be {@literal null}.
		 * @param context must not be {@literal null}.
		 */
		public MetadataBackedField(ColumnName name, CassandraPersistentEntity<?> entity,
				MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> context) {
			this(name, entity, context, null);
		}

		/**
		 * Creates a new {@link MetadataBackedField} with the given name, {@link MongoPersistentEntity} and
		 * {@link MappingContext} with the given {@link MongoPersistentProperty}.
		 *
		 * @param name must not be {@literal null} or empty.
		 * @param entity must not be {@literal null}.
		 * @param context must not be {@literal null}.
		 * @param property may be {@literal null}.
		 */
		public MetadataBackedField(ColumnName name, CassandraPersistentEntity<?> entity,
				MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> context,
				CassandraPersistentProperty property) {

			super(name);

			Assert.notNull(entity, "MongoPersistentEntity must not be null!");

			this.entity = entity;
			this.mappingContext = context;
			this.path = getPath(
					name.getColumnName().isPresent() ? name.getColumnName().get() : name.getCqlIdentifier().get().toCql());
			this.property = property;
		}

		/**
		 * Returns the {@link PersistentPropertyPath} for the given {@code pathExpression}.
		 *
		 * @param pathExpression
		 * @return
		 */
		private PersistentPropertyPath<CassandraPersistentProperty> getPath(String pathExpression) {

			try {
				PropertyPath path = PropertyPath.from(pathExpression.replaceAll("\\.\\d", ""), entity.getTypeInformation());
				PersistentPropertyPath<CassandraPersistentProperty> propertyPath = mappingContext
						.getPersistentPropertyPath(path);

				return propertyPath;
			} catch (PropertyReferenceException e) {
				return null;
			}
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#with(java.lang.String)
		 */
		@Override
		public MetadataBackedField with(ColumnName name) {
			return new MetadataBackedField(name, entity, mappingContext, property);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#getProperty()
		 */
		@Override
		public CassandraPersistentProperty getProperty() {
			return path == null ? property : path.getLeafProperty();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.Field#getTargetKey()
		 */
		@Override
		public ColumnName getMappedKey() {
			return path == null ? name : ColumnName.from(path.getLeafProperty().getColumnName());
		}
	}
}
