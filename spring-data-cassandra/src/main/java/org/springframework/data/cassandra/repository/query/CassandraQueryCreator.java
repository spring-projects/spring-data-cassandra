/*
 * Copyright 2010-2016 the original author or authors.
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
package org.springframework.data.cassandra.repository.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.core.cql.CqlIdentifier;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.repository.query.ConvertingParameterAccessor.PotentiallyConvertingIterator;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.PersistentPropertyPath;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;

/**
 * Custom query creator to create Cassandra criteria.
 *
 * @author Matthew Adams
 * @author Mark Paluch
 * @author John Blum
 */
class CassandraQueryCreator extends AbstractQueryCreator<Select, Clause> {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraQueryCreator.class);
	private static final Pattern PUNCTUATION_PATTERN = Pattern.compile("\\p{Punct}");

	private final CassandraMappingContext mappingContext;
	private final CassandraPersistentEntity<?> entity;
	private final CqlIdentifier tableName;
	private final WhereBuilder whereBuilder = new WhereBuilder();

	/**
	 * Creates a new {@link CassandraQueryCreator} from the given {@link PartTree}, {@link ConvertingParameterAccessor}
	 * and {@link MappingContext}.
	 *
	 * @param tree must not be {@literal null}.
	 * @param accessor must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 * @param entityMetadata must not be {@literal null}.
	 */
	public CassandraQueryCreator(PartTree tree, CassandraParameterAccessor accessor,
			CassandraMappingContext mappingContext, CassandraEntityMetadata<?> entityMetadata) {

		super(tree, accessor);

		Assert.notNull(mappingContext, "CassandraMappingContext must not be null");
		Assert.notNull(entityMetadata, "CassandraEntityMetadata must not be null");

		this.mappingContext = mappingContext;
		this.entity = mappingContext.getPersistentEntity(entityMetadata.getJavaType());
		this.tableName = entityMetadata.getTableName();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#create(org.springframework.data.repository.query.parser.Part, java.util.Iterator)
	 */
	@Override
	protected Clause create(Part part, Iterator<Object> iterator) {

		PersistentPropertyPath<CassandraPersistentProperty> path =
			mappingContext.getPersistentPropertyPath(part.getProperty());

		CassandraPersistentProperty property = path.getLeafProperty();

		return from(part, property, (PotentiallyConvertingIterator) iterator);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#and(org.springframework.data.repository.query.parser.Part, java.lang.Object, java.util.Iterator)
	 */
	@Override
	protected Clause and(Part part, Clause base, Iterator<Object> iterator) {

		if (base == null) {
			return whereBuilder.and(create(part, iterator));
		}

		whereBuilder.and(base);

		return create(part, iterator);
	}

	/*
	 * Cassandra does not support OR queries.
	 *
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#or(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected Clause or(Clause base, Clause criteria) {
		throw new InvalidDataAccessApiUsageException("Cassandra does not support an OR operator");
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#complete(java.lang.Object, org.springframework.data.domain.Sort)
	 */
	@Override
	protected Select complete(Clause criteria, Sort sort) {

		if (criteria != null) {
			whereBuilder.and(criteria);
		}

		Select select = StatementBuilder.select(entity, tableName, whereBuilder, sort);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Created query {}", select);
		}

		return select;
	}

	private Clause from(Part part, CassandraPersistentProperty property, PotentiallyConvertingIterator parameters) {

		Type type = part.getType();

		switch (type) {
			case AFTER:
			case GREATER_THAN:
				return QueryBuilder.gt(columnName(property), parameters.nextConverted(property));
			case GREATER_THAN_EQUAL:
				return QueryBuilder.gte(columnName(property), parameters.nextConverted(property));
			case BEFORE:
			case LESS_THAN:
				return QueryBuilder.lt(columnName(property), parameters.nextConverted(property));
			case LESS_THAN_EQUAL:
				return QueryBuilder.lte(columnName(property), parameters.nextConverted(property));
			case IN:
				return QueryBuilder.in(columnName(property), nextAsArray(property, parameters));
			case LIKE:
			case STARTING_WITH:
			case ENDING_WITH:
				return QueryBuilder.like(columnName(property), like(type, parameters.nextConverted(property)));
			case CONTAINING:
				return containing(property, parameters.nextConverted(property));
			case TRUE:
				return QueryBuilder.eq(columnName(property), true);
			case FALSE:
				return QueryBuilder.eq(columnName(property), false);
			case SIMPLE_PROPERTY:
				return QueryBuilder.eq(columnName(property), parameters.nextConverted(property));
			default:
				throw new InvalidDataAccessApiUsageException(String.format(
					"Unsupported keyword [%s] in part [%s]", type, part));
		}
	}

	private static String columnName(CassandraPersistentProperty property) {
		return property.getColumnName().toCql();
	}

	private Clause containing(CassandraPersistentProperty property, Object bindableValue) {

		if (property.isCollectionLike() || property.isMapLike()) {
			return QueryBuilder.contains(columnName(property), bindableValue);
		}

		return QueryBuilder.like(columnName(property), like(Type.CONTAINING, bindableValue));
	}

	private Object like(Type type, Object value) {

		if (value != null) {
			switch (type) {
				case LIKE:
					return value;
				case CONTAINING:
					return "%" + value + "%";
				case STARTING_WITH:
					return value + "%";
				case ENDING_WITH:
					return "%" + value;
			}

			throw new IllegalArgumentException(String.format("Part Type [%s] not supported with like queries", type));
		}

		return null;
	}

	private Object[] nextAsArray(CassandraPersistentProperty property, PotentiallyConvertingIterator iterator) {

		Object next = iterator.nextConverted(property);

		if (next instanceof Collection) {
			return ((Collection<?>) next).toArray();
		} else if (next.getClass().isArray()) {
			return (Object[]) next;
		}

		return new Object[] { next };
	}

	/**
	 * Where clause builder. Collects {@link Clause clauses} and builds the where-clause depending on the WHERE type.
	 *
	 * @author Mark Paluch
	 */
	static class WhereBuilder {

		private List<Clause> clauses = new ArrayList<Clause>();

		Clause and(Clause clause) {
			clauses.add(clause);
			return clause;
		}

		Select.Where build(Select.Where where) {
			for (Clause clause : clauses) {
				where = where.and(clause);
			}

			return where;
		}
	}

	/**
	 * @author Mark Paluch
	 */
	static class StatementBuilder {

		/**
		 * Build a {@link Select} statement from the given {@link WhereBuilder} and {@link Sort}. Resolves property names
		 * for {@link Sort} using the {@link CassandraPersistentEntity}.
		 */
		static Select select(CassandraPersistentEntity<?> entity, CqlIdentifier tableName, WhereBuilder whereBuilder, Sort sort) {

			Select select = QueryBuilder.select().from(tableName.toCql());

			whereBuilder.build(select.where());

			if (sort != null) {
				for (Order order : sort) {

					String dotPath = order.getProperty();
					CassandraPersistentProperty property = getPersistentProperty(entity, dotPath);

					if (order.isAscending()) {
						select.orderBy(QueryBuilder.asc(columnName(property)));
					} else {
						select.orderBy(QueryBuilder.desc(columnName(property)));
					}
				}
			}

			return select;
		}

		private static CassandraPersistentProperty getPersistentProperty(CassandraPersistentEntity<?> entity,
				String dotPath) {

			String[] segments = PUNCTUATION_PATTERN.split(dotPath);

			CassandraPersistentProperty property = null;
			CassandraPersistentEntity<?> currentEntity = entity;

			for (String segment : segments) {
				property = currentEntity.getPersistentProperty(segment);

				if (property != null && property.isCompositePrimaryKey()) {
					currentEntity = property.getCompositePrimaryKeyEntity();
				}
			}

			if (property != null) {
				return property;
			}

			throw new IllegalArgumentException(String.format(
				"Cannot resolve path [%s] to a property of [%s]", dotPath, entity.getName()));
		}
	}
}
