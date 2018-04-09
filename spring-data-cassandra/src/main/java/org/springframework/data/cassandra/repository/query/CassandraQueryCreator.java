/*
 * Copyright 2010-2018 the original author or authors.
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.CriteriaDefinition;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.repository.query.ConvertingParameterAccessor.PotentiallyConvertingIterator;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;

import com.datastax.driver.core.querybuilder.Clause;

/**
 * Custom query creator to create Cassandra criteria.
 *
 * @author Matthew Adams
 * @author Mark Paluch
 * @author John Blum
 */
class CassandraQueryCreator extends AbstractQueryCreator<Query, CriteriaDefinition> {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraQueryCreator.class);

	private final MappingContext<?, CassandraPersistentProperty> mappingContext;

	private final QueryBuilder queryBuilder = new QueryBuilder();

	/**
	 * Create a new {@link CassandraQueryCreator} from the given {@link PartTree}, {@link ConvertingParameterAccessor} and
	 * {@link MappingContext}.
	 *
	 * @param tree must not be {@literal null}.
	 * @param parameterAccessor must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	public CassandraQueryCreator(PartTree tree, CassandraParameterAccessor parameterAccessor,
			MappingContext<?, CassandraPersistentProperty> mappingContext) {

		super(tree, parameterAccessor);

		Assert.notNull(mappingContext, "CassandraMappingContext must not be null");

		this.mappingContext = mappingContext;
	}

	/**
	 * Returns the {@link MappingContext} used by this template to access mapping meta-data used to store (map) object to
	 * Cassandra tables.
	 *
	 * @return the {@link MappingContext} used by this template.
	 * @see CassandraMappingContext
	 */
	protected MappingContext<?, CassandraPersistentProperty> getMappingContext() {
		return this.mappingContext;
	}

	/**
	 * Returns the {@link QueryBuilder} used to construct Cassandra CQL queries.
	 *
	 * @return the {@link QueryBuilder} used to construct Cassandra CQL queries.
	 */
	protected QueryBuilder getQueryBuilder() {
		return this.queryBuilder;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#create(org.springframework.data.repository.query.parser.Part, java.util.Iterator)
	 */
	@Override
	protected CriteriaDefinition create(Part part, Iterator<Object> iterator) {

		PersistentPropertyPath<CassandraPersistentProperty> path = getMappingContext()
				.getPersistentPropertyPath(part.getProperty());

		CassandraPersistentProperty property = path.getLeafProperty();

		Assert.state(property != null && path.toDotPath() != null, "Leaf property must not be null");

		return from(part, property, Criteria.where(path.toDotPath()), (PotentiallyConvertingIterator) iterator);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#and(org.springframework.data.repository.query.parser.Part, java.lang.Object, java.util.Iterator)
	 */
	@Override
	protected CriteriaDefinition and(Part part, CriteriaDefinition base, Iterator<Object> iterator) {

		getQueryBuilder().and(base);

		return create(part, iterator);
	}

	/*
	 * Cassandra does not support OR queries.
	 *
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#or(java.lang.Object, java.lang.Object)
	 */
	@Override
	protected CriteriaDefinition or(CriteriaDefinition base, CriteriaDefinition criteria) {
		throw new InvalidDataAccessApiUsageException("Cassandra does not support an OR operator");
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.parser.AbstractQueryCreator#complete(java.lang.Object, org.springframework.data.domain.Sort)
	 */
	@Override
	protected Query complete(CriteriaDefinition criteria, Sort sort) {

		if (criteria != null) {
			getQueryBuilder().and(criteria);
		}

		Query query = getQueryBuilder().create(sort);

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Created query [%s]", query));
		}

		return query;
	}

	private CriteriaDefinition from(Part part, CassandraPersistentProperty property, Criteria where,
			PotentiallyConvertingIterator parameters) {

		Type type = part.getType();

		switch (type) {
			case AFTER:
			case GREATER_THAN:
				return where.gt(parameters.nextConverted(property));
			case GREATER_THAN_EQUAL:
				return where.gte(parameters.nextConverted(property));
			case BEFORE:
			case LESS_THAN:
				return where.lt(parameters.nextConverted(property));
			case LESS_THAN_EQUAL:
				return where.lte(parameters.nextConverted(property));
			case IN:
				return where.in(nextAsArray(property, parameters));
			case LIKE:
			case STARTING_WITH:
			case ENDING_WITH:
				return where.like(like(type, parameters.nextConverted(property)));
			case CONTAINING:
				return containing(where, property, parameters.nextConverted(property));
			case TRUE:
				return where.is(true);
			case FALSE:
				return where.is(false);
			case SIMPLE_PROPERTY:
				return where.is(parameters.nextConverted(property));
			default:
				throw new InvalidDataAccessApiUsageException(
						String.format("Unsupported keyword [%s] in part [%s]", type, part));
		}
	}

	private CriteriaDefinition containing(Criteria where, CassandraPersistentProperty property, Object bindableValue) {

		if (property.isCollectionLike() || property.isMapLike()) {
			return where.contains(bindableValue);
		}

		return where.like(like(Type.CONTAINING, bindableValue));
	}

	private Object like(Type type, Object value) {

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
	static class QueryBuilder {

		private List<CriteriaDefinition> criterias = new ArrayList<>();

		CriteriaDefinition and(CriteriaDefinition clause) {
			criterias.add(clause);
			return clause;
		}

		Query create(Sort sort) {

			Query query = Query.query(criterias);

			return query.sort(sort);
		}
	}
}
