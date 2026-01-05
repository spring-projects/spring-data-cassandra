/*
 * Copyright 2010-present the original author or authors.
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
package org.springframework.data.cassandra.repository.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.CriteriaDefinition;
import org.springframework.data.cassandra.core.query.Filter;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.VectorSort;
import org.springframework.data.core.PropertyPath;
import org.springframework.data.domain.Range;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Vector;
import org.springframework.data.mapping.PersistentPropertyPath;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;

/**
 * Custom query creator to create Cassandra criteria.
 * <p>
 * Only intended for internal use.
 *
 * @author Matthew Adams
 * @author Mark Paluch
 * @author John Blum
 * @author Chris Bono
 */
public class CassandraQueryCreator extends AbstractQueryCreator<Query, Filter> {

	private static final Log LOG = LogFactory.getLog(CassandraQueryCreator.class);

	private final MappingContext<?, CassandraPersistentProperty> mappingContext;

	private final QueryBuilder queryBuilder = new QueryBuilder();
	private final CassandraParameterAccessor parameterAccessor;
	private final PartTree tree;

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

		this.tree = tree;
		this.parameterAccessor = parameterAccessor;
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

	@Override
	protected Filter create(Part part, Iterator<Object> iterator) {

		PersistentPropertyPath<CassandraPersistentProperty> path = getMappingContext()
				.getPersistentPropertyPath(part.getProperty());

		CassandraPersistentProperty property = path.getLeafProperty();
		Object filterOrCriteria = from(part, property, Criteria.where(path.toDotPath()), iterator);

		if (filterOrCriteria instanceof CriteriaDefinition) {
			return Filter.from((CriteriaDefinition) filterOrCriteria);
		}

		return (Filter) filterOrCriteria;
	}

	@Override
	protected Filter and(Part part, @Nullable Filter base, Iterator<Object> iterator) {

		if (base != null) {
			for (CriteriaDefinition criterion : base) {
				getQueryBuilder().and(criterion);
			}
		}

		return create(part, iterator);
	}

	@Override
	protected Filter or(Filter base, Filter criteria) {
		throw new InvalidDataAccessApiUsageException("Cassandra does not support an OR operator");
	}

	@Override
	protected Query complete(@Nullable Filter criteria, Sort sort) {

		if (criteria != null) {

			for (CriteriaDefinition criterion : criteria) {
				getQueryBuilder().and(criterion);
			}
		}

		Query query = sort.isUnsorted() && parameterAccessor.getVector() != null ? getQueryBuilder().create(getVectorSort())
				: getQueryBuilder().create(sort);

		if (LOG.isDebugEnabled()) {
			LOG.debug(String.format("Created query [%s]", query));
		}

		return query;
	}

	private Sort getVectorSort() {
		return VectorSort.ann(getVectorProperty().toDotPath(), parameterAccessor.getVector());
	}

	PropertyPath getVectorProperty() {

		for (PartTree.OrPart parts : tree) {
			for (Part part : parts) {

				if (part.getType() == Type.NEAR || part.getType() == Type.WITHIN) {
					return part.getProperty();
				}
			}
		}

		throw new IllegalArgumentException("No Near/Within property found");
	}

	/**
	 * Returns a {@link Filter} or {@link CriteriaDefinition} object representing the criterion for a {@link Part}.
	 */
	private @Nullable Object from(Part part, CassandraPersistentProperty property, Criteria where,
			Iterator<Object> parameters) {

		Type type = part.getType();

		switch (type) {
			case AFTER:
			case GREATER_THAN:
				return where.gt(parameters.next());
			case GREATER_THAN_EQUAL:
				return where.gte(parameters.next());
			case BEFORE:
			case LESS_THAN:
				return where.lt(parameters.next());
			case LESS_THAN_EQUAL:
				return where.lte(parameters.next());
			case BETWEEN:
				return computeBetweenPart(where, parameters);
			case IN:
				return where.in(nextAsArray(parameters));
			case LIKE:
			case STARTING_WITH:
			case ENDING_WITH:
				return where.like(like(type, parameters.next()));
			case CONTAINING:
				return containing(where, property, parameters.next());
			case TRUE:
				return where.is(true);
			case FALSE:
				return where.is(false);
			case SIMPLE_PROPERTY:
				return where.is(parameters.next());

			case NEAR:
			case WITHIN:

				Object next = parameters.next();

				if (!(next instanceof Vector)) {

					throw new IllegalArgumentException("Expected a Vector for Near/Within keyword but got [%s]"
							.formatted(next == null ? "null" : next.getClass()));
				}

				return null;
			default:
				throw new InvalidDataAccessApiUsageException(
						String.format("Unsupported keyword [%s] in part [%s]", type, part));
		}

	}

	/**
	 * Compute a {@link Type#BETWEEN} {@link Part}.
	 * <p>
	 * In case the first {@literal value} is actually a {@link Range} the lower and upper bounds of the {@link Range} are
	 * used according to their {@link Range.Bound#isInclusive() inclusion} definition. Otherwise, the {@literal value} is
	 * used for greater than and {@link Iterator#next() parameters.next()} as less than criterions.
	 *
	 * @param where must not be {@literal null}.
	 * @param parameters must not be {@literal null}.
	 * @return
	 * @since 2.2
	 */
	private static Filter computeBetweenPart(Criteria where, Iterator<Object> parameters) {

		Object value = parameters.next();
		if (!(value instanceof Range)) {
			return Filter.from(Criteria.where(where.getColumnName()).gt(value),
					Criteria.where(where.getColumnName()).lt(parameters.next()));
		}

		Range<?> range = (Range<?>) value;
		List<CriteriaDefinition> criteria = new ArrayList<>();
		Optional<?> min = range.getLowerBound().getValue();
		Optional<?> max = range.getUpperBound().getValue();

		min.ifPresent(it -> {

			if (range.getLowerBound().isInclusive()) {
				criteria.add(Criteria.where(where.getColumnName()).gte(it));
			} else {
				criteria.add(Criteria.where(where.getColumnName()).gt(it));
			}
		});

		max.ifPresent(it -> {

			if (range.getUpperBound().isInclusive()) {
				criteria.add(Criteria.where(where.getColumnName()).lte(it));
			} else {
				criteria.add(Criteria.where(where.getColumnName()).lt(it));
			}
		});

		return Filter.from(criteria);
	}

	private CriteriaDefinition containing(Criteria where, CassandraPersistentProperty property, Object bindableValue) {

		if (property.isCollectionLike() || property.isMapLike()) {
			return where.contains(bindableValue);
		}

		return where.like(like(Type.CONTAINING, bindableValue));
	}

	protected Object like(Type type, Object value) {

		return switch (type) {
			case LIKE -> value;
			case CONTAINING -> "%" + value + "%";
			case STARTING_WITH -> value + "%";
			case ENDING_WITH -> "%" + value;
			default ->
				throw new IllegalArgumentException(String.format("Part Type [%s] not supported with like queries", type));
		};

	}

	private Object[] nextAsArray(Iterator<Object> iterator) {

		Object next = iterator.next();

		if (next instanceof Collection) {
			return ((Collection<?>) next).toArray();
		} else if (next.getClass().isArray()) {
			return (Object[]) next;
		}

		return new Object[] { next };
	}

	/**
	 * Where clause builder. Collects {@link CriteriaDefinition clauses} and builds the where-clause depending on the
	 * WHERE type.
	 *
	 * @author Mark Paluch
	 */
	static class QueryBuilder {

		private final List<CriteriaDefinition> criterias = new ArrayList<>();

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
