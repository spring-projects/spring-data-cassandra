/*
 * Copyright 2010-2013 the original author or authors.
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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.PersistentPropertyPath;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.IgnoreCaseType;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Custom query creator to create Cassandra criteria.
 */
class CassandraQueryCreator extends AbstractQueryCreator<Select, Clause> {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraQueryCreator.class);
	private final CassandraParameterAccessor accessor;

	private final CassandraMappingContext context;

	/**
	 * Creates a new {@link CassandraQueryCreator} from the given {@link PartTree}, {@link ConvertingParameterAccessor}
	 * and {@link MappingContext}.
	 * 
	 * @param tree
	 * @param accessor
	 * @param context
	 */
	public CassandraQueryCreator(PartTree tree, CassandraParameterAccessor accessor, CassandraMappingContext context) {

		super(tree, accessor);

		Assert.notNull(context);

		this.accessor = accessor;
		this.context = context;
	}

	@Override
	protected Clause create(Part part, Iterator<Object> iterator) {

		PersistentPropertyPath<CassandraPersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		CassandraPersistentProperty property = path.getLeafProperty();
		Clause criteria = from(part, property,
				null /* TODO where(path.toDotPath(CassandraPersistentProperty.PropertyToFieldNameConverter.INSTANCE))*/,
				iterator);

		return criteria;
	}

	@Override
	protected Clause and(Part part, Clause base, Iterator<Object> iterator) {

		if (base == null) {
			return create(part, iterator);
		}

		PersistentPropertyPath<CassandraPersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		CassandraPersistentProperty property = path.getLeafProperty();

		return from(part, property,
				null /* TODO base.and(path.toDotPath(CassandraPersistentProperty.PropertyToFieldNameConverter.INSTANCE))*/,
				iterator);
	}

	@Override
	protected Clause or(Clause base, Clause criteria) {
		throw new InvalidDataAccessApiUsageException(String.format("Cassandra does not support an OR operator!"));
	}

	@Override
	protected Select complete(Clause criteria, Sort sort) {

		if (criteria == null) {
			return null;
		}

		Select select = QueryBuilder.select().all().from("TODO");
		select.where(criteria);

		if (LOG.isDebugEnabled()) {
			LOG.debug("Created query " + select.getQueryString());
		}

		return select;
	}

	private Clause from(Part part, CassandraPersistentProperty property, Clause criteria, Iterator<Object> parameters) {

		Type type = part.getType();

		switch (type) {
		// TODO
		// case AFTER:
		// case GREATER_THAN:
		// return criteria.gt(parameters.nextConverted(property));
		// case GREATER_THAN_EQUAL:
		// return criteria.gte(parameters.nextConverted(property));
		// case BEFORE:
		// case LESS_THAN:
		// return criteria.lt(parameters.nextConverted(property));
		// case LESS_THAN_EQUAL:
		// return criteria.lte(parameters.nextConverted(property));
		// case BETWEEN:
		// return criteria.gt(parameters.nextConverted(property)).lt(parameters.nextConverted(property));
		// case IS_NOT_NULL:
		// return criteria.ne(null);
		// case IS_NULL:
		// return criteria.is(null);
		// case NOT_IN:
		// return criteria.nin(nextAsArray(parameters, property));
		// case IN:
		// return criteria.in(nextAsArray(parameters, property));
		// case LIKE:
		// case STARTING_WITH:
		// case ENDING_WITH:
		// case CONTAINING:
		// return addAppropriateLikeRegexTo(criteria, part, parameters.next().toString());
		// case REGEX:
		// return criteria.regex(parameters.next().toString());
		// case EXISTS:
		// return criteria.exists((Boolean) parameters.next());
		// case TRUE:
		// return criteria.is(true);
		// case FALSE:
		// return criteria.is(false);
		// case WITHIN:
		//
		// Object parameter = parameters.next();
		// return criteria.within((Shape) parameter);
		// case SIMPLE_PROPERTY:
		//
		// return isSimpleComparisionPossible(part) ? criteria.is(parameters.nextConverted(property))
		// : createLikeRegexCriteriaOrThrow(part, property, criteria, parameters, false);
		//
		// case NEGATING_SIMPLE_PROPERTY:
		//
		// return isSimpleComparisionPossible(part) ? criteria.ne(parameters.nextConverted(property))
		// : createLikeRegexCriteriaOrThrow(part, property, criteria, parameters, true);
			default:
				throw new UnsupportedCassandraQueryOperatorException(String.format(""));
		}
	}

	private boolean isSimpleComparisionPossible(Part part) {

		switch (part.shouldIgnoreCase()) {
			case NEVER:
				return true;
			case WHEN_POSSIBLE:
				return part.getProperty().getType() != String.class;
			case ALWAYS:
				return false;
			default:
				return true;
		}
	}

	/**
	 * Returns the next element from the given {@link Iterator} expecting it to be of a certain type.
	 * 
	 * @param <T>
	 * @param iterator
	 * @param type
	 * @throws IllegalArgumentException in case the next element in the iterator is not of the given type.
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private <T> T nextAs(Iterator<Object> iterator, Class<T> type) {
		Object parameter = iterator.next();
		if (parameter.getClass().isAssignableFrom(type)) {
			return (T) parameter;
		}

		throw new IllegalArgumentException(String.format("Expected parameter type of %s but got %s!", type,
				parameter.getClass()));
	}

	private Object[] nextAsArray(Iterator<Object> iterator, CassandraPersistentProperty property) {
		Object next = iterator.next(); // TODO nextConverted(property);

		if (next instanceof Collection) {
			return ((Collection<?>) next).toArray();
		} else if (next.getClass().isArray()) {
			return (Object[]) next;
		}

		return new Object[] { next };
	}
}
