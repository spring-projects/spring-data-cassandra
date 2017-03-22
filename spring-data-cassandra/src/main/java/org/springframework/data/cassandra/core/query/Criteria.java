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
package org.springframework.data.cassandra.core.query;

import static org.springframework.util.ObjectUtils.*;

import java.util.Arrays;
import java.util.Collection;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.util.Assert;

/**
 * Basic class for creating queries. It follows a fluent API style so that you can easily chain together multiple
 * criteria. Static import of the 'Criteria.where' method will improve readability.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class Criteria implements CriteriaDefinition {

	private static final String CONTAINS_KEY = "CONTAINS KEY";

	private ColumnName columnName;

	private CriteriaDefinition.Predicate predicate;

	/**
	 * Create an empty {@link Criteria}.
	 */
	protected Criteria() {}

	/**
	 * Create an empty {@link Criteria}.
	 */
	protected Criteria(ColumnName columnName) {
		this.columnName = columnName;
	}

	public Criteria(ColumnName key, CriteriaDefinition.Predicate predicate) {

		Assert.notNull(key, "Key must not be null");
		Assert.notNull(predicate, "Predicate must not be null");

		this.columnName = key;
		this.predicate = predicate;
	}

	/**
	 * Static factory method to create a {@link Criteria} using the provided {@code columnName}.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link ChainedCriteria} for {@code columnName}.
	 */
	public static Criteria where(String columnName) {
		return new Criteria(ColumnName.from(columnName));
	}

	/**
	 * Create a criterion using equality.
	 *
	 * @param o
	 * @return {@literal this} {@link Criteria} object.
	 */
	public CriteriaDefinition is(Object o) {
		this.predicate = new CriteriaDefinition.Predicate("=", o);
		return this;
	}

	/**
	 * Create a criterion using the {@literal >} operator.
	 *
	 * @param o
	 * @return {@literal this} {@link Criteria} object.
	 */
	public CriteriaDefinition lt(Object o) {

		Assert.notNull(o, "Value must not be null");

		this.predicate = new CriteriaDefinition.Predicate("<", o);
		return this;
	}

	/**
	 * Create a criterion using the {@literal >=} operator.
	 *
	 * @param o
	 * @return {@literal this} {@link Criteria} object.
	 */
	public CriteriaDefinition lte(Object o) {

		Assert.notNull(o, "Value must not be null");

		this.predicate = new CriteriaDefinition.Predicate("<=", o);
		return this;
	}

	/**
	 * Create a criterion using the {@literal <} operator.
	 *
	 * @param o
	 * @return {@literal this} {@link Criteria} object.
	 */
	public CriteriaDefinition gt(Object o) {

		Assert.notNull(o, "Value must not be null");

		this.predicate = new CriteriaDefinition.Predicate(">", o);
		return this;
	}

	/**
	 * Create a criterion using the {@literal <=} operator.
	 *
	 * @param o
	 * @return {@literal this} {@link Criteria} object.
	 */
	public CriteriaDefinition gte(Object o) {

		Assert.notNull(o, "Value must not be null");

		this.predicate = new CriteriaDefinition.Predicate(">=", o);
		return this;
	}

	/**
	 * Create a criterion using the {@literal IN} operator.
	 *
	 * @param o the values to match against
	 * @return {@literal this} {@link Criteria} object.
	 */
	public CriteriaDefinition in(Object... o) {

		Assert.notNull(o, "Value must not be null");

		if (o.length > 1 && o[1] instanceof Collection) {
			throw new InvalidDataAccessApiUsageException(
					"You can only pass in one argument of type " + o[1].getClass().getName());
		}

		return in(Arrays.asList(o));
	}

	/**
	 * Create a criterion using the {@literal IN} operator.
	 *
	 * @param c the collection containing the values to match against
	 * @return {@literal this} {@link Criteria} object.
	 */
	public CriteriaDefinition in(Collection<?> c) {

		Assert.notNull(c, "Value must not be null");

		this.predicate = new CriteriaDefinition.Predicate("IN", c);
		return this;
	}

	/**
	 * Create a criterion using the {@literal LIKE} operator.
	 *
	 * @param c the collection containing the values to match against
	 * @return {@literal this} {@link Criteria} object.
	 */
	public CriteriaDefinition like(Object o) {

		Assert.notNull(o, "Value must not be null");

		this.predicate = new CriteriaDefinition.Predicate("LIKE", o);
		return this;
	}

	/**
	 * Create a criterion using the {@literal CONTAINS} operator.
	 *
	 * @param c the collection containing the values to match against
	 * @return {@literal this} {@link Criteria} object.
	 */
	public CriteriaDefinition contains(Object o) {

		Assert.notNull(o, "Value must not be null");

		this.predicate = new CriteriaDefinition.Predicate("CONTAINS", o);
		return this;
	}

	/**
	 * Create a criterion using the {@literal CONTAINS KEY} operator.
	 *
	 * @param c the collection containing the values to match against
	 * @return {@literal this} {@link Criteria} object.
	 */
	public CriteriaDefinition containsKey(Object o) {

		Assert.notNull(o, "Value must not be null");

		this.predicate = new CriteriaDefinition.Predicate(CONTAINS_KEY, o);
		return this;
	}

	/**
	 * @return the {@link ColumnName}.
	 */
	public ColumnName getColumnName() {
		return this.columnName;
	}

	/**
	 * @return the {@link Predicate}.
	 */
	public CriteriaDefinition.Predicate getPredicate() {
		return predicate;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (this == obj) {
			return true;
		}

		if (obj == null || !(obj instanceof Criteria)) {
			return false;
		}

		Criteria that = (Criteria) obj;

		return simpleCriteriaEquals(this, that);
	}

	protected boolean simpleCriteriaEquals(CriteriaDefinition left, CriteriaDefinition right) {

		boolean keyEqual = left.getColumnName() == null ? right.getColumnName() == null
				: left.getColumnName().equals(right.getColumnName());
		boolean criteriaEqual = left.getPredicate().equals(right.getPredicate());

		return keyEqual && criteriaEqual;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {

		int result = 17;

		result += nullSafeHashCode(columnName);
		result += nullSafeHashCode(predicate);

		return result;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return SerializationUtils.serializeToCqlSafely(this);
	}
}
