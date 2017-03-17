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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.springframework.util.Assert;

/**
 * Central class for creating queries. It follows a fluent API style so that you can easily chain together multiple
 * criteria. Static import of the 'ChainedCriteria.where' method will improve readability.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class ChainedCriteria extends Criteria implements Filter {

	private final List<CriteriaDefinition> criteriaChain;

	private ChainedCriteria(ColumnName key) {

		super(key);

		this.criteriaChain = new ArrayList<>();
		this.criteriaChain.add(this);
	}

	private ChainedCriteria(List<CriteriaDefinition> criteriaChain, ColumnName key) {

		super(key);

		this.criteriaChain = criteriaChain;
		this.criteriaChain.add(this);
	}

	private ChainedCriteria(List<CriteriaDefinition> criteriaChain, ColumnName key,
			CriteriaDefinition.Predicate predicate) {

		super(key, predicate);

		this.criteriaChain = criteriaChain;
		this.criteriaChain.add(this);
	}

	/**
	 * Static factory method to create a {@link ChainedCriteria} using the provided key
	 *
	 * @param key
	 * @return a new {@link ChainedCriteria} for {@code columnName}.
	 */
	public static ChainedCriteria where(String key) {
		return new ChainedCriteria(ColumnName.from(key));
	}

	/**
	 * Create a new {@link ChainedCriteria} given {@code criteriaDefinitions}.
	 *
	 * @param criteriaDefinitions must not be {@literal null} or empty.
	 * @return a new {@link ChainedCriteria} for {@code criteriaDefinitions}.
	 */
	public static ChainedCriteria from(CriteriaDefinition... criteriaDefinitions) {

		Assert.notNull(criteriaDefinitions, "CriteriaDefinitions must not be null");
		Assert.notEmpty(criteriaDefinitions, "CriteriaDefinitions must not be empty");

		return from(Arrays.asList(criteriaDefinitions));
	}

	/**
	 * Create a new {@link ChainedCriteria} given {@code criteriaDefinitions}.
	 *
	 * @param criteriaDefinitions must not be {@literal null} or empty.
	 * @return a new {@link ChainedCriteria} for {@code criteriaDefinitions}.
	 */
	public static ChainedCriteria from(Iterable<? extends CriteriaDefinition> criteriaDefinitions) {

		Assert.notNull(criteriaDefinitions, "CriteriaDefinitions must not be null");

		Iterator<? extends CriteriaDefinition> iterator = criteriaDefinitions.iterator();
		Assert.isTrue(iterator.hasNext(), "Criteria chain must not be empty");

		List<CriteriaDefinition> chain = new ArrayList<>();
		Set<CriteriaDefinition> seen = new HashSet<>();

		while (iterator.hasNext()) {

			CriteriaDefinition criteriaDefinition = iterator.next();

			if (criteriaDefinition instanceof ChainedCriteria) {
				ChainedCriteria chainedCriteria = (ChainedCriteria) criteriaDefinition;

				for (CriteriaDefinition nested : chainedCriteria.criteriaChain) {

					if (!seen.add(nested)) {
						continue;
					}

					new ChainedCriteria(chain, nested.getColumnName(), nested.getPredicate());
				}
			}

			if (!seen.add(criteriaDefinition)) {
				continue;
			}

			new ChainedCriteria(chain, criteriaDefinition.getColumnName(), criteriaDefinition.getPredicate());
		}

		return (ChainedCriteria) chain.get(chain.size() - 1);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.query.Criteria#is(java.lang.Object)
	 */
	@Override
	public ChainedCriteria is(Object o) {
		super.is(o);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.query.Criteria#lt(java.lang.Object)
	 */
	@Override
	public ChainedCriteria lt(Object o) {
		super.lt(o);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.query.Criteria#lte(java.lang.Object)
	 */
	@Override
	public ChainedCriteria lte(Object o) {
		super.lte(o);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.query.Criteria#gt(java.lang.Object)
	 */
	@Override
	public ChainedCriteria gt(Object o) {
		super.gt(o);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.query.Criteria#gte(java.lang.Object)
	 */
	@Override
	public ChainedCriteria gte(Object o) {
		super.gte(o);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.query.Criteria#in(java.lang.Object[])
	 */
	@Override
	public ChainedCriteria in(Object... o) {
		super.in(o);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.query.Criteria#in(java.util.Collection)
	 */
	@Override
	public ChainedCriteria in(Collection<?> c) {
		super.in(c);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.query.Criteria#like(java.lang.Object)
	 */
	@Override
	public ChainedCriteria like(Object o) {
		super.like(o);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.query.Criteria#contains(java.lang.Object)
	 */
	@Override
	public ChainedCriteria contains(Object o) {
		super.contains(o);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.query.Criteria#containsKey(java.lang.Object)
	 */
	@Override
	public ChainedCriteria containsKey(Object o) {
		super.containsKey(o);
		return this;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.query.Filter#getCriteriaDefinitions()
	 */
	@Override
	public List<CriteriaDefinition> getCriteriaDefinitions() {
		return criteriaChain;
	}

	/**
	 * Continue {@link Criteria} chaining using for a {@code columnName}. This method is used to speficy additional
	 * criterias.
	 *
	 * @param columnName must not be {@literal null}.
	 * @return a new {@link ChainedCriteria} holding the criteria chain and beginning a predicate for {@code columnName}.
	 */
	public ChainedCriteria and(String columnName) {

		List<CriteriaDefinition> chain = new ArrayList<>(this.criteriaChain.size() + 1);
		chain.addAll(this.criteriaChain);

		return new ChainedCriteria(chain, ColumnName.from(columnName));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.query.Criteria#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {

		if (!super.equals(obj)) {
			return false;
		}

		if (obj instanceof ChainedCriteria) {

			ChainedCriteria that = (ChainedCriteria) obj;

			if (this.criteriaChain.size() != that.criteriaChain.size()) {
				return false;
			}

			for (int i = 0; i < this.criteriaChain.size(); i++) {

				CriteriaDefinition left = this.criteriaChain.get(i);
				CriteriaDefinition right = that.criteriaChain.get(i);

				if (!simpleCriteriaEquals(left, right)) {
					return false;
				}
			}
		}

		return true;
	}
}
