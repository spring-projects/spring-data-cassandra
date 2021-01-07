/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.query;

import java.util.Arrays;
import java.util.Iterator;

import org.springframework.data.util.Streamable;
import org.springframework.util.Assert;

/**
 * Filter consisting of {@link CriteriaDefinition}s to be used with {@literal SELECT}, {@literal UPDATE} and
 * {@literal DELETE} queries. A {@link Filter} describes the matched set of rows to execute a particular operation.
 *
 * @author Mark Paluch
 * @since 2.0
 */
@FunctionalInterface
public interface Filter extends Streamable<CriteriaDefinition> {

	/**
	 * @return the {@link CriteriaDefinition}s.
	 */
	Iterable<CriteriaDefinition> getCriteriaDefinitions();

	/**
	 * Create a simple {@link Filter} given {@link CriteriaDefinition}s.
	 *
	 * @param criteriaDefinitions must not be {@literal null}.
	 * @return the {@link Filter} object for {@link CriteriaDefinition}s.
	 */
	static Filter from(CriteriaDefinition... criteriaDefinitions) {

		Assert.notNull(criteriaDefinitions, "CriteriaDefinitions must not be null");

		return from(Arrays.asList(criteriaDefinitions));
	}

	/**
	 * Create a simple {@link Filter} given {@link CriteriaDefinition}s.
	 *
	 * @param criteriaDefinitions must not be {@literal null}.
	 * @return the {@link Filter} object for {@link CriteriaDefinition}s.
	 */
	static Filter from(Iterable<? extends CriteriaDefinition> criteriaDefinitions) {

		Assert.notNull(criteriaDefinitions, "CriteriaDefinitions must not be null");

		return new DefaultFilter(criteriaDefinitions);
	}

	/* (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	default Iterator<CriteriaDefinition> iterator() {
		return getCriteriaDefinitions().iterator();
	}
}
