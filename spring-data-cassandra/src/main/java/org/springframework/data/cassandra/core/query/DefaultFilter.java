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

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Default implementation of {@link Filter}.
 *
 * @author Mark Paluch
 * @see org.springframework.data.cassandra.core.query.Filter
 * @since 2.0
 */
class DefaultFilter implements Filter {

	private final Iterable<CriteriaDefinition> criteriaDefinitions;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	DefaultFilter(Iterable<? extends CriteriaDefinition> criteriaDefinitions) {
		this.criteriaDefinitions = (Iterable) criteriaDefinitions;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.query.Filter#getCriteriaDefinitions()
	 */
	@Override
	public Iterable<CriteriaDefinition> getCriteriaDefinitions() {
		return criteriaDefinitions;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return StreamSupport.stream(this.spliterator(), false) //
				.map(SerializationUtils::serializeToCqlSafely) //
				.collect(Collectors.joining(" AND "));
	}
}
