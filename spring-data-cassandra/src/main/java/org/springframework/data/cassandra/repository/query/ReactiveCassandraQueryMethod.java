/*
 * Copyright 2016-2021 the original author or authors.
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

import java.lang.reflect.Method;

import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.repository.query.CassandraParameters.CassandraParameter;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.util.ReactiveWrapperConverters;
import org.springframework.data.repository.util.ReactiveWrappers;
import org.springframework.data.util.Lazy;

/**
 * Reactive specific implementation of {@link CassandraQueryMethod}.
 *
 * @author Mark Paluch
 * @since 2.0
 */
public class ReactiveCassandraQueryMethod extends CassandraQueryMethod {

	private final Lazy<Boolean> isCollectionQuery;

	/**
	 * Create a new {@link ReactiveCassandraQueryMethod} from the given {@link Method}.
	 *
	 * @param method must not be {@literal null}.
	 * @param metadata must not be {@literal null}.
	 * @param projectionFactory must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	public ReactiveCassandraQueryMethod(Method method, RepositoryMetadata metadata, ProjectionFactory projectionFactory,
			MappingContext<? extends CassandraPersistentEntity<?>, ? extends CassandraPersistentProperty> mappingContext) {

		super(method, metadata, projectionFactory, mappingContext);

		this.isCollectionQuery = Lazy.of(() -> (!(isPageQuery() || isSliceQuery())
				&& ReactiveWrappers.isMultiValueType(metadata.getReturnType(method).getType())) || super.isCollectionQuery());
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#isCollectionQuery()
	 */
	@Override
	public boolean isCollectionQuery() {
		return isCollectionQuery.get();
	}

	/*
	 * All reactive query methods are streaming queries.
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#isStreamQuery()
	 */
	@Override
	public boolean isStreamQuery() {
		return true;
	}

	/**
	 * Check if the given {@link org.springframework.data.repository.query.QueryMethod} receives a reactive parameter
	 * wrapper as one of its parameters.
	 *
	 * @return
	 */
	public boolean hasReactiveWrapperParameter() {

		for (CassandraParameter cassandraParameter : getParameters()) {
			if (ReactiveWrapperConverters.supports(cassandraParameter.getType())) {
				return true;
			}
		}

		return false;
	}
}
