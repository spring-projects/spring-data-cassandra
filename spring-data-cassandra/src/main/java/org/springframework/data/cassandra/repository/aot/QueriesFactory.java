/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.cassandra.repository.aot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import org.jspecify.annotations.Nullable;

import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.data.cassandra.core.StatementFactory;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.convert.UpdateMapper;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.query.CriteriaDefinition;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.repository.config.CassandraRepositoryConfigurationExtension;
import org.springframework.data.cassandra.repository.query.CassandraQueryMethod;
import org.springframework.data.cassandra.repository.query.ParameterBinding;
import org.springframework.data.cassandra.repository.query.StringBasedQuery;
import org.springframework.data.domain.Sort;
import org.springframework.data.repository.config.PropertiesBasedNamedQueriesFactoryBean;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.support.PropertiesBasedNamedQueries;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.StringUtils;

/**
 * Factory for {@link AotQueries}.
 *
 * @author Mark Paluch
 * @since 5.0
 */
class QueriesFactory {

	private final NamedQueries namedQueries;
	private final ValueExpressionDelegate delegate;
	private final CassandraMappingContext mappingContext;
	private final StatementFactory statementFactory;

	public QueriesFactory(RepositoryConfigurationSource configurationSource, ClassLoader classLoader,
			ValueExpressionDelegate delegate, CassandraMappingContext mappingContext) {

		this.namedQueries = getNamedQueries(configurationSource, classLoader);
		this.delegate = delegate;
		this.mappingContext = mappingContext;
		UpdateMapper updateMapper = new UpdateMapper(new MappingCassandraConverter(mappingContext)) {
			@Override
			protected @Nullable Object getMappedValue(Field field, CriteriaDefinition.Operator operator, Object value) {
				return value;
			}
		};
		this.statementFactory = new StatementFactory(updateMapper);
	}

	private NamedQueries getNamedQueries(RepositoryConfigurationSource configSource, ClassLoader classLoader) {

		String location = configSource.getNamedQueryLocation().orElse(null);

		if (location == null) {
			location = new CassandraRepositoryConfigurationExtension().getDefaultNamedQueryLocation();
		}

		if (StringUtils.hasText(location)) {

			try {

				PathMatchingResourcePatternResolver resolver = new PathMatchingResourcePatternResolver(classLoader);

				PropertiesBasedNamedQueriesFactoryBean factoryBean = new PropertiesBasedNamedQueriesFactoryBean();
				factoryBean.setLocations(resolver.getResources(location));
				factoryBean.afterPropertiesSet();
				return Objects.requireNonNull(factoryBean.getObject());
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		return new PropertiesBasedNamedQueries(new Properties());
	}

	/**
	 * Creates the {@link AotQueries} used within a specific {@link CassandraQueryMethod}.
	 *
	 * @param repositoryInformation
	 * @param returnedType
	 * @param query
	 * @param queryMethod
	 * @return
	 */
	public AotQuery createQuery(RepositoryInformation repositoryInformation, ReturnedType returnedType,
			MergedAnnotation<Query> query, CassandraQueryMethod queryMethod) {

		boolean count = query.isPresent() && query.getBoolean("count");
		boolean exists = query.isPresent() && query.getBoolean("exists");

		if (query.isPresent() && StringUtils.hasText(query.getString("value"))) {
			return buildStringQuery(query.getString("value"), queryMethod, count, exists);
		}

		String queryName = queryMethod.getNamedQueryName();
		if (namedQueries.hasQuery(queryName)) {
			return buildNamedQuery(queryName, queryMethod, count, exists);
		}

		return buildPartTreeQuery(repositoryInformation, returnedType, queryMethod);
	}

	private AotQuery buildStringQuery(String queryString, CassandraQueryMethod queryMethod, boolean count,
			boolean exists) {

		StringBasedQuery query = parseQuery(queryMethod, queryString);
		List<ParameterBinding> bindings = query.getQueryParameterBindings();

		return StringAotQuery.of(query.getPostProcessedQuery(), bindings, count, exists);
	}

	private AotQuery buildNamedQuery(String queryName, CassandraQueryMethod queryMethod, boolean count,
			boolean exists) {

		String queryString = namedQueries.getQuery(queryName);
		StringBasedQuery query = parseQuery(queryMethod, queryString);
		List<ParameterBinding> bindings = query.getQueryParameterBindings();

		return StringAotQuery.named(queryName, query.getPostProcessedQuery(), bindings, count, exists);
	}

	private StringBasedQuery parseQuery(CassandraQueryMethod queryMethod, String queryString) {
		return new StringBasedQuery(queryString, queryMethod.getParameters(), delegate);
	}

	private AotQuery buildPartTreeQuery(RepositoryInformation repositoryInformation, ReturnedType returnedType,
			CassandraQueryMethod queryMethod) {

		PartTree partTree = new PartTree(queryMethod.getName(), repositoryInformation.getDomainType());

		List<ParameterBinding> parameterBindings = new ArrayList<>();
		AotQueryCreator queryCreator = new AotQueryCreator(partTree, queryMethod.getParameters(), mappingContext,
				parameterBindings);

		BasicCassandraPersistentEntity<?> entity = mappingContext
				.getRequiredPersistentEntity(repositoryInformation.getDomainType());

		org.springframework.data.cassandra.core.query.Query query = queryCreator.createQuery(Sort.unsorted());

		String queryString;

		if (partTree.isDelete()) {
			queryString = statementFactory.delete(query, entity).build().getQuery();
		} else if (partTree.isCountProjection() || partTree.isExistsProjection()) {
			queryString = statementFactory.count(query, entity).build().getQuery();
		} else {
			queryString = statementFactory.select(query, entity).build().getQuery();
		}

		return new DerivedAotQuery(queryString, parameterBindings, query, partTree);
	}

}
