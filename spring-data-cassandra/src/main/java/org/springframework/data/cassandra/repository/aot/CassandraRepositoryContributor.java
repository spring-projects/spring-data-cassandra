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

import java.lang.reflect.Method;
import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.annotation.MergedAnnotations;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.convert.CassandraCustomConversions;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.repository.query.CassandraQueryMethod;
import org.springframework.data.repository.aot.generate.AotRepositoryClassBuilder;
import org.springframework.data.repository.aot.generate.AotRepositoryConstructorBuilder;
import org.springframework.data.repository.aot.generate.MethodContributor;
import org.springframework.data.repository.aot.generate.RepositoryContributor;
import org.springframework.data.repository.config.AotRepositoryContext;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.TypeName;
import org.springframework.util.StringUtils;

/**
 * Cassandra specific {@link RepositoryContributor}.
 *
 * @author Chris Bono
 * @author Mark Paluch
 * @since 5.0
 */
public class CassandraRepositoryContributor extends RepositoryContributor {

	private final AotRepositoryContext context;
	private final CassandraMappingContext mappingContext;
	private final CassandraCustomConversions customConversions;
	private final QueriesFactory queryFactory;

	public CassandraRepositoryContributor(AotRepositoryContext context) {

		super(context);
		this.context = context;
		this.mappingContext = new CassandraMappingContext();
		this.customConversions = new CassandraCustomConversions(List.of());
		this.queryFactory = new QueriesFactory(context.getConfigurationSource(), context.getRequiredClassLoader(),
				ValueExpressionDelegate.create(), mappingContext);
	}

	@Override
	protected void customizeClass(AotRepositoryClassBuilder builder) {
		builder.customize(b -> b.superclass(TypeName.get(AotRepositoryFragmentSupport.class)));
	}

	@Override
	protected void customizeConstructor(AotRepositoryConstructorBuilder constructorBuilder) {

		constructorBuilder.addParameter("operations", CassandraOperations.class, customizer -> {

			String cassandraTemplateRef = getCassandraTemplateRef();
			customizer.bindToField()
					.origin(StringUtils.hasText(cassandraTemplateRef)
							? new RuntimeBeanReference(cassandraTemplateRef, CassandraOperations.class)
							: new RuntimeBeanReference(CassandraOperations.class));
		});

		constructorBuilder.addParameter("context", RepositoryFactoryBeanSupport.FragmentCreationContext.class, false);
	}

	private @Nullable String getCassandraTemplateRef() {
		return context.getConfigurationSource().getAttribute("cassandraTemplateRef").orElse(null);
	}

	@Override
	@SuppressWarnings("NullAway")
	protected @Nullable MethodContributor<? extends QueryMethod> contributeQueryMethod(Method method) {

		CassandraQueryMethod queryMethod = new CassandraQueryMethod(method, getRepositoryInformation(),
				getProjectionFactory(), mappingContext);

		ReturnedType returnedType = queryMethod.getResultProcessor().getReturnedType();
		MergedAnnotation<Query> query = MergedAnnotations.from(method).get(Query.class);

		if (queryMethod.isSearchQuery()) {
			return null;
		}

		AotQuery aotQuery = queryFactory.createQuery(getRepositoryInformation(), returnedType, query, queryMethod);

		return MethodContributor.forQueryMethod(queryMethod).withMetadata(new CassandraQueryMetadata(aotQuery))
				.contribute(context -> {

			CodeBlock.Builder body = CodeBlock.builder();

			String queryVariableName = context.localVariable("query");

			body.add(CassandraCodeBlocks.queryBuilder(context, queryMethod).usingQueryVariableName(queryVariableName)
							.query(aotQuery).query(query).build());

			body.add(CassandraCodeBlocks.executionBuilder(context, customConversions, queryMethod)
							.usingQueryVariableName(queryVariableName).query(aotQuery).build());

			return body.build();
		});
	}

}
