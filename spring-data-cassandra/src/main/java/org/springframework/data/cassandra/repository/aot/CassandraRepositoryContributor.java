/*
 * Copyright 2025 the original author or authors.
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
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jspecify.annotations.Nullable;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.repository.query.CassandraQueryMethod;
import org.springframework.data.repository.aot.generate.AotRepositoryClassBuilder;
import org.springframework.data.repository.aot.generate.AotRepositoryConstructorBuilder;
import org.springframework.data.repository.aot.generate.MethodContributor;
import org.springframework.data.repository.aot.generate.RepositoryContributor;
import org.springframework.data.repository.config.AotRepositoryContext;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.TypeName;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import static org.springframework.data.cassandra.repository.aot.CassandraCodeBlocks.*;

/**
 * Cassandra specific {@link RepositoryContributor}.
 *
 * @author Chris Bono
 * @since 5.0
 */
public class CassandraRepositoryContributor extends RepositoryContributor {

	private static final Log logger = LogFactory.getLog(RepositoryContributor.class);

	private final AotQueryCreator queryCreator;
	private final CassandraMappingContext mappingContext;

	public CassandraRepositoryContributor(AotRepositoryContext repositoryContext) {

		super(repositoryContext);
		this.queryCreator = new AotQueryCreator();
		this.mappingContext = new CassandraMappingContext();
	}

	@Override
	protected void customizeClass(AotRepositoryClassBuilder builder) {
		builder.customize(b -> b.superclass(TypeName.get(CassandraAotRepositoryFragmentSupport.class)));
	}

	@Override
	protected void customizeConstructor(AotRepositoryConstructorBuilder constructorBuilder) {

		constructorBuilder.addParameter("operations", TypeName.get(CassandraOperations.class));
		constructorBuilder.addParameter("context", TypeName.get(RepositoryFactoryBeanSupport.FragmentCreationContext.class),
				false);

		constructorBuilder.customize((builder) -> {
			builder.addStatement("super(operations, context)");
		});
	}

	@Override
	@SuppressWarnings("NullAway")
	protected @Nullable MethodContributor<? extends QueryMethod> contributeQueryMethod(Method method) {

		var queryMethod = new CassandraQueryMethod(method, getRepositoryInformation(), getProjectionFactory(),
				mappingContext);

		QueryInteraction query = createStringQuery(getRepositoryInformation(), queryMethod,
				AnnotatedElementUtils.findMergedAnnotation(method, Query.class), method.getParameterCount());

		if (queryMethod.hasAnnotatedQuery()) {
			if (StringUtils.hasText(queryMethod.getAnnotatedQuery())
					&& Pattern.compile("[\\?:][#$]\\{.*\\}").matcher(queryMethod.getAnnotatedQuery()).find()) {

				if (logger.isDebugEnabled()) {
					logger.debug(
							"Skipping AOT generation for [%s]. SpEL expressions are not supported".formatted(method.getName()));
				}
				return MethodContributor.forQueryMethod(queryMethod).metadataOnly(query);
			}
		}

		if (backoff(queryMethod)) {
			return null;
		}

		if (query.isDelete()) {
			return deleteMethodContributor(queryMethod, query);
		}

//		if (queryMethod.isModifyingQuery()) {
//			Update updateSource = queryMethod.getUpdateSource();
//			if (StringUtils.hasText(updateSource.value())) {
//				UpdateInteraction update = new UpdateInteraction(query, new StringUpdate(updateSource.value()));
//				return updateMethodContributor(queryMethod, update);
//			}
//		}

		return queryMethodContributor(queryMethod, query);
	}

	@SuppressWarnings("NullAway")
	private QueryInteraction createStringQuery(RepositoryInformation repositoryInformation, CassandraQueryMethod queryMethod,
			@Nullable Query queryAnnotation, int parameterCount) {

		QueryInteraction query;
		if (queryMethod.hasAnnotatedQuery() && queryAnnotation != null) {
			query = new QueryInteraction(new StringQuery(queryMethod.getAnnotatedQuery()), queryAnnotation.count(),
					false, queryAnnotation.exists());
		} else {

			PartTree partTree = new PartTree(queryMethod.getName(), repositoryInformation.getDomainType());
			query = new QueryInteraction(queryCreator.createQuery(partTree, parameterCount), partTree.isCountProjection(),
					partTree.isDelete(), partTree.isExistsProjection());
		}

		if (queryAnnotation != null && StringUtils.hasText(queryAnnotation.sort())) {
			query = query.withSort(queryAnnotation.sort());
		}
		if (queryAnnotation != null && StringUtils.hasText(queryAnnotation.fields())) {
			query = query.withFields(queryAnnotation.fields());
		}

		return query;
	}

	private static boolean backoff(CassandraQueryMethod method) {

		boolean skip = method.isScrollQuery() || method.isStreamQuery();

		if (skip && logger.isDebugEnabled()) {
			logger.debug("Skipping AOT generation for [%s]. Method is either streaming or scrolling query"
					.formatted(method.getName()));
		}
		return skip;
	}

//	private static MethodContributor<CassandraQueryMethod> updateMethodContributor(CassandraQueryMethod queryMethod,
//			UpdateInteraction update) {
//
//		return MethodContributor.forQueryMethod(queryMethod).withMetadata(update).contribute(context -> {
//
//			CodeBlock.Builder builder = CodeBlock.builder();
//			builder.add(context.codeBlocks().logDebug("invoking [%s]".formatted(context.getMethod().getName())));
//
//			// update filter
//			String filterVariableName = update.name();
//			builder.add(queryBlockBuilder(context, queryMethod).filter(update.getFilter())
//					.usingQueryVariableName(filterVariableName).build());
//
//			// update definition
//			String updateVariableName = "updateDefinition";
//			builder.add(
//					updateBlockBuilder(context, queryMethod).update(update).usingUpdateVariableName(updateVariableName).build());
//
//			builder.add(updateExecutionBlockBuilder(context, queryMethod).withFilter(filterVariableName)
//					.referencingUpdate(updateVariableName).build());
//			return builder.build();
//		});
//	}

	private static MethodContributor<CassandraQueryMethod> deleteMethodContributor(CassandraQueryMethod queryMethod,
			QueryInteraction query) {

		return MethodContributor.forQueryMethod(queryMethod).withMetadata(query).contribute(context -> {

			CodeBlock.Builder builder = CodeBlock.builder();
			QueryCodeBlockBuilder queryCodeBlockBuilder = queryBlockBuilder(context, queryMethod).filter(query);

			String queryVariableName = context.localVariable(query.name());
			builder.add(queryCodeBlockBuilder.usingQueryVariableName(queryVariableName).build());
			builder.add(deleteExecutionBlockBuilder(context, queryMethod).referencing(queryVariableName).build());
			return builder.build();
		});
	}

	private static MethodContributor<CassandraQueryMethod> queryMethodContributor(CassandraQueryMethod queryMethod,
			QueryInteraction query) {

		return MethodContributor.forQueryMethod(queryMethod).withMetadata(query).contribute(context -> {

			CodeBlock.Builder builder = CodeBlock.builder();
			QueryCodeBlockBuilder queryCodeBlockBuilder = queryBlockBuilder(context, queryMethod).filter(query);

			builder.add(queryCodeBlockBuilder.usingQueryVariableName(context.localVariable(query.name())).build());
			builder.add(queryExecutionBlockBuilder(context, queryMethod).forQuery(query).build());
			return builder.build();
		});
	}

}
