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

import java.util.regex.Pattern;

import org.jspecify.annotations.NullUnmarked;

import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.repository.query.CassandraQueryMethod;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;
import org.springframework.util.StringUtils;

/**
 * {@link CodeBlock} generator for common Cassandra tasks.
 *
 * @author Chris Bono
 */
class CassandraCodeBlocks {

	private static final Pattern PARAMETER_BINDING_PATTERN = Pattern.compile("\\?(\\d+)");

	/**
	 * Builder for generating query parsing {@link CodeBlock}.
	 *
	 * @param context
	 * @param queryMethod
	 * @return new instance of {@link QueryCodeBlockBuilder}.
	 */
	static QueryCodeBlockBuilder queryBlockBuilder(AotQueryMethodGenerationContext context,
			CassandraQueryMethod queryMethod) {
		return new QueryCodeBlockBuilder(context, queryMethod);
	}

	/**
	 * Builder for generating finder query execution {@link CodeBlock}.
	 *
	 * @param context
	 * @param queryMethod
	 * @return
	 */
	static QueryExecutionCodeBlockBuilder queryExecutionBlockBuilder(AotQueryMethodGenerationContext context,
			CassandraQueryMethod queryMethod) {

		return new QueryExecutionCodeBlockBuilder(context, queryMethod);
	}
	
	@NullUnmarked
	static class QueryExecutionCodeBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final CassandraQueryMethod queryMethod;
		private QueryInteraction query;

		QueryExecutionCodeBlockBuilder(AotQueryMethodGenerationContext context, CassandraQueryMethod queryMethod) {

			this.context = context;
			this.queryMethod = queryMethod;
		}

		QueryExecutionCodeBlockBuilder forQuery(QueryInteraction query) {

			this.query = query;
			return this;
		}

		CodeBlock build() {

			String cassandraOpsRef = context.fieldNameOf(CassandraOperations.class);

			Builder builder = CodeBlock.builder();

			boolean isProjecting = context.getReturnedType().isProjecting();
			Object actualReturnType = isProjecting ? context.getActualReturnType().getType()
					: context.getRepositoryInformation().getDomainType();

			builder.add("\n");

			if (isProjecting) {
				builder.addStatement("$T<$T> finder = $L.query($T.class).as($T.class)", FindWithQuery.class, actualReturnType,
						cassandraOpsRef, context.getRepositoryInformation().getDomainType(), actualReturnType);
			} else {

				builder.addStatement("$T<$T> finder = $L.query($T.class)", FindWithQuery.class, actualReturnType, cassandraOpsRef,
						context.getRepositoryInformation().getDomainType());
			}

			String terminatingMethod;

			if (queryMethod.isCollectionQuery() || queryMethod.isPageQuery() || queryMethod.isSliceQuery()) {
				terminatingMethod = "all()";
			} else if (query.isCount()) {
				terminatingMethod = "count()";
			} else if (query.isExists()) {
				terminatingMethod = "exists()";
			} else {
				terminatingMethod = Optional.class.isAssignableFrom(context.getReturnType().toClass()) ? "one()" : "oneValue()";
			}

			if (queryMethod.isPageQuery()) {
				builder.addStatement("return new $T(finder, $L).execute($L)", PagedExecution.class,
						context.getPageableParameterName(), query.name());
			} else if (queryMethod.isSliceQuery()) {
				builder.addStatement("return new $T(finder, $L).execute($L)", SlicedExecution.class,
						context.getPageableParameterName(), query.name());
			} else {
				builder.addStatement("return finder.matching($L).$L", query.name(), terminatingMethod);
			}

			return builder.build();
		}
	}

	@NullUnmarked
	static class QueryCodeBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final CassandraQueryMethod queryMethod;

		private QueryInteraction source;
		private List<String> arguments;
		private String queryVariableName;

		QueryCodeBlockBuilder(AotQueryMethodGenerationContext context, CassandraQueryMethod queryMethod) {

			this.context = context;
			this.arguments = context.getBindableParameterNames();
			this.queryMethod = queryMethod;
		}

		QueryCodeBlockBuilder filter(QueryInteraction query) {

			this.source = query;
			return this;
		}

		QueryCodeBlockBuilder usingQueryVariableName(String queryVariableName) {
			this.queryVariableName = queryVariableName;
			return this;
		}

		CodeBlock build() {

			Builder builder = CodeBlock.builder();

			builder.add("\n");
			builder.add(renderExpressionToQuery(source.getQuery().getQueryString(), queryVariableName));

			if (StringUtils.hasText(source.getQuery().getFieldsString())) {

				builder.add(renderExpressionToDocument(source.getQuery().getFieldsString(), "fields", arguments));
				builder.addStatement("$L.setFieldsObject(fields)", queryVariableName);
			}

			String sortParameter = context.getSortParameterName();
			if (StringUtils.hasText(sortParameter)) {
				builder.addStatement("$L.with($L)", queryVariableName, sortParameter);
			} else if (StringUtils.hasText(source.getQuery().getSortString())) {

				builder.add(renderExpressionToDocument(source.getQuery().getSortString(), "sort", arguments));
				builder.addStatement("$L.setSortObject(sort)", queryVariableName);
			}

			String limitParameter = context.getLimitParameterName();
			if (StringUtils.hasText(limitParameter)) {
				builder.addStatement("$L.limit($L)", queryVariableName, limitParameter);
			} else if (context.getPageableParameterName() == null && source.getQuery().isLimited()) {
				builder.addStatement("$L.limit($L)", queryVariableName, source.getQuery().getLimit());
			}

			String pageableParameter = context.getPageableParameterName();
			if (StringUtils.hasText(pageableParameter) && !queryMethod.isPageQuery() && !queryMethod.isSliceQuery()) {
				builder.addStatement("$L.with($L)", queryVariableName, pageableParameter);
			}

			MergedAnnotation<Hint> hintAnnotation = context.getAnnotation(Hint.class);
			String hint = hintAnnotation.isPresent() ? hintAnnotation.getString("value") : null;

			if (StringUtils.hasText(hint)) {
				builder.addStatement("$L.withHint($S)", queryVariableName, hint);
			}

			MergedAnnotation<ReadPreference> readPreferenceAnnotation = context.getAnnotation(ReadPreference.class);
			String readPreference = readPreferenceAnnotation.isPresent() ? readPreferenceAnnotation.getString("value") : null;

			if (StringUtils.hasText(readPreference)) {
				builder.addStatement("$L.withReadPreference($T.valueOf($S))", queryVariableName,
						com.Cassandra.ReadPreference.class, readPreference);
			}

			// TODO: Meta annotation

			return builder.build();
		}

		private CodeBlock renderExpressionToQuery(@Nullable String source, String variableName) {

			Builder builder = CodeBlock.builder();
			if (!StringUtils.hasText(source)) {

				builder.addStatement("$T $L = new $T(new $T())", BasicQuery.class, variableName, BasicQuery.class,
						Document.class);
			} else if (!containsPlaceholder(source)) {

				String tmpVarName = "%sString".formatted(variableName);
				builder.addStatement("String $L = $S", tmpVarName, source);

				builder.addStatement("$T $L = new $T($T.parse($L))", BasicQuery.class, variableName, BasicQuery.class,
						Document.class, tmpVarName);
			} else {

				String tmpVarName = "%sString".formatted(variableName);
				builder.addStatement("String $L = $S", tmpVarName, source);
				builder.addStatement("$T $L = createQuery($L, new $T[]{ $L })", BasicQuery.class, variableName, tmpVarName,
						Object.class, StringUtils.collectionToDelimitedString(arguments, ", "));
			}

			return builder.build();
		}
	}

	@NullUnmarked
	static class UpdateCodeBlockBuilder {

		private UpdateInteraction source;
		private List<String> arguments;
		private String updateVariableName;

		public UpdateCodeBlockBuilder(AotQueryMethodGenerationContext context, CassandraQueryMethod queryMethod) {
			this.arguments = context.getBindableParameterNames();
		}

		public UpdateCodeBlockBuilder update(UpdateInteraction update) {
			this.source = update;
			return this;
		}

		public UpdateCodeBlockBuilder usingUpdateVariableName(String updateVariableName) {
			this.updateVariableName = updateVariableName;
			return this;
		}

		CodeBlock build() {

			Builder builder = CodeBlock.builder();

			builder.add("\n");
			String tmpVariableName = updateVariableName + "Document";
			builder.add(renderExpressionToDocument(source.getUpdate().getUpdateString(), tmpVariableName, arguments));
			builder.addStatement("$T $L = new $T($L)", BasicUpdate.class, updateVariableName, BasicUpdate.class,
					tmpVariableName);

			return builder.build();
		}
	}

	private static CodeBlock renderExpressionToDocument(@Nullable String source, String variableName,
			List<String> arguments) {

		Builder builder = CodeBlock.builder();
		if (!StringUtils.hasText(source)) {
			builder.addStatement("$T $L = new $T()", Document.class, variableName, Document.class);
		} else if (!containsPlaceholder(source)) {

			String tmpVarName = "%sString".formatted(variableName);
			builder.addStatement("String $L = $S", tmpVarName, source);
			builder.addStatement("$T $L = $T.parse($L)", Document.class, variableName, Document.class, tmpVarName);
		} else {

			String tmpVarName = "%sString".formatted(variableName);
			builder.addStatement("String $L = $S", tmpVarName, source);
			builder.addStatement("$T $L = bindParameters($L, new $T[]{ $L })", Document.class, variableName, tmpVarName,
					Object.class, StringUtils.collectionToDelimitedString(arguments, ", "));
		}
		return builder.build();
	}

	private static boolean containsPlaceholder(String source) {
		return PARAMETER_BINDING_PATTERN.matcher(source).find();
	}
}
