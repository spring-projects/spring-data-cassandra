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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;

import org.jspecify.annotations.NullUnmarked;
import org.jspecify.annotations.Nullable;

import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.ExecutableSelectOperation;
import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.query.CassandraPageRequest;
import org.springframework.data.cassandra.core.query.CassandraScrollPosition;
import org.springframework.data.cassandra.core.query.ColumnName;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Criteria;
import org.springframework.data.cassandra.core.query.CriteriaDefinition;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.cassandra.repository.query.CassandraQueryMethod;
import org.springframework.data.cassandra.repository.query.ParameterBinding;
import org.springframework.data.cassandra.repository.query.WindowUtil;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Sort;
import org.springframework.data.javapoet.LordOfTheStrings;
import org.springframework.data.javapoet.TypeNames;
import org.springframework.data.repository.aot.generate.AotQueryMethodGenerationContext;
import org.springframework.data.repository.aot.generate.MethodReturn;
import org.springframework.data.util.Streamable;
import org.springframework.javapoet.CodeBlock;
import org.springframework.javapoet.CodeBlock.Builder;
import org.springframework.javapoet.TypeName;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;

/**
 * {@link CodeBlock} generator for common Cassandra tasks.
 *
 * @author Chris Bono
 * @author Mark Paluch
 * @since 5.0
 */
class CassandraCodeBlocks {

	/**
	 * Builder for generating query parsing {@link CodeBlock}.
	 *
	 * @param context
	 * @param queryMethod
	 * @return new instance of {@link QueryBlockBuilder}.
	 */
	static QueryBlockBuilder queryBuilder(AotQueryMethodGenerationContext context, CassandraQueryMethod queryMethod) {
		return new QueryBlockBuilder(context, queryMethod);
	}

	/**
	 * Builder for generating finder query execution {@link CodeBlock}.
	 *
	 * @param context
	 * @param customConversions
	 * @param queryMethod
	 * @return
	 */
	static QueryExecutionBlockBuilder executionBuilder(AotQueryMethodGenerationContext context,
			CustomConversions customConversions, CassandraQueryMethod queryMethod) {

		return new QueryExecutionBlockBuilder(context, customConversions, queryMethod);
	}

	@NullUnmarked
	static class QueryBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final CassandraQueryMethod queryMethod;

		private @Nullable AotQuery query;
		private String queryVariableName;
		private MergedAnnotation<Query> queryAnnotation = MergedAnnotation.missing();

		QueryBlockBuilder(AotQueryMethodGenerationContext context, CassandraQueryMethod queryMethod) {

			this.context = context;
			this.queryMethod = queryMethod;
		}

		QueryBlockBuilder query(AotQuery query) {
			this.query = query;
			return this;
		}

		QueryBlockBuilder usingQueryVariableName(String queryVariableName) {
			this.queryVariableName = queryVariableName;
			return this;
		}

		QueryBlockBuilder query(MergedAnnotation<Query> query) {
			this.queryAnnotation = query;
			return this;
		}

		CodeBlock build() {

			if (query instanceof StringAotQuery sq) {
				return buildStringQuery(sq);
			} else if (query instanceof DerivedAotQuery derived) {
				return buildDerivedQuery(derived);
			}

			throw new UnsupportedOperationException("Unsupported query: " + query);
		}

		private CodeBlock buildStringQuery(StringAotQuery query) {

			Builder builder = CodeBlock.builder();
			String pagingState = null;

			if (StringUtils.hasText(context.getPageableParameterName())) {

				pagingState = context.localVariable("pagingState");
				builder.addStatement("$1T $2L = $3L instanceof $4T $5L ? $5L.getPagingState() : null", ByteBuffer.class,
						pagingState, context.getPageableParameterName(), CassandraPageRequest.class,
						context.localVariable("pageRequest"));

			} else if (StringUtils.hasText(context.getScrollPositionParameterName())) {

				pagingState = context.localVariable("pagingState");
				builder.addStatement("$1T $2L = $3L instanceof $4T $5L && !$5L.isInitial() ? $5L.getPagingState() : null",
						ByteBuffer.class, pagingState, context.getScrollPositionParameterName(), CassandraScrollPosition.class,
						context.localVariable("cassandraScrollPosition"));
			}

			builder.add(buildQuery(query));
			builder.add(buildOptions(pagingState));

			return builder.build();
		}

		private CodeBlock buildQuery(StringAotQuery query) {

			CodeBlock.Builder builder = CodeBlock.builder();

			if (query.getParameterBindings().isEmpty()) {
				builder.addStatement("$1T $2L = $1T.newInstance($3S)", SimpleStatement.class, queryVariableName,
						query.getQueryString());

				return builder.build();
			}

			builder.addStatement("Object[] $L = new Object[$L]", context.localVariable("args"),
					query.getParameterBindings().size());

			int index = 0;
			for (ParameterBinding binding : query.getParameterBindings()) {

				// TODO:Conversion, Data type
				builder.addStatement("$1L[$2L] = $3L", context.localVariable("args"), index++, getParameter(binding));
			}

			builder.addStatement("$1T $2L = $1T.newInstance($3S, $4L)", SimpleStatement.class, queryVariableName,
					query.getQueryString(), context.localVariable("args"));

			return builder.build();
		}

		private CodeBlock buildOptions(@Nullable String pagingState) {

			CodeBlock.Builder builder = CodeBlock.builder();

			if (queryAnnotation.isPresent()) {

				Query.Idempotency idempotent = queryAnnotation.getEnum("idempotent", Query.Idempotency.class);

				if (idempotent != Query.Idempotency.UNDEFINED) {
					builder.addStatement("$1L = $1L.setIdempotent($2L)", queryVariableName,
							idempotent == Query.Idempotency.IDEMPOTENT);
				}
			}

			if (StringUtils.hasText(pagingState)) {
				builder.addStatement("$1L = $1L.setPagingState($2L)", queryVariableName, pagingState);
			}

			if (StringUtils.hasText(context.getPageableParameterName())) {

				builder.beginControlFlow("if ($1L.isPaged())", context.getPageableParameterName());
				builder.addStatement("$1L = $1L.setPageSize($2L.getPageSize())", queryVariableName,
						context.getPageableParameterName());
				builder.endControlFlow();
			}

			if (StringUtils.hasText(context.getLimitParameterName())) {

				builder.beginControlFlow("if ($1L.isLimited())", context.getLimitParameterName());
				builder.addStatement("$1L = $1L.setPageSize($2L.max())", queryVariableName, context.getLimitParameterName());
				builder.endControlFlow();
			}

			if (queryMethod.hasConsistencyLevel()) {

				ConsistencyLevel consistencyLevel = queryMethod.getRequiredAnnotatedConsistencyLevel();

				builder.addStatement("$1L = $1L.setConsistencyLevel($2T.$3L)", queryVariableName, ConsistencyLevel.class,
						consistencyLevel.name());
			}

			return builder.build();
		}

		private CodeBlock buildDerivedQuery(DerivedAotQuery derived) {

			org.springframework.data.cassandra.core.query.Query query = derived.getQuery();

			Builder builder = CodeBlock.builder();

			builder.add(buildQuery(query));
			builder.add(buildColumns(query.getColumns()));
			builder.add(buildSortScrollLimit(derived, query));

			builder.add(buildQueryOptions(derived));

			return builder.build();
		}

		private CodeBlock buildQuery(org.springframework.data.cassandra.core.query.Query query) {

			Builder queryBuilder = CodeBlock.builder();

			if (query.isEmpty()) {

				queryBuilder.addStatement("$1T $2L = $1T.empty()", org.springframework.data.cassandra.core.query.Query.class,
						queryVariableName);

				return queryBuilder.build();
			}

			LordOfTheStrings.CodeBlockBuilder CodeBlockBuilder = LordOfTheStrings.builder(queryBuilder);
			CodeBlockBuilder.addStatement(it -> {
				it.add("$1T $2L = $1T.query(", org.springframework.data.cassandra.core.query.Query.class, queryVariableName);

				it.addAll(query, ".and(", (criteria) -> {

					LordOfTheStrings.CodeBlockBuilder builder = LordOfTheStrings.builder("$1T.where($2S)", Criteria.class,
							criteria.getColumnName().toCql());
					appendPredicate(criteria, builder);
					builder.add(")");

					return builder.build();
				});
			});

			return CodeBlockBuilder.build();
		}

		private CodeBlock buildColumns(Columns columns) {

			if (columns.isEmpty()) {
				return CodeBlock.builder().build();
			}

			boolean first = true;
			Builder columnBuilder = CodeBlock.builder();
			columnBuilder.add("$[");
			columnBuilder.add("$1T $2L = $1T.from(", Columns.class, context.localVariable("columns"));

			for (ColumnName column : columns) {

				columnBuilder.add("$S", column.toCql());

				if (first) {
					first = false;
				} else {
					columnBuilder.add(", ");
				}
			}

			columnBuilder.add(");\n$]");

			columnBuilder.addStatement("$1L = $1L.columns($2L)", queryVariableName, context.localVariable("columns"));

			return columnBuilder.build();
		}

		private CodeBlock buildSortScrollLimit(DerivedAotQuery derived,
				org.springframework.data.cassandra.core.query.Query query) {

			Builder builder = CodeBlock.builder();

			// Slice and Window require an indication for hasNext() so we increase the query limit and leave the page size set
			// to the limit.
			boolean increaseLimitByOne = queryMethod.isSliceQuery() || queryMethod.isScrollQuery();

			if (query.getSort().isSorted()) {
				builder.addStatement("$1L = $1L.sort($2L)", queryVariableName, buildSort(query.getSort()));
			}

			if (derived.isLimited()) {
				builder.addStatement("$1L = $1L.limit($2L)", queryVariableName,
						derived.getLimit().max() + (increaseLimitByOne ? 1 : 0));
			}

			if (StringUtils.hasText(context.getLimitParameterName())) {

				if (increaseLimitByOne) {
					builder.beginControlFlow("if ($1L.isLimited())", context.getLimitParameterName());
					builder.addStatement("$1L = $1L.limit($2L.max() + 1)", queryVariableName, context.getLimitParameterName());
					builder.endControlFlow();
				} else {
					builder.addStatement("$1L = $1L.limit($2L)", queryVariableName, context.getLimitParameterName());
				}
			}

			if (StringUtils.hasText(context.getSortParameterName())) {
				builder.addStatement("$1L = $1L.sort($2L)", queryVariableName, context.getSortParameterName());
			} else if (StringUtils.hasText(context.getPageableParameterName())) {
				builder.addStatement("$1L = $1L.pageRequest($2L)", queryVariableName, context.getPageableParameterName());
			}

			if (StringUtils.hasText(context.getScrollPositionParameterName())) {
				builder.beginControlFlow("if (!$1L.isInitial())", context.getScrollPositionParameterName());
				builder.addStatement("$1L = $1L.pagingState(($2T) $3L)", queryVariableName, CassandraScrollPosition.class,
						context.getScrollPositionParameterName());
				builder.endControlFlow();
			}

			if (queryAnnotation.isPresent()) {

				boolean allowFiltering = queryAnnotation.getBoolean("allowFiltering");

				if (allowFiltering) {
					builder.addStatement("$1L = $1L.withAllowFiltering()", queryVariableName);
				}
			}

			return builder.build();
		}

		private static CodeBlock buildSort(Sort sort) {

			LordOfTheStrings.InvocationBuilder invocation = LordOfTheStrings.invoke("$T.by($L)", Sort.class);

			invocation.arguments(sort, order -> {

				LordOfTheStrings.CodeBlockBuilder builder = LordOfTheStrings.builder("$T.$L($S)", Sort.Order.class,
						order.isAscending() ? "asc" : "desc", order.getProperty());

				if (order.isIgnoreCase()) {
					builder.add(".ignoreCase()");
				}

				return builder.build();
			});

			return invocation.build();
		}

		private CodeBlock buildQueryOptions(DerivedAotQuery derived) {

			CodeBlock.Builder builder = CodeBlock.builder();

			boolean requiresOptions = StringUtils.hasText(context.getLimitParameterName())
					|| StringUtils.hasText(context.getPageableParameterName()) || derived.isLimited()
					|| queryMethod.hasConsistencyLevel() || queryMethod.isSliceQuery() | queryMethod.isScrollQuery();

			int queryOptionsIndex = queryMethod.getParameters().getQueryOptionsIndex();

			if (requiresOptions) {

				if (queryOptionsIndex != -1) {

					String queryOptions = context.getParameterName(queryOptionsIndex);
					builder.addStatement("$1T $2L = $3L.mutate()", QueryOptions.QueryOptionsBuilder.class,
							context.localVariable("optionsBuilder"), queryOptions);
				} else {
					builder.addStatement("$1T $2L = $3T.builder()", QueryOptions.QueryOptionsBuilder.class,
							context.localVariable("optionsBuilder"), QueryOptions.class);
				}

				applyOptions(derived.getLimit(), builder);

				builder.addStatement("$1L = $1L.queryOptions($2L.build())", queryVariableName,
						context.localVariable("optionsBuilder"));

			} else if (queryOptionsIndex != -1) {

				String queryOptions = context.getParameterName(queryOptionsIndex);
				builder.addStatement("$1L = $1L.queryOptions($2L)", queryVariableName, queryOptions);
			}

			return builder.build();
		}

		private void applyOptions(Limit limit, Builder builder) {

			if (limit.isLimited()) {
				builder.addStatement("$1L.pageSize($2L)", context.localVariable("optionsBuilder"), limit.max());
			}

			if (StringUtils.hasText(context.getPageableParameterName())) {

				builder.beginControlFlow("if ($1L.isPaged())", context.getPageableParameterName());
				builder.addStatement("$1L.pageSize($2L.getPageSize())", context.localVariable("optionsBuilder"),
						context.getPageableParameterName());
				builder.endControlFlow();
			}

			if (StringUtils.hasText(context.getLimitParameterName())) {

				builder.beginControlFlow("if ($1L.isLimited())", context.getLimitParameterName());
				builder.addStatement("$1L.pageSize($2L.max())", context.localVariable("optionsBuilder"),
						context.getLimitParameterName());
				builder.endControlFlow();
			}

			if (queryMethod.hasConsistencyLevel()) {
				ConsistencyLevel consistencyLevel = queryMethod.getRequiredAnnotatedConsistencyLevel();
				builder.addStatement("$1L.consistencyLevel($2T.$3L)", context.localVariable("optionsBuilder"),
						ConsistencyLevel.class, consistencyLevel.name());
			}
		}

		private void appendPredicate(CriteriaDefinition criteriaDefinition,
				LordOfTheStrings.CodeBlockBuilder criteriaBuilder) {

			CriteriaDefinition.Predicate predicate = criteriaDefinition.getPredicate();

			if (predicate.getOperator() == CriteriaDefinition.Operators.EQ) {
				criteriaBuilder.add(".is($L)", render(predicate.getValue()));
			} else if (predicate.getOperator() == CriteriaDefinition.Operators.NE) {
				criteriaBuilder.add(".ne($L)", render(predicate.getValue()));
			} else if (predicate.getOperator() == CriteriaDefinition.Operators.GT) {
				criteriaBuilder.add(".gt($L)", render(predicate.getValue()));
			} else if (predicate.getOperator() == CriteriaDefinition.Operators.GTE) {
				criteriaBuilder.add(".gte($L)", render(predicate.getValue()));
			} else if (predicate.getOperator() == CriteriaDefinition.Operators.LT) {
				criteriaBuilder.add(".lt($L)", render(predicate.getValue()));
			} else if (predicate.getOperator() == CriteriaDefinition.Operators.LTE) {
				criteriaBuilder.add(".lte($L)", render(predicate.getValue()));
			} else if (predicate.getOperator() == CriteriaDefinition.Operators.IS_NOT_NULL) {
				criteriaBuilder.add(".isNotNull()");
			} else if (predicate.getOperator() == CriteriaDefinition.Operators.LIKE) {
				criteriaBuilder.add(".like($L)", render(predicate.getValue()));
			} else if (predicate.getOperator() == CriteriaDefinition.Operators.CONTAINS) {
				criteriaBuilder.add(".contains($L)", render(predicate.getValue()));
			} else if (predicate.getOperator() == CriteriaDefinition.Operators.CONTAINS_KEY) {
				criteriaBuilder.add(".containsKey($L)", render(predicate.getValue()));
			} else if (predicate.getOperator() == CriteriaDefinition.Operators.IN) {
				criteriaBuilder.add(".in($L)", render(predicate.getValue()));
			} else {
				throw new UnsupportedOperationException("Operator not supported yet: " + predicate.getOperator());
			}
		}

		private Object render(@Nullable Object value) {

			if (value instanceof ParameterBinding binding) {

				String parameterName = getParameterName(binding);

				if (binding instanceof LikeParameterBinding like) {

					return switch (like.getType()) {

						case CONTAINING -> "\"%\" + " + parameterName + " + \"%\"";
						case STARTING_WITH -> parameterName + " + \"%\"";
						case ENDING_WITH -> "\"%\" + " + parameterName;
						default -> parameterName;
					};
				}

				return parameterName;
			}

			return value != null ? value.toString() : null;
		}

		String getParameterName(ParameterBinding binding) {

			if (binding.getOrigin() instanceof ParameterBinding.MethodInvocationArgument mia) {

				ParameterBinding.BindingIdentifier identifier = mia.identifier();
				if (identifier.hasPosition()) {
					return context.getParameterName(identifier.getPosition());
				}
				return identifier.getName();
			}

			throw new UnsupportedOperationException("Unsupported origin: " + binding.getOrigin());
		}

		private CodeBlock getParameter(ParameterBinding binding) {

			ParameterBinding.ParameterOrigin origin = binding.getOrigin();
			if (origin.isMethodArgument() && origin instanceof ParameterBinding.MethodInvocationArgument mia) {
				return CodeBlock.of("potentiallyConvertBindingValue($L)", getParameterName(binding));
			}

			if (origin.isExpression() && origin instanceof ParameterBinding.Expression expr) {

				String expressionString = expr.expression().getExpressionString();
				// re-wrap expression
				if (!expressionString.startsWith("$")) {
					expressionString = "#{" + expressionString + "}";
				}

				return LordOfTheStrings.invoke("evaluateExpression($L)")
						.argument(context.getExpressionMarker().enclosingMethod()) //
						.argument("$S", expressionString) //
						.arguments(context.getAllParameterNames()) //
						.build();
			}

			throw new UnsupportedOperationException("Not supported yet for: " + origin);
		}

	}

	@NullUnmarked
	static class QueryExecutionBlockBuilder {

		private final AotQueryMethodGenerationContext context;
		private final CustomConversions customConversions;
		private final CassandraQueryMethod queryMethod;
		private @Nullable AotQuery query;
		private @Nullable String queryVariableName;

		QueryExecutionBlockBuilder(AotQueryMethodGenerationContext context, CustomConversions customConversions,
				CassandraQueryMethod queryMethod) {

			this.context = context;
			this.customConversions = customConversions;
			this.queryMethod = queryMethod;
		}

		QueryExecutionBlockBuilder query(AotQuery query) {

			this.query = query;
			return this;
		}

		QueryExecutionBlockBuilder usingQueryVariableName(String queryVariableName) {
			this.queryVariableName = queryVariableName;
			return this;
		}

		CodeBlock build() {

			Builder builder = CodeBlock.builder();
			MethodReturn methodReturn = context.getMethodReturn();

			boolean isProjecting = !query.isCount() && !query.isExists()
					&& (context.getMethodReturn().isProjecting()
							&& !customConversions.isSimpleType(context.getMethodReturn().toClass()))
					|| StringUtils.hasText(context.getDynamicProjectionParameterName());
			Class<?> domainType = context.getRepositoryInformation().getDomainType();

			builder.add("\n");

			if (query.isDelete()) {

				LordOfTheStrings.InvocationBuilder method;
				if (query instanceof StringAotQuery) {

					method = LordOfTheStrings.invoke("$L.getCqlOperations().execute($L)",
							context.fieldNameOf(CassandraOperations.class), queryVariableName);
				} else {
					method = LordOfTheStrings.invoke("$L.delete($L, $T.class)", context.fieldNameOf(CassandraOperations.class),
							queryVariableName, domainType);
				}

				if (methodReturn.isVoid()) {
					builder.addStatement(method.build());
				} else {
					builder.addStatement(method.assignTo("boolean $L", context.localVariable("result")));
				}

				builder.addStatement(LordOfTheStrings.returning(methodReturn.toClass()) //
						.whenBoolean("$L", context.localVariable("result")) //
						.build());

				return builder.build();
			}

			boolean isInterfaceProjection = methodReturn.isInterfaceProjection();
			boolean requiresConversion = false;

			boolean isMapProjection = methodReturn.getActualReturnClass().equals(Map.class);
			boolean rawProjection = isMapProjection || methodReturn.toClass().equals(ResultSet.class);

			TypeName actualReturnType = isMapProjection ? methodReturn.getActualClassName()
					: TypeNames.typeNameOrWrapper(methodReturn.getActualType());
			Object asDynamicTypeNameOrProjectionTypeParameter = actualReturnType;

			if (StringUtils.hasText(context.getDynamicProjectionParameterName())) {
				asDynamicTypeNameOrProjectionTypeParameter = context.getDynamicProjectionParameterName();
			}

			if (query instanceof StringAotQuery) {

				if (StringUtils.hasText(context.getDynamicProjectionParameterName())) {

					builder.addStatement("$1T<$2T> $3L = $4L.query($5L).as($6L)",
							ExecutableSelectOperation.TerminatingResults.class, actualReturnType, context.localVariable("select"),
							context.fieldNameOf(CassandraOperations.class), queryVariableName,
							context.getDynamicProjectionParameterName());

				} else if (isProjecting && !rawProjection) {

					requiresConversion = isInterfaceProjection;

					builder.addStatement("$1T<$2T> $3L = $4L.query($5L).as($2T.class)",
							ExecutableSelectOperation.TerminatingResults.class,
							requiresConversion ? context.getDomainType() : actualReturnType, context.localVariable("select"),
							context.fieldNameOf(CassandraOperations.class), queryVariableName);

				} else {

					if (query.isExists() || query.isCount()) {
						builder.addStatement("$1T $2L = $3L.query($4L)", ExecutableSelectOperation.TerminatingProjections.class,
								context.localVariable("select"), context.fieldNameOf(CassandraOperations.class), queryVariableName);
					} else {
						builder.addStatement("$1T<$2T> $3L = $4L.query($5L).as($2T.class)",
								ExecutableSelectOperation.TerminatingResults.class, actualReturnType, context.localVariable("select"),
								context.fieldNameOf(CassandraOperations.class), queryVariableName);
					}
				}
			} else {

				if (isProjecting) {

					String as = StringUtils.hasText(context.getDynamicProjectionParameterName()) ? "$6L" : "$6T.class";

					builder.addStatement("$1T<$2T> $3L = $4L.query($5T.class).as(%s).matching($7L)".formatted(as),
							ExecutableSelectOperation.TerminatingSelect.class, actualReturnType, context.localVariable("select"),
							context.fieldNameOf(CassandraOperations.class), domainType, asDynamicTypeNameOrProjectionTypeParameter,
							queryVariableName);
				} else {

					builder.addStatement("$1T<$2T> $3L = $4L.query($2T.class).matching($5L)",
							ExecutableSelectOperation.TerminatingSelect.class, domainType, context.localVariable("select"),
							context.fieldNameOf(CassandraOperations.class), queryVariableName);
				}
			}

			String terminatingMethod;

			boolean paginated = StringUtils.hasText(context.getPageableParameterName())
					|| StringUtils.hasText(context.getScrollPositionParameterName())
					|| StringUtils.hasText(context.getLimitParameterName());

			boolean streamableResult = queryMethod.isScrollQuery() || queryMethod.isSliceQuery() || queryMethod.isPageQuery()
					|| (queryMethod.isCollectionQuery() && paginated);

			if (streamableResult) {
				terminatingMethod = "slice()";
			} else if (queryMethod.isCollectionQuery()) {
				terminatingMethod = "all()";
			} else if (queryMethod.isStreamQuery()) {
				terminatingMethod = "stream()";
			} else if (query.isCount()) {
				terminatingMethod = "count()";
			} else if (query.isExists()) {
				terminatingMethod = "count() > 0";
			} else if (query.isLimited()) {
				terminatingMethod = "firstValue()";
			} else {
				terminatingMethod = "oneValue()";
			}

			Builder execution = CodeBlock.builder();

			if (queryMethod.isScrollQuery()) {
				execution.add("$T.of($L.$L)", WindowUtil.class, context.localVariable("select"), terminatingMethod);
			} else if (methodReturn.isArray()) {
				execution.add("$L.$L.toArray(new $T[0])", context.localVariable("select"), terminatingMethod,
						methodReturn.getActualClassName());
			} else {

				if (rawProjection && isMapProjection) {
					execution.add("($T) $L.$L", methodReturn.getClassName(), context.localVariable("select"), terminatingMethod);
				} else {
					execution.add("$L.$L", context.localVariable("select"), terminatingMethod);
				}
			}

			LordOfTheStrings.TypedReturnBuilder returnBuilder = LordOfTheStrings.returning(methodReturn.toClass());

			if (requiresConversion) {

				String conversionMethod;

				if (queryMethod.isCollectionQuery() || streamableResult || queryMethod.isStreamQuery()) {
					conversionMethod = "convertMany";
				} else {
					conversionMethod = "convertOne";
				}

				CodeBlock result = CodeBlock.of("$L($L, $T.class)", conversionMethod, execution.build(), actualReturnType);

				if (streamableResult && Collection.class.isAssignableFrom(methodReturn.toClass())) {
					result = CodeBlock.of("$L.getContent()", result);
				} else if (!streamableResult && methodReturn.toClass().equals(Streamable.class)) {
					result = CodeBlock.of("$T.of(($T) $L)", Streamable.class, Iterable.class, result);
				}

				builder.addStatement(returnBuilder //
						.optional("($T) $L", methodReturn.getTypeName(), result) //
						.build());
			} else {

				CodeBlock executionBlock = execution.build();

				if (query.isCount()) {
					returnBuilder.whenPrimitiveOrBoxed(Integer.class, "(int) $L", executionBlock);
				} else if (streamableResult && Collection.class.isAssignableFrom(methodReturn.toClass())) {
					executionBlock = CodeBlock.of("$L.getContent()", executionBlock);
				} else if (!streamableResult && methodReturn.toClass().equals(Streamable.class)) {
					executionBlock = CodeBlock.of("$T.of($L)", Streamable.class, executionBlock);
				}

				builder.addStatement(returnBuilder.optional(executionBlock) //
						.build());
			}

			return builder.build();
		}
	}

}
