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

import java.util.List;

import org.jspecify.annotations.Nullable;

import org.springframework.data.cassandra.core.cql.QueryOptions;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.query.CassandraScrollPosition;
import org.springframework.data.cassandra.repository.query.CassandraParameters;
import org.springframework.data.cassandra.repository.query.CassandraParametersParameterAccessor;
import org.springframework.data.cassandra.repository.query.CassandraQueryCreator;
import org.springframework.data.cassandra.repository.query.ParameterBinding;
import org.springframework.data.domain.ScoringFunction;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * Query creator to create queries during AOT processing using {@link ParameterBinding} placeholders.
 *
 * @author Chris Bono
 * @author Mark Paluch
 * @since 5.0
 */
class AotQueryCreator extends CassandraQueryCreator {

	public AotQueryCreator(PartTree tree, CassandraParameters parameters,
			MappingContext<?, CassandraPersistentProperty> mappingContext, List<ParameterBinding> parameterBindings) {
		super(tree, new AotPlaceholderParameterAccessor(parameters, parameterBindings), mappingContext);
	}

	@Override
	protected Object like(Part.Type type, Object value) {

		if (value instanceof ParameterBinding pb) {
			return new LikeParameterBinding(pb, type);
		}

		return super.like(type, value);
	}

	static class AotPlaceholderParameterAccessor extends CassandraParametersParameterAccessor {

		private final List<ParameterBinding> parameterBindings;

		public AotPlaceholderParameterAccessor(CassandraParameters parameters, List<ParameterBinding> parameterBindings) {
			super(parameters, new Object[parameters.getNumberOfParameters()]);
			this.parameterBindings = parameterBindings;
		}

		@Override
		public @Nullable Object getValue(int parameterIndex) {

			ParameterBinding binding = ParameterBinding.indexed(parameterIndex);
			parameterBindings.add(binding);

			return binding;
		}

		@Override
		public CassandraScrollPosition getScrollPosition() {
			return null;
		}

		@Override
		public @Nullable ScoringFunction getScoringFunction() {
			return null;
		}

		@Override
		public @Nullable QueryOptions getQueryOptions() {
			return null;
		}

		@Override
		public @Nullable Object getBindableValue(int index) {
			return getValue(getParameters().getBindableParameter(index).getIndex());
		}

		@Override
		public boolean hasBindableNullValue() {
			return false;
		}

	}

}
