/*
 * Copyright 2020-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import java.util.function.Function;

/**
 * {@link NamingStrategy} that applies a transformation {@link Function} after invoking a delegate
 * {@link NamingStrategy}.
 *
 * @author Mark Paluch
 * @since 3.0
 */
public class TransformingNamingStrategy implements NamingStrategy {

	private final Function<String, String> mappingFunction;

	private final NamingStrategy delegate;

	public TransformingNamingStrategy(NamingStrategy delegate, Function<String, String> mappingFunction) {
		this.delegate = delegate;
		this.mappingFunction = mappingFunction;
	}

	@Override
	public String getTableName(CassandraPersistentEntity<?> entity) {
		return this.mappingFunction.apply(this.delegate.getTableName(entity));
	}

	@Override
	public String getUserDefinedTypeName(CassandraPersistentEntity<?> entity) {
		return this.mappingFunction.apply(this.delegate.getUserDefinedTypeName(entity));
	}

	@Override
	public String getColumnName(CassandraPersistentProperty property) {
		return this.mappingFunction.apply(this.delegate.getColumnName(property));
	}
}
