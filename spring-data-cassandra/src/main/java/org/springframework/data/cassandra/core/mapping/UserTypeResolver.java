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
package org.springframework.data.cassandra.core.mapping;

import org.springframework.lang.Nullable;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.UserDefinedType;

/**
 * Strategy interface to resolve {@link UserDefinedType} by {@link String name}.
 *
 * @author Mark Paluch
 * @see UserDefinedType
 * @see CqlIdentifier
 * @since 1.5
 */
@FunctionalInterface
public interface UserTypeResolver {

	/**
	 * Resolve a {@link UserDefinedType} by {@link String name}.
	 *
	 * @param typeName {@link String name} of the {@link UserDefinedType} to resolve; must not be {@literal null}.
	 * @return the resolved {@link UserDefinedType} or {@literal null} if not found.
	 */
	@Nullable
	UserDefinedType resolveType(CqlIdentifier typeName);

}
