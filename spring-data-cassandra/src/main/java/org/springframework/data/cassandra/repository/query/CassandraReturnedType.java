/*
 * Copyright 2016-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository.query;

import java.util.Map;

import org.springframework.data.convert.CustomConversions;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.util.ClassUtils;

/**
 * Represents a {@link ReturnedType} in the context of Spring Data Cassandra.
 *
 * @author Mark Paluch
 */
class CassandraReturnedType {

	private final ReturnedType returnedType;

	private final CustomConversions customConversions;

	CassandraReturnedType(ReturnedType returnedType, CustomConversions customConversions) {
		this.returnedType = returnedType;
		this.customConversions = customConversions;
	}

	boolean isProjecting() {

		if (!returnedType.isProjecting()) {
			return false;
		}

		// Spring Data Cassandra allows List<Map<String, Object> and Map<String, Object> declarations
		// on query methods so we don't want to let projection kick in
		if (ClassUtils.isAssignable(Map.class, returnedType.getReturnedType())) {
			return false;
		}

		// Type conversion using registered conversions is handled on template level
		if (customConversions.hasCustomWriteTarget(returnedType.getReturnedType())) {
			return false;
		}

		// Don't apply projection on Cassandra simple types
		return !customConversions.isSimpleType(returnedType.getReturnedType());
	}

	Class<?> getDomainType() {
		return returnedType.getDomainType();
	}

	Class<?> getReturnedType() {
		return returnedType.getReturnedType();
	}
}
