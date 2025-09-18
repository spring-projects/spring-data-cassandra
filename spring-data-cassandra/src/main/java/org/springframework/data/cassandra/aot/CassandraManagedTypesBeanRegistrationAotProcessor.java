/*
 * Copyright 2022-2025 the original author or authors.
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
package org.springframework.data.cassandra.aot;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.function.Predicate;

import org.jspecify.annotations.Nullable;

import org.springframework.data.aot.ManagedTypesBeanRegistrationAotProcessor;
import org.springframework.data.cassandra.CassandraManagedTypes;
import org.springframework.data.util.Predicates;
import org.springframework.data.util.TypeCollector;
import org.springframework.data.util.TypeUtils;
import org.springframework.util.ClassUtils;

/**
 * Cassandra-specific extension to {@link ManagedTypesBeanRegistrationAotProcessor}.
 *
 * @author Mark Paluch
 * @since 4.0
 */
class CassandraManagedTypesBeanRegistrationAotProcessor extends ManagedTypesBeanRegistrationAotProcessor {

	public CassandraManagedTypesBeanRegistrationAotProcessor() {
		setModuleIdentifier("cassandra");
	}

	protected boolean matchesByType(@Nullable Class<?> beanType) {
		return beanType != null && ClassUtils.isAssignable(CassandraManagedTypes.class, beanType);
	}

	/**
	 * Type filters to exclude Cassandra driver types.
	 */
	static class CassandraTypeFilters implements TypeCollector.TypeCollectorFilters {

		private static final Predicate<Class<?>> CLASS_FILTER = it -> TypeUtils.type(it).isPartOf(
				"org.springframework.data.cassandra.core", "org.springframework.data.cassandra.repository",
				"org.apache.cassandra", "com.datastax");

		@Override
		public Predicate<Class<?>> classPredicate() {
			return CLASS_FILTER.negate();
		}

		@Override
		public Predicate<Field> fieldPredicate() {
			return Predicates.<Field> declaringClass(CLASS_FILTER).negate();
		}

		@Override
		public Predicate<Method> methodPredicate() {
			return Predicates.<Method> declaringClass(CLASS_FILTER).negate();
		}

	}

}
