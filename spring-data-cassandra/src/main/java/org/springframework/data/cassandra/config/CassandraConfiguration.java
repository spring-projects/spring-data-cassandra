/*
 * Copyright 2026-present the original author or authors.
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
package org.springframework.data.cassandra.config;

import java.util.List;
import java.util.Set;

import org.jspecify.annotations.Nullable;

import org.springframework.data.cassandra.CassandraManagedTypes;
import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.UserTypeResolver;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.ClassUtils;

import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;

/**
 * Utility class to create Cassandra related infrastructure components.
 *
 * @author Mark Paluch
 * @since 5.0.4
 */
class CassandraConfiguration {

	/**
	 * Scans for the initial entity set.
	 *
	 * @param beanClassLoader the class loader to use, can be {@literal null} to default to
	 *          {@link ClassUtils#getDefaultClassLoader}.
	 * @param basePackages array of base packages to scan for entities. Must not be {@literal null} or empty.
	 * @return set of initial entity classes.
	 */
	public static Set<Class<?>> getInitialEntitySet(@Nullable ClassLoader beanClassLoader, String... basePackages) {

		CassandraEntityClassScanner scanner = new CassandraEntityClassScanner();
		scanner.setBeanClassLoader(beanClassLoader);
		scanner.setEntityBasePackages(List.of(basePackages));

		return scanner.scanForEntityClasses();
	}

	/**
	 * Create a {@link MappingContext} instance to map entities to {@link Object Java Objects}.
	 */
	public static CassandraMappingContext createMappingContext(CassandraManagedTypes managedTypes,
			CustomConversions customConversions) {

		CassandraMappingContext mappingContext = new CassandraMappingContext();

		mappingContext.setManagedTypes(managedTypes);
		mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());

		return mappingContext;
	}

	/**
	 * Creates a {@link CassandraConverter}.
	 *
	 * @return a new {@link CassandraConverter}.
	 */
	public static CassandraConverter createConverter(CustomConversions customConversions,
			CassandraMappingContext mappingContext, UserTypeResolver resolver, CodecRegistry codecRegistry) {

		MappingCassandraConverter converter = new MappingCassandraConverter(mappingContext);

		converter.setCodecRegistry(codecRegistry);
		converter.setUserTypeResolver(resolver);
		converter.setCustomConversions(customConversions);

		return converter;
	}

}
