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
package org.springframework.data.cassandra.repository.aot;

import java.util.function.Predicate;
import org.jspecify.annotations.Nullable;
import org.springframework.aot.generate.GenerationContext;
import org.springframework.beans.factory.aot.BeanRegistrationAotProcessor;
import org.springframework.data.aot.AotContext;
import org.springframework.data.cassandra.core.mapping.CassandraSimpleTypeHolder;
import org.springframework.data.repository.aot.generate.RepositoryContributor;
import org.springframework.data.repository.config.AotRepositoryContext;
import org.springframework.data.repository.config.RepositoryRegistrationAotProcessor;
import org.springframework.data.util.TypeContributor;
import org.springframework.data.util.TypeUtils;

/**
 * Cassandra specific {@link BeanRegistrationAotProcessor AOT processor}.
 *
 * @author Chris Bono
 * @since 5.0
 */
public class CassandraRepositoryRegistrationAotProcessor extends RepositoryRegistrationAotProcessor {

	private static final Predicate<Class<?>> IS_SIMPLE_TYPE = (type) -> CassandraSimpleTypeHolder.HOLDER.isSimpleType(type);

	@Override
	protected @Nullable RepositoryContributor contribute(AotRepositoryContext repositoryContext, GenerationContext generationContext) {

		super.contribute(repositoryContext, generationContext);

		repositoryContext.getResolvedTypes().stream()
				.filter(IS_SIMPLE_TYPE.negate())
				.forEach(type -> TypeContributor.contribute(type, (__) -> true, generationContext));

		boolean enabled = Boolean.parseBoolean(
				repositoryContext.getEnvironment().getProperty(AotContext.GENERATED_REPOSITORIES_ENABLED, "false"));

		return enabled ? new CassandraRepositoryContributor(repositoryContext) : null;
	}

	@Override
	protected void contributeType(Class<?> type, GenerationContext generationContext) {

		if (TypeUtils.type(type).isPartOf("org.springframework.data.cassandra", "org.apache.cassandra", "com.datastax")) {
			return;
		}

		super.contributeType(type, generationContext);
	}
}
