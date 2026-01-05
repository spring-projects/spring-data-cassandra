/*
 * Copyright 2022-present the original author or authors.
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

import org.junit.jupiter.api.Test;
import org.springframework.aot.generate.ClassNameGenerator;
import org.springframework.aot.generate.DefaultGenerationContext;
import org.springframework.aot.generate.InMemoryGeneratedFiles;
import org.springframework.data.cassandra.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.cassandra.core.mapping.event.BeforeSaveCallback;
import org.springframework.data.cassandra.core.mapping.event.ReactiveBeforeConvertCallback;
import org.springframework.data.cassandra.core.mapping.event.ReactiveBeforeSaveCallback;
import org.springframework.data.cassandra.repository.support.SimpleCassandraRepository;
import org.springframework.data.cassandra.repository.support.SimpleReactiveCassandraRepository;
import org.springframework.javapoet.ClassName;

/**
 * Unit tests for {@link CassandraRuntimeHints}.
 *
 * @author Mark Paluch
 */
class CassandraRuntimeHintsUnitTests {

	@Test // GH-1280
	void shouldRegisterCassandraHints() {

		CassandraRuntimeHints registrar = new CassandraRuntimeHints();

		DefaultGenerationContext context = new DefaultGenerationContext(new ClassNameGenerator(ClassName.get(Object.class)),
				new InMemoryGeneratedFiles());
		registrar.registerHints(context.getRuntimeHints(), null);

		new CodeContributionAssert(context).contributesReflectionFor(SimpleCassandraRepository.class,
				SimpleReactiveCassandraRepository.class);
		new CodeContributionAssert(context).contributesReflectionFor(BeforeConvertCallback.class, BeforeSaveCallback.class,
				ReactiveBeforeConvertCallback.class, ReactiveBeforeSaveCallback.class);
	}
}
