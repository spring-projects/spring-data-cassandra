/*
 * Copyright 2022 the original author or authors.
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

import java.util.Arrays;

import org.springframework.aot.hint.MemberCategory;
import org.springframework.aot.hint.RuntimeHintsRegistrar;
import org.springframework.aot.hint.TypeReference;
import org.springframework.data.cassandra.core.mapping.event.BeforeConvertCallback;
import org.springframework.data.cassandra.core.mapping.event.BeforeSaveCallback;
import org.springframework.data.cassandra.core.mapping.event.ReactiveBeforeConvertCallback;
import org.springframework.data.cassandra.core.mapping.event.ReactiveBeforeSaveCallback;
import org.springframework.data.cassandra.repository.support.SimpleCassandraRepository;
import org.springframework.data.cassandra.repository.support.SimpleReactiveCassandraRepository;
import org.springframework.data.repository.util.ReactiveWrappers;
import org.springframework.lang.Nullable;

/**
 * {@link RuntimeHintsRegistrar} for repository types and entity callbacks.
 *
 * @author Mark Paluch
 * @since 4.0
 */
class CassandraRuntimeHintsRegistrar implements RuntimeHintsRegistrar {

	private static final boolean PROJECT_REACTOR_PRESENT = ReactiveWrappers
			.isAvailable(ReactiveWrappers.ReactiveLibrary.PROJECT_REACTOR);

	@Override
	public void registerHints(org.springframework.aot.hint.RuntimeHints hints, @Nullable ClassLoader classLoader) {

		hints.reflection().registerTypes(Arrays.asList(TypeReference.of(SimpleCassandraRepository.class), //
				TypeReference.of(BeforeConvertCallback.class), //
				TypeReference.of(BeforeSaveCallback.class)),
				builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
						MemberCategory.INVOKE_PUBLIC_METHODS));

		if (PROJECT_REACTOR_PRESENT) {

			hints.reflection().registerTypes(Arrays.asList(TypeReference.of(SimpleReactiveCassandraRepository.class), //
					TypeReference.of(ReactiveBeforeConvertCallback.class), //
					TypeReference.of(ReactiveBeforeSaveCallback.class)),
					builder -> builder.withMembers(MemberCategory.INVOKE_DECLARED_CONSTRUCTORS,
							MemberCategory.INVOKE_PUBLIC_METHODS));
		}
	}
}
