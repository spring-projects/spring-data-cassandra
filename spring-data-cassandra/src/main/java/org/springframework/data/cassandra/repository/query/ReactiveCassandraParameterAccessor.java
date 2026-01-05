/*
 * Copyright 2016-present the original author or authors.
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
package org.springframework.data.cassandra.repository.query;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.Nullable;
import org.reactivestreams.Publisher;

import org.springframework.data.core.ReactiveWrappers;
import org.springframework.data.repository.util.ReactiveWrapperConverters;

/**
 * Reactive {@link org.springframework.data.repository.query.ParametersParameterAccessor} implementation that subscribes
 * to reactive parameter wrapper types upon creation. This class performs synchronization when accessing parameters.
 *
 * @author Mark Paluch
 * @since 2.0
 */
class ReactiveCassandraParameterAccessor extends CassandraParametersParameterAccessor {

	private final @Nullable Object[] values;
	private final CassandraQueryMethod method;

	ReactiveCassandraParameterAccessor(CassandraQueryMethod method, @Nullable Object[] values) {

		super(method, values);
		this.method = method;
		this.values = values;
	}

	@Override
	public @Nullable Object[] getValues() {

		@Nullable
		Object[] result = new Object[values.length];

		for (int index = 0; index < result.length; index++) {
			result[index] = getValue(index);
		}

		return result;
	}

	public @Nullable Object getBindableValue(int index) {
		return getValue(getParameters().getBindableParameter(index).getIndex());
	}

	/**
	 * Resolve parameters that were provided through reactive wrapper types. Flux is collected into a list, values from
	 * Mono's are used directly.
	 *
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public Mono<ReactiveCassandraParameterAccessor> resolveParameters() {

		boolean hasReactiveWrapper = false;

		for (Object value : values) {
			if (value == null || !ReactiveWrappers.supports(value.getClass())) {
				continue;
			}

			hasReactiveWrapper = true;
			break;
		}

		if (!hasReactiveWrapper) {
			return Mono.just(this);
		}

		@Nullable
		Object[] resolved = new Object[values.length];
		Map<Integer, Optional<?>> holder = new ConcurrentHashMap<>();
		List<Publisher<?>> publishers = new ArrayList<>();

		for (int i = 0; i < values.length; i++) {

			Object value = resolved[i] = values[i];
			if (value == null || !ReactiveWrappers.supports(value.getClass())) {
				continue;
			}

			if (ReactiveWrappers.isSingleValueType(value.getClass())) {

				int index = i;
				publishers.add(ReactiveWrapperConverters.toWrapper(value, Mono.class) //
						.map(Optional::of) //
						.defaultIfEmpty(Optional.empty()) //
						.doOnNext(it -> holder.put(index, (Optional<?>) it)));
			} else {

				int index = i;
				publishers.add(ReactiveWrapperConverters.toWrapper(value, Flux.class) //
						.collectList() //
						.doOnNext(it -> holder.put(index, Optional.of(it))));
			}
		}

		return Flux.merge(publishers).then().thenReturn(resolved).map(values -> {
			holder.forEach((index, v) -> values[index] = v.orElse(null));
			return new ReactiveCassandraParameterAccessor(method, values);
		});
	}

}
