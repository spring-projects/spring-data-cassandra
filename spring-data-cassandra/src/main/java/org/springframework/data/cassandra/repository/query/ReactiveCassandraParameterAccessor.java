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
package org.springframework.data.cassandra.repository.query;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoProcessor;

import java.util.ArrayList;
import java.util.List;

import org.springframework.data.repository.util.ReactiveWrapperConverters;
import org.springframework.data.repository.util.ReactiveWrappers;

/**
 * Reactive {@link org.springframework.data.repository.query.ParametersParameterAccessor} implementation that subscribes
 * to reactive parameter wrapper types upon creation. This class performs synchronization when acessing parameters.
 *
 * @author Mark Paluch
 * @since 2.0
 */
class ReactiveCassandraParameterAccessor extends CassandraParametersParameterAccessor {

	private final Object[] values;

	private final List<MonoProcessor<?>> subscriptions;

	@SuppressWarnings("ConstantConditions")
	ReactiveCassandraParameterAccessor(CassandraQueryMethod method, Object[] values) {

		super(method, values);

		this.values = values;
		this.subscriptions = new ArrayList<>(values.length);

		for (Object value : values) {
			if (value == null || !ReactiveWrappers.supports(value.getClass())) {
				subscriptions.add(null);
				continue;
			}

			if (ReactiveWrappers.isSingleValueType(value.getClass())) {
				subscriptions.add(ReactiveWrapperConverters.toWrapper(value, Mono.class).toProcessor());
			} else {
				subscriptions.add(ReactiveWrapperConverters.toWrapper(value, Flux.class).collectList().toProcessor());
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParametersParameterAccessor#getValue(int)
	 */
	@SuppressWarnings({ "unchecked", "ConstantConditions" })
	@Override
	protected <T> T getValue(int index) {
		return (subscriptions.get(index) != null ? (T) subscriptions.get(index).block() : super.getValue(index));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.repository.query.CassandraParametersParameterAccessor#getValues()
	 */
	@Override
	public Object[] getValues() {

		Object[] result = new Object[values.length];

		for (int index = 0; index < result.length; index++) {
			result[index] = getValue(index);
		}

		return result;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.ParametersParameterAccessor#getBindableValue(int)
	 */
	public Object getBindableValue(int index) {
		return getValue(getParameters().getBindableParameter(index).getIndex());
	}
}
