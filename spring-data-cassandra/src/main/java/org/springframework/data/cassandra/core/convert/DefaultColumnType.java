/*
 * Copyright 2020-present the original author or authors.
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
package org.springframework.data.cassandra.core.convert;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * Default {@link ColumnType} implementation.
 *
 * @author Mark Paluch
 * @since 3.0
 */
class DefaultColumnType implements ColumnType {

	public static final DefaultColumnType OBJECT = new DefaultColumnType(TypeInformation.OBJECT);

	private final TypeInformation<?> typeInformation;
	private final List<ColumnType> parameters;

	DefaultColumnType(TypeInformation<?> typeInformation, ColumnType... parameters) {
		this.typeInformation = typeInformation;
		this.parameters = Arrays.asList(parameters);
	}

	List<ColumnType> getParameters() {
		return parameters;
	}

	@Override
	public Class<?> getType() {
		return typeInformation.getType();
	}

	@Override
	public boolean isCollectionLike() {
		return typeInformation.isCollectionLike();
	}

	@Override
	public boolean isList() {
		return List.class.isAssignableFrom(typeInformation.getType());
	}

	@Override
	public boolean isSet() {
		return Set.class.isAssignableFrom(typeInformation.getType());
	}

	@Override
	public boolean isMap() {
		return typeInformation.isMap();
	}

	@Nullable
	@Override
	public ColumnType getComponentType() {
		return !parameters.isEmpty() ? parameters.get(0) : null;
	}

	@Nullable
	@Override
	public ColumnType getMapValueType() {
		return parameters.size() > 1 ? parameters.get(1) : null;
	}

	@Override
	public String toString() {

		if (parameters.isEmpty()) {
			return getType().getName();
		}

		return String.format("%s<%s>", getType().getName(),
				parameters.stream().map(Object::toString).collect(Collectors.toList()));
	}
}
