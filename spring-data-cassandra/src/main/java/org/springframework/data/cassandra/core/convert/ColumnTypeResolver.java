/*
 * Copyright 2020 the original author or authors.
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

import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.CassandraType;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Resolves {@link ColumnType} for properties, {@link TypeInformation}, and {@code values}.
 *
 * @author Mark Paluch
 * @since 3.0
 */
public interface ColumnTypeResolver {

	/**
	 * Resolve a {@link CassandraColumnType} from a {@link CassandraPersistentProperty}. Considers
	 * {@link CassandraType}-annotated properties.
	 *
	 * @param property must not be {@literal null}.
	 * @return
	 * @see CassandraType
	 * @see CassandraPersistentProperty
	 * @throws org.springframework.dao.InvalidDataAccessApiUsageException
	 */
	default CassandraColumnType resolve(CassandraPersistentProperty property) {

		Assert.notNull(property, "Property must not be null");

		if (property.isAnnotationPresent(CassandraType.class)) {
			return resolve(property.getRequiredAnnotation(CassandraType.class));
		}

		return resolve(property.getTypeInformation());
	}

	/**
	 * Resolve a {@link CassandraColumnType} from {@link TypeInformation}. Considers potentially registered custom
	 * converters and simple type rules.
	 *
	 * @param typeInformation must not be {@literal null}.
	 * @return
	 * @see org.springframework.data.cassandra.core.mapping.CassandraSimpleTypeHolder
	 * @see CassandraCustomConversions
	 * @throws org.springframework.dao.InvalidDataAccessApiUsageException
	 */
	CassandraColumnType resolve(TypeInformation<?> typeInformation);

	/**
	 * Resolve a {@link CassandraColumnType} from a {@link CassandraType} annotation.
	 *
	 * @param annotation must not be {@literal null}.
	 * @return
	 * @see CassandraType
	 * @see CassandraPersistentProperty
	 * @throws org.springframework.dao.InvalidDataAccessApiUsageException
	 */
	CassandraColumnType resolve(CassandraType annotation);

	/**
	 * Resolve a {@link ColumnType} from a {@code value}. Considers potentially registered custom converters and simple
	 * type rules.
	 *
	 * @param value
	 * @return
	 * @see org.springframework.data.cassandra.core.mapping.CassandraSimpleTypeHolder
	 * @see CassandraCustomConversions
	 */
	ColumnType resolve(@Nullable Object value);
}
