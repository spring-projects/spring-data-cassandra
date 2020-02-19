/*
 * Copyright 2016-2020 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import org.springframework.data.util.TypeInformation;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * {@link org.springframework.data.mapping.PersistentEntity} for a mapped user-defined type (UDT). A mapped UDT consists
 * of a set of fields. Each field requires a data type that can be either a simple Cassandra type or an UDT.
 *
 * @author Mark Paluch
 * @since 1.5
 * @see UserDefinedType
 */
public class CassandraUserTypePersistentEntity<T> extends BasicCassandraPersistentEntity<T> {

	/**
	 * Create a new {@link CassandraUserTypePersistentEntity}.
	 *
	 * @param typeInformation must not be {@literal null}.
	 * @param verifier must not be {@literal null}.
	 */
	public CassandraUserTypePersistentEntity(TypeInformation<T> typeInformation,
			CassandraPersistentEntityMetadataVerifier verifier) {

		super(typeInformation, verifier);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity#determineTableName()
	 */
	@Override
	protected CqlIdentifier determineTableName() {

		UserDefinedType annotation = findAnnotation(UserDefinedType.class);

		if (annotation != null) {
			return determineName(annotation.value(), annotation.forceQuote());
		}

		return IdentifierFactory.create(getNamingStrategy().getUserDefinedTypeName(this), false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity#isUserDefinedType()
	 */
	@Override
	public boolean isUserDefinedType() {
		return true;
	}
}
