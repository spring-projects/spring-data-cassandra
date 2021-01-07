/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.core.mapping;

import org.springframework.data.mapping.MappingException;

/**
 * Interface for Cassandra Persistent Entity Mapping Verification.
 *
 * @author David Webb
 * @author Mark Paluch
 */
@FunctionalInterface
public interface CassandraPersistentEntityMetadataVerifier {

	/**
	 * Performs verification on the Persistent Entity to ensure all markers and marker combinations are valid.
	 *
	 * @param entity the entity to verify, must not be {@literal null}.
	 */
	void verify(CassandraPersistentEntity<?> entity) throws MappingException;
}
