/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.mapping;

import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.util.TypeInformation;

/**
 * Interface for Cassandra Persistent Entity Mapping Verification.
 * 
 * @author David Webb
 */
public interface CassandraPersistentEntityMetadataVerifier {

	/**
	 * Performs verification on the Persistent Entity to ensure all markers and marker combinations are valid.
	 * 
	 * @param entity
	 */
	void verify(CassandraPersistentEntity<?> entity) throws MappingException;

	/**
	 * Checks if given entity is actually persistent.
	 * @param entity
	 * @return true if entity is peristent (a table or a primary key class)
	 */
	boolean isPersistent(TypeInformation<?> entity);
	
}
