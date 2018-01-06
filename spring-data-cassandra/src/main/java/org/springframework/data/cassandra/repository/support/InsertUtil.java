/*
 * Copyright 2017-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.repository.support;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import lombok.experimental.UtilityClass;

import org.springframework.data.cassandra.core.convert.CassandraConverter;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * Utility to create {@link com.datastax.driver.core.querybuilder.Insert} statements for repository use.
 *
 * @author Mark Paluch
 * @since 2.0
 */
@UtilityClass
class InsertUtil {

	/**
	 * Create a {@link Insert} statement containing all properties including these with {@literal null} values.
	 *
	 * @param entity the entity, must not be {@literal null}.
	 * @return the constructed {@link Insert} statement.
	 */
	static Insert createInsert(CassandraConverter converter, Object entity) {

		CassandraPersistentEntity<?> persistentEntity = converter.getMappingContext()
				.getRequiredPersistentEntity(entity.getClass());

		Map<String, Object> toInsert = new LinkedHashMap<>();

		converter.write(entity, toInsert, persistentEntity);

		Insert insert = QueryBuilder.insertInto(persistentEntity.getTableName().toCql());

		for (Entry<String, Object> entry : toInsert.entrySet()) {
			insert.value(entry.getKey(), entry.getValue());
		}

		return insert;
	}
}
