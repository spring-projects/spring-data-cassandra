/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.cassandra.core.convert;

import org.springframework.data.cassandra.core.CassandraDataObject;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.convert.EntityConverter;
import org.springframework.data.convert.EntityReader;

/**
 * Central Mongo specific converter interface which combines
 * {@link CassandraWriter} and {@link MongoReader}. v
 * 
 * @author Brian O'Neill
 */
public interface CassandraConverter
		extends
		EntityConverter<CassandraPersistentEntity<?>, CassandraPersistentProperty, Object, CassandraDataObject>,
		CassandraWriter<Object>, EntityReader<Object, CassandraDataObject> {

}
