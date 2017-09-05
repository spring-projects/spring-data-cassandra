/*
 * Copyright 2017 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.repository.MapId;
import org.springframework.data.util.ClassTypeInformation;

/**
 * Unit tests for class {@link MappingCassandraEntityInformation org.springframework.data.cassandra.repository.support.MappingCassandraEntityInformation}.
 *
 * @author Michael Hausegger, hausegger.michael@googlemail.com
 */
public class MappingCassandraEntityInformationTest {

	@Test(expected = IllegalArgumentException.class)  //DATACASS-405
	public void testGetIdDoesNotAcceptEntityIfNull() throws Exception {

		CassandraPersistentEntity cassandraPersistentEntity = new BasicCassandraPersistentEntity(ClassTypeInformation.from(this.getClass()));

		MappingCassandraEntityInformation mappingCassandraEntityInformation = new MappingCassandraEntityInformation(cassandraPersistentEntity, new MappingCassandraConverter());

		mappingCassandraEntityInformation.getId(null);
	}


	@Test  //DATACASS-405
	public void testGetIdType() throws Exception {

		CassandraPersistentEntity cassandraPersistentEntity = new BasicCassandraPersistentEntity(ClassTypeInformation.from(this.getClass()));

		MappingCassandraEntityInformation mappingCassandraEntityInformation = new MappingCassandraEntityInformation(cassandraPersistentEntity, new MappingCassandraConverter());

		assertThat(mappingCassandraEntityInformation.getIdType()).isEqualTo(MapId.class);
	}


	@Test  //DATACASS-405
	public void testGetTableName() throws Exception {

		CassandraPersistentEntity cassandraPersistentEntity = new BasicCassandraPersistentEntity(ClassTypeInformation.from(this.getClass()));

		MappingCassandraEntityInformation mappingCassandraEntityInformation = new MappingCassandraEntityInformation(cassandraPersistentEntity, new MappingCassandraConverter());

		assertThat(mappingCassandraEntityInformation.getTableName()).isEqualTo("mappingcassandraentityinformationtest");
	}
}