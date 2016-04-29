/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.data.cassandra.test.integration.mapping.mapid.repo;

import static org.junit.Assert.*;
import static org.springframework.data.cassandra.repository.support.BasicMapId.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.repository.MapId;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.test.integration.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration tests for repositories using {@link MapId}.
 *
 * @author Matthew T. Adams.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class RepositoryMapIdIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = RepositoryMapIdIntegrationTests.class)
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { SinglePrimaryKeyColumn.class.getPackage().getName() };
		}
	}

	@Autowired CassandraOperations template;
	@Autowired SinglePrimaryKecColumnRepository singlePrimaryKecColumnRepository;
	@Autowired MultiPrimaryKeyColumnsRepository multiPrimaryKeyColumnsRepository;

	@Before
	public void before() {
		assertNotNull(template);
		assertNotNull(singlePrimaryKecColumnRepository);
		assertNotNull(multiPrimaryKeyColumnsRepository);
	}

	@Test
	public void testSinglePkc() {

		// insert
		SinglePrimaryKeyColumn inserted = new SinglePrimaryKeyColumn(uuid());
		inserted.setValue(uuid());
		SinglePrimaryKeyColumn saved = singlePrimaryKecColumnRepository.save(inserted);
		assertSame(saved, inserted);

		// select
		MapId id = id("key", saved.getKey());
		SinglePrimaryKeyColumn selected = singlePrimaryKecColumnRepository.findOne(id);
		assertNotSame(selected, saved);
		assertEquals(saved.getKey(), selected.getKey());
		assertEquals(saved.getValue(), selected.getValue());

		// update
		selected.setValue(uuid());
		SinglePrimaryKeyColumn updated = singlePrimaryKecColumnRepository.save(selected);
		assertSame(updated, selected);

		selected = singlePrimaryKecColumnRepository.findOne(id);
		assertNotSame(selected, updated);
		assertEquals(updated.getValue(), selected.getValue());

		// delete
		singlePrimaryKecColumnRepository.delete(selected);
		assertNull(singlePrimaryKecColumnRepository.findOne(id));
	}

	@Test
	public void testMultiPkc() {

		// insert
		MultiPrimaryKeyColumns inserted = new MultiPrimaryKeyColumns(uuid(), uuid());
		inserted.setValue(uuid());
		MultiPrimaryKeyColumns saved = multiPrimaryKeyColumnsRepository.save(inserted);
		assertSame(saved, inserted);

		// select
		MapId id = id("key0", saved.getKey0()).with("key1", saved.getKey1());
		MultiPrimaryKeyColumns selected = multiPrimaryKeyColumnsRepository.findOne(id);
		assertNotSame(selected, saved);
		assertEquals(saved.getKey0(), selected.getKey0());
		assertEquals(saved.getKey1(), selected.getKey1());
		assertEquals(saved.getValue(), selected.getValue());

		// update
		selected.setValue(uuid());
		MultiPrimaryKeyColumns updated = multiPrimaryKeyColumnsRepository.save(selected);
		assertSame(updated, selected);

		selected = multiPrimaryKeyColumnsRepository.findOne(id);
		assertNotSame(selected, updated);
		assertEquals(updated.getValue(), selected.getValue());

		// delete
		template.delete(selected);
		assertNull(multiPrimaryKeyColumnsRepository.findOne(id));
	}

}
