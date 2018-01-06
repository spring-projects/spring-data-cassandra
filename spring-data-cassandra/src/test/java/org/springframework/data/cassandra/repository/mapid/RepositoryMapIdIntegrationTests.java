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
package org.springframework.data.cassandra.repository.mapid;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.core.mapping.BasicMapId.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.mapping.MapId;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Integration tests for repositories using {@link MapId}.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
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
		assertThat(template).isNotNull();
		assertThat(singlePrimaryKecColumnRepository).isNotNull();
		assertThat(multiPrimaryKeyColumnsRepository).isNotNull();
	}

	@Test
	public void testSinglePkc() {

		// insert
		SinglePrimaryKeyColumn inserted = new SinglePrimaryKeyColumn(uuid());
		inserted.setValue(uuid());
		SinglePrimaryKeyColumn saved = singlePrimaryKecColumnRepository.save(inserted);
		assertThat(inserted).isSameAs(saved);

		// select
		MapId id = id("key", saved.getKey());
		SinglePrimaryKeyColumn selected = singlePrimaryKecColumnRepository.findById(id).get();
		assertThat(saved).isNotSameAs(selected);
		assertThat(selected.getKey()).isEqualTo(saved.getKey());
		assertThat(selected.getValue()).isEqualTo(saved.getValue());

		// update
		selected.setValue(uuid());
		SinglePrimaryKeyColumn updated = singlePrimaryKecColumnRepository.save(selected);
		assertThat(selected).isSameAs(updated);

		selected = singlePrimaryKecColumnRepository.findById(id).get();
		assertThat(updated).isNotSameAs(selected);
		assertThat(selected.getValue()).isEqualTo(updated.getValue());

		// delete
		singlePrimaryKecColumnRepository.delete(selected);
		assertThat(singlePrimaryKecColumnRepository.findById(id)).isEmpty();
	}

	@Test
	public void testMultiPkc() {

		// insert
		MultiPrimaryKeyColumns inserted = new MultiPrimaryKeyColumns(uuid(), uuid());
		inserted.setValue(uuid());
		MultiPrimaryKeyColumns saved = multiPrimaryKeyColumnsRepository.save(inserted);
		assertThat(inserted).isSameAs(saved);

		// select
		MapId id = id("key0", saved.getKey0()).with("key1", saved.getKey1());
		MultiPrimaryKeyColumns selected = multiPrimaryKeyColumnsRepository.findById(id).get();
		assertThat(saved).isNotSameAs(selected);
		assertThat(selected.getKey0()).isEqualTo(saved.getKey0());
		assertThat(selected.getKey1()).isEqualTo(saved.getKey1());
		assertThat(selected.getValue()).isEqualTo(saved.getValue());

		// update
		selected.setValue(uuid());
		MultiPrimaryKeyColumns updated = multiPrimaryKeyColumnsRepository.save(selected);
		assertThat(selected).isSameAs(updated);

		selected = multiPrimaryKeyColumnsRepository.findById(id).get();
		assertThat(updated).isNotSameAs(selected);
		assertThat(selected.getValue()).isEqualTo(updated.getValue());

		// delete
		template.delete(selected);
		assertThat(multiPrimaryKeyColumnsRepository.findById(id)).isEmpty();
	}

}
