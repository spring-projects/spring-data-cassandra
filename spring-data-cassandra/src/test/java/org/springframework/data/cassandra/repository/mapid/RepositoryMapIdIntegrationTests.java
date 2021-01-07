/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.repository.mapid;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.core.mapping.BasicMapId.*;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.core.mapping.BasicMapId;
import org.springframework.data.cassandra.core.mapping.MapId;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Integration tests for repositories using {@link MapId}.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 */
@SpringJUnitConfig
class RepositoryMapIdIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = RepositoryMapIdIntegrationTests.class)
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { SinglePrimaryKeyColumn.class.getPackage().getName() };
		}
	}

	@Autowired CassandraOperations template;
	@Autowired SinglePrimaryKecColumnRepository singlePrimaryKeyColumnRepository;
	@Autowired MultiPrimaryKeyColumnsRepository multiPrimaryKeyColumnsRepository;

	@BeforeEach
	void before() {
		assertThat(template).isNotNull();
		assertThat(singlePrimaryKeyColumnRepository).isNotNull();
		assertThat(multiPrimaryKeyColumnsRepository).isNotNull();
	}

	@Test
	void testSinglePkc() {

		// insert
		SinglePrimaryKeyColumn inserted = new SinglePrimaryKeyColumn(uuid());
		inserted.setValue(uuid());
		SinglePrimaryKeyColumn saved = singlePrimaryKeyColumnRepository.save(inserted);
		assertThat(inserted).isSameAs(saved);

		// select
		MapId id = id("key", saved.getKey());
		SinglePrimaryKeyColumn selected = singlePrimaryKeyColumnRepository.findById(id).get();
		assertThat(saved).isNotSameAs(selected);
		assertThat(selected.getKey()).isEqualTo(saved.getKey());
		assertThat(selected.getValue()).isEqualTo(saved.getValue());

		List<SinglePrimaryKeyColumn> allById = singlePrimaryKeyColumnRepository.findAllById(Collections.singletonList(id));

		assertThat(allById).containsOnly(saved);

		// update
		selected.setValue(uuid());
		SinglePrimaryKeyColumn updated = singlePrimaryKeyColumnRepository.save(selected);
		assertThat(selected).isSameAs(updated);

		selected = singlePrimaryKeyColumnRepository.findById(id).get();
		assertThat(updated).isNotSameAs(selected);
		assertThat(selected.getValue()).isEqualTo(updated.getValue());

		// delete
		singlePrimaryKeyColumnRepository.delete(selected);
		assertThat(singlePrimaryKeyColumnRepository.findById(id)).isEmpty();
	}

	@Test // DATACASS-661
	void findAllByIdRejectsEmptyMapId() {
		assertThatThrownBy(() -> multiPrimaryKeyColumnsRepository.findAllById(Collections.singletonList(BasicMapId.id())))
				.isInstanceOf(InvalidDataAccessApiUsageException.class);
	}

	@Test
	void testMultiPkc() {

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

		assertThatThrownBy(() -> multiPrimaryKeyColumnsRepository.findAllById(Collections.singletonList(id)))
				.isInstanceOf(InvalidDataAccessApiUsageException.class);

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
