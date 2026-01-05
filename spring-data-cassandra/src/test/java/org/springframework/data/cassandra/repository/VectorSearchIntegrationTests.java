/*
 * Copyright 2016-present the original author or authors.
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
package org.springframework.data.cassandra.repository;

import static org.assertj.core.api.Assertions.*;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.PersistenceCreator;
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.core.mapping.SaiIndexed;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.mapping.VectorType;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.ScoringFunction;
import org.springframework.data.domain.SearchResult;
import org.springframework.data.domain.SearchResults;
import org.springframework.data.domain.Similarity;
import org.springframework.data.domain.Vector;
import org.springframework.data.domain.VectorScoringFunctions;
import org.springframework.data.repository.CrudRepository;
import org.springframework.data.repository.query.QueryCreationException;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Integration tests for Vector Search using repositories.
 *
 * @author Mark Paluch
 */
@SpringJUnitConfig
class VectorSearchIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	Vector VECTOR = Vector.of(0.2001f, 0.32345f, 0.43456f, 0.54567f, 0.65678f);

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = VectorSearchRepository.class, considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(classes = VectorSearchRepository.class, type = FilterType.ASSIGNABLE_TYPE))
	public static class Config extends IntegrationTestConfig {

		@Override
		protected Set<Class<?>> getInitialEntitySet() {
			return Collections.singleton(WithVectorFields.class);
		}

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.RECREATE_DROP_UNUSED;
		}
	}

	@Autowired VectorSearchRepository repository;

	@BeforeEach
	void setUp() {

		repository.deleteAll();

		WithVectorFields w1 = new WithVectorFields("de", "one", Vector.of(0.1001f, 0.22345f, 0.33456f, 0.44567f, 0.55678f));
		WithVectorFields w2 = new WithVectorFields("de", "two", Vector.of(0.2001f, 0.32345f, 0.43456f, 0.54567f, 0.65678f));
		WithVectorFields w3 = new WithVectorFields("en", "three",
				Vector.of(0.9001f, 0.82345f, 0.73456f, 0.64567f, 0.55678f));
		WithVectorFields w4 = new WithVectorFields("de", "four",
				Vector.of(0.9001f, 0.92345f, 0.93456f, 0.94567f, 0.95678f));

		repository.saveAll(List.of(w1, w2, w3, w4));
	}

	@Test // GH-1573
	void searchWithoutScoringFunctionShouldFail() {
		assertThatExceptionOfType(QueryCreationException.class)
				.isThrownBy(() -> repository.searchByEmbeddingNear(VECTOR, Limit.of(100)));
	}

	@Test // GH-1573
	void shouldConsiderScoringFunction() {

		SearchResults<WithVectorFields> results = repository.searchByEmbeddingNearAndCountry(VECTOR,
				ScoringFunction.dotProduct(), "de", Limit.of(100));

		assertThat(results).hasSize(3);

		for (SearchResult<WithVectorFields> result : results) {
			assertThat(result.getScore()).isInstanceOf(Similarity.class);
			assertThat(result.getScore().getValue()).isNotCloseTo(0d, offset(0.1d));
		}

		results = repository.searchByEmbeddingNearAndCountry(VECTOR, VectorScoringFunctions.EUCLIDEAN, "de", Limit.of(100));

		assertThat(results).hasSize(3);

		for (SearchResult<WithVectorFields> result : results) {

			assertThat(result.getScore()).isInstanceOf(Similarity.class);
			assertThat(result.getScore().getValue()).isNotCloseTo(0.3d, offset(0.1d));
		}
	}

	@Test // GH-1573
	void shouldRunAnnotatedSearchByVector() {

		SearchResults<WithVectorFields> results = repository.searchAnnotatedByEmbeddingNear(VECTOR, "de", Limit.of(100));

		assertThat(results).hasSize(3);
		for (SearchResult<WithVectorFields> result : results) {
			assertThat(result.getScore()).isInstanceOf(Similarity.class);
			assertThat(result.getScore().getValue()).isNotCloseTo(0d, offset(0.1d));
		}
	}

	@Test // GH-1573
	void shouldFindByVector() {

		List<WithVectorFields> result = repository.findByEmbeddingNear(VECTOR, Limit.of(100));

		assertThat(result).hasSize(4);
	}

	interface VectorSearchRepository extends CrudRepository<WithVectorFields, UUID> {

		SearchResults<WithVectorFields> searchByEmbeddingNearAndCountry(Vector embedding, ScoringFunction function,
				String country, Limit limit);

		SearchResults<WithVectorFields> searchByEmbeddingNear(Vector embedding, Limit limit);

		List<WithVectorFields> findByEmbeddingNear(Vector embedding, Limit limit);

		@Query("""
				SELECT id,description,country,similarity_cosine(embedding,:embedding) AS score
				FROM withvectorfields
				WHERE country = :country
				ORDER BY embedding ANN OF :embedding
				LIMIT :limit
				""")
		SearchResults<WithVectorFields> searchAnnotatedByEmbeddingNear(Vector embedding, String country, Limit limit);

	}

	@Table
	static class WithVectorFields {

		@Id String id;
		@SaiIndexed String country;
		String description;

		@VectorType(dimensions = 5)
		@SaiIndexed Vector embedding;

		@PersistenceCreator
		public WithVectorFields(String id, String country, String description, Vector embedding) {
			this.id = id;
			this.country = country;
			this.description = description;
			this.embedding = embedding;
		}

		public WithVectorFields(String country, String description, Vector embedding) {
			this.id = UUID.randomUUID().toString();
			this.country = country;
			this.description = description;
			this.embedding = embedding;
		}

		public String getId() {
			return id;
		}

		public String getCountry() {
			return country;
		}

		public String getDescription() {
			return description;
		}

		public Vector getEmbedding() {
			return embedding;
		}

		@Override
		public String toString() {
			return "WithVectorFields{" + "id='" + id + '\'' + ", country='" + country + '\'' + ", description='" + description
					+ '\'' + '}';
		}

	}

}
