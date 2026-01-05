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

import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

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
import org.springframework.data.cassandra.repository.config.EnableReactiveCassandraRepositories;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.ScoringFunction;
import org.springframework.data.domain.SearchResult;
import org.springframework.data.domain.Similarity;
import org.springframework.data.domain.Vector;
import org.springframework.data.domain.VectorScoringFunctions;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Integration tests for Vector Search using reactive repositories.
 *
 * @author Mark Paluch
 */
@SpringJUnitConfig
class ReactiveVectorSearchIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	Vector VECTOR = Vector.of(0.2001f, 0.32345f, 0.43456f, 0.54567f, 0.65678f);

	@Configuration
	@EnableReactiveCassandraRepositories(basePackageClasses = ReactiveVectorSearchRepository.class,
			considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(classes = ReactiveVectorSearchRepository.class,
					type = FilterType.ASSIGNABLE_TYPE))
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

	@Autowired ReactiveVectorSearchRepository repository;

	@BeforeEach
	void setUp() {

		repository.deleteAll().as(StepVerifier::create).verifyComplete();

		WithVectorFields w1 = new WithVectorFields("de", "one", Vector.of(0.1001f, 0.22345f, 0.33456f, 0.44567f, 0.55678f));
		WithVectorFields w2 = new WithVectorFields("de", "two", Vector.of(0.2001f, 0.32345f, 0.43456f, 0.54567f, 0.65678f));
		WithVectorFields w3 = new WithVectorFields("en", "three",
				Vector.of(0.9001f, 0.82345f, 0.73456f, 0.64567f, 0.55678f));
		WithVectorFields w4 = new WithVectorFields("de", "four",
				Vector.of(0.9001f, 0.92345f, 0.93456f, 0.94567f, 0.95678f));

		repository.saveAll(List.of(w1, w2, w3, w4)).as(StepVerifier::create).expectNextCount(4).verifyComplete();
	}

	@Test // GH-
	void shouldConsiderScoringFunction() {

		Vector vector = Vector.of(0.9f, 0.54f, 0.12f, 0.1f, 0.95f);

		List<SearchResult<WithVectorFields>> results = repository
				.searchByEmbeddingNear(vector, VectorScoringFunctions.COSINE, Limit.of(100)).collectList().block();

		assertThat(results).hasSize(4);
		for (SearchResult<WithVectorFields> result : results) {
			assertThat(result.getScore()).isInstanceOf(Similarity.class);
			assertThat(result.getScore().getValue()).isNotCloseTo(0d, offset(0.1d));
		}

		results = repository.searchByEmbeddingNear(VECTOR, VectorScoringFunctions.EUCLIDEAN, Limit.of(100)).collectList()
				.block();

		assertThat(results).hasSize(4);
		for (SearchResult<WithVectorFields> result : results) {
			assertThat(result.getScore()).isInstanceOf(Similarity.class);
			assertThat(result.getScore().getValue()).isNotCloseTo(0.3d, offset(0.1d));
		}
	}

	@Test // GH-
	void shouldRunAnnotatedSearchByVector() {

		List<SearchResult<WithVectorFields>> results = repository.searchAnnotatedByEmbeddingNear(VECTOR, Limit.of(100))
				.collectList().block();

		assertThat(results).hasSize(4);
		for (SearchResult<WithVectorFields> result : results) {
			assertThat(result.getScore()).isInstanceOf(Similarity.class);
			assertThat(result.getScore().getValue()).isNotCloseTo(0d, offset(0.1d));
		}
	}

	@Test // GH-
	void shouldFindByVector() {

		List<WithVectorFields> result = repository.findByEmbeddingNear(VECTOR, Limit.of(100)).collectList().block();

		assertThat(result).hasSize(4);
	}

	interface ReactiveVectorSearchRepository extends ReactiveCrudRepository<WithVectorFields, UUID> {

		Flux<SearchResult<WithVectorFields>> searchByEmbeddingNear(Vector embedding, ScoringFunction function, Limit limit);

		Flux<WithVectorFields> findByEmbeddingNear(Vector embedding, Limit limit);

		@Query("SELECT id,description,country,similarity_cosine(embedding,:embedding) AS score FROM withvectorfields ORDER BY embedding ANN OF :embedding LIMIT :limit")
		Flux<SearchResult<WithVectorFields>> searchAnnotatedByEmbeddingNear(Vector embedding, Limit limit);

	}

	@Table
	static class WithVectorFields {

		@Id String id;
		String country;
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
