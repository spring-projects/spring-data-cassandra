/*
 * Copyright 2016-2025 the original author or authors.
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
import org.springframework.data.cassandra.config.SchemaAction;
import org.springframework.data.cassandra.core.mapping.Indexed;
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
import org.springframework.data.domain.Vector;
import org.springframework.data.domain.VectorScoringFunctions;
import org.springframework.data.repository.CrudRepository;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Integration tests for Vector Search using repositories.
 *
 * @author Mark Paluch
 */
@SpringJUnitConfig
class VectorSearchIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = CommentsRepository.class, considerNestedRepositories = true,
			includeFilters = @ComponentScan.Filter(classes = CommentsRepository.class, type = FilterType.ASSIGNABLE_TYPE))
	public static class Config extends IntegrationTestConfig {

		@Override
		protected Set<Class<?>> getInitialEntitySet() {
			return Collections.singleton(Comments.class);
		}

		@Override
		public SchemaAction getSchemaAction() {
			return SchemaAction.RECREATE_DROP_UNUSED;
		}
	}

	@Autowired CommentsRepository repository;

	@BeforeEach
	void setUp() {

		repository.deleteAll();

		Comments one = new Comments();
		one.setId(UUID.randomUUID());
		one.setLanguage("en");
		one.setEmbedding(Vector.of(0.45f, 0.09f, 0.01f, 0.2f, 0.11f));
		one.setComment("Raining too hard should have postponed");

		Comments two = new Comments();
		two.setId(UUID.randomUUID());
		two.setLanguage("en");
		two.setEmbedding(Vector.of(0.99f, 0.5f, -10.99f, -100.1f, 0.34f));
		two.setComment("Second rest stop was out of water");

		Comments three = new Comments();
		three.setId(UUID.randomUUID());
		three.setLanguage("en");
		three.setEmbedding(Vector.of(0.9f, 0.54f, 0.12f, 0.1f, 0.95f));
		three.setComment("LATE RIDERS SHOULD NOT DELAY THE START");

		repository.saveAll(List.of(one, two, three));

	}

	@Test // GH-
	void shouldConsiderScoringFunction() {

		Vector vector = Vector.of(0.9f, 0.54f, 0.12f, 0.1f, 0.95f);

		SearchResults<Comments> result = repository.searchByEmbeddingNear(vector,
				VectorScoringFunctions.COSINE, Limit.of(100));

		assertThat(result).hasSize(3);
		for (SearchResult<Comments> commentSearch : result) {
			assertThat(commentSearch.getScore().getValue()).isNotCloseTo(0d, offset(0.1d));
		}

		result = repository.searchByEmbeddingNear(vector, VectorScoringFunctions.EUCLIDEAN, Limit.of(100));

		assertThat(result).hasSize(3);
		for (SearchResult<Comments> commentSearch : result) {
			assertThat(commentSearch.getScore().getValue()).isNotCloseTo(0.3d, offset(0.1d));
		}
	}

	@Test // GH-
	void shouldRunAnnotatedSearchByVector() {

		Vector vector = Vector.of(0.9f, 0.54f, 0.12f, 0.1f, 0.95f);

		SearchResults<Comments> result = repository.searchAnnotatedByEmbeddingNear(vector, Limit.of(100));

		assertThat(result).hasSize(3);
		for (SearchResult<Comments> commentSearch : result) {
			assertThat(commentSearch.getScore().getValue()).isNotCloseTo(0d, offset(0.1d));
		}
	}

	@Test // GH-
	void shouldFindByVector() {

		Vector vector = Vector.of(0.9f, 0.54f, 0.12f, 0.1f, 0.95f);

		List<Comments> result = repository.findByEmbeddingNear(vector, Limit.of(100));

		assertThat(result).hasSize(3);
	}

	@Table
	static class Comments {

		@Id UUID id;
		String comment;

		@Indexed String language;

		@VectorType(dimensions = 5)
		@SaiIndexed Vector embedding;

		public UUID getId() {
			return id;
		}

		public void setId(UUID id) {
			this.id = id;
		}

		public String getLanguage() {
			return language;
		}

		public void setLanguage(String language) {
			this.language = language;
		}

		public String getComment() {
			return comment;
		}

		public void setComment(String comment) {
			this.comment = comment;
		}

		public Vector getEmbedding() {
			return embedding;
		}

		public void setEmbedding(Vector embedding) {
			this.embedding = embedding;
		}
	}

	interface CommentsRepository extends CrudRepository<Comments, UUID> {

		SearchResults<Comments> searchByEmbeddingNear(Vector embedding, ScoringFunction function, Limit limit);

		List<Comments> findByEmbeddingNear(Vector embedding, Limit limit);

		@Query("SELECT id,comment,language,similarity_cosine(embedding,:embedding) AS score FROM comments ORDER BY embedding ANN OF :embedding LIMIT :limit")
		SearchResults<Comments> searchAnnotatedByEmbeddingNear(Vector embedding, Limit limit);

	}

}
