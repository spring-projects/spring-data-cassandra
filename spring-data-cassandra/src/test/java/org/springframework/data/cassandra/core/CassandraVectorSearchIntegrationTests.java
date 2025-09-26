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
package org.springframework.data.cassandra.core;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.core.query.Criteria.*;

import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.core.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.mapping.SaiIndexed;
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.core.mapping.VectorType;
import org.springframework.data.cassandra.core.query.Columns;
import org.springframework.data.cassandra.core.query.Query;
import org.springframework.data.cassandra.core.query.VectorSort;
import org.springframework.data.cassandra.repository.support.SchemaTestUtils;
import org.springframework.data.cassandra.support.CassandraVersion;
import org.springframework.data.cassandra.test.util.AbstractKeyspaceCreatingIntegrationTests;
import org.springframework.data.domain.Vector;
import org.springframework.data.util.Version;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Integration tests for {@link CassandraTemplate} using Vector Search.
 *
 * @author Mark Paluch
 */
class CassandraVectorSearchIntegrationTests extends AbstractKeyspaceCreatingIntegrationTests {

	private static final Version CASSANDRA_5 = Version.parse("5.0");

	private Version cassandraVersion;

	private CassandraTemplate template;

	@BeforeEach
	void setUp() {

		MappingCassandraConverter converter = new MappingCassandraConverter();
		converter.setUserTypeResolver(new SimpleUserTypeResolver(session, CqlIdentifier.fromCql(keyspace)));
		converter.afterPropertiesSet();

		cassandraVersion = CassandraVersion.get(session);

		template = new CassandraTemplate(new CqlTemplate(session), converter);

		prepareTemplate(template);

		SchemaTestUtils.potentiallyCreateTableFor(Comments.class, template);
	}

	/**
	 * Post-process the {@link CassandraTemplate} before running the tests.
	 *
	 * @param template
	 */
	void prepareTemplate(CassandraTemplate template) {

	}

	@Test // GH-1504
	void shouldQueryVector() {

		assertThat(cassandraVersion.isGreaterThanOrEqualTo(CASSANDRA_5)).isTrue();

		Comments one = new Comments();
		one.setId(UUID.randomUUID());
		one.setLanguage("en");
		one.setVector(Vector.of(0.45f, 0.09f, 0.01f, 0.2f, 0.11f));
		one.setComment("Raining too hard should have postponed");

		Comments two = new Comments();
		two.setId(UUID.randomUUID());
		two.setLanguage("en");
		two.setVector(Vector.of(0.99f, 0.5f, 0.99f, 0.1f, 0.34f));
		two.setComment("Second rest stop was out of water");

		Comments three = new Comments();
		three.setId(UUID.randomUUID());
		three.setLanguage("en");
		three.setVector(Vector.of(0.9f, 0.54f, 0.12f, 0.1f, 0.95f));
		three.setComment("LATE RIDERS SHOULD NOT DELAY THE START");

		template.insert(one);
		template.insert(two);
		template.insert(three);

		Vector vector = Vector.of(0.2f, 0.15f, 0.3f, 0.2f, 0.05f);

		Columns columns = Columns.empty().include("comment").select("vector",
				it -> it.similarity(vector).cosine().as("similarity"));
		Query query = Query.select(columns).and(where("language").is("en")).limit(3).sort(VectorSort.ann("vector", vector));

		List<CommentSearch> result = template.query(Comments.class).as(CommentSearch.class).matching(query).all();

		assertThat(result).hasSize(3);
		for (CommentSearch commentSearch : result) {
			assertThat(commentSearch.similarity).isNotCloseTo(0f, offset(0.1f));
		}
	}

	static class CommentSearch {

		String comment;

		float similarity;

		@Override
		public String toString() {
			return "CommentSearch{" + "comment='" + comment + '\'' + ", similarity=" + similarity + '}';
		}
	}

	@Table
	static class Comments {

		@Id UUID id;
		String comment;
		@SaiIndexed String language;

		@VectorType(dimensions = 5)
		@SaiIndexed Vector vector;

		public UUID getId() {
			return id;
		}

		public void setId(UUID id) {
			this.id = id;
		}

		public String getComment() {
			return comment;
		}

		public void setComment(String comment) {
			this.comment = comment;
		}

		public String getLanguage() {
			return language;
		}

		public void setLanguage(String language) {
			this.language = language;
		}

		public Vector getVector() {
			return vector;
		}

		public void setVector(Vector vector) {
			this.vector = vector;
		}
	}

}
