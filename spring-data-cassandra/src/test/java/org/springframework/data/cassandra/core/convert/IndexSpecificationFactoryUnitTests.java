/*
 * Copyright 2020-present the original author or authors.
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
package org.springframework.data.cassandra.core.convert;

import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.springframework.data.annotation.AccessType;
import org.springframework.data.annotation.AccessType.Type;
import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification;
import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification.ColumnFunction;
import org.springframework.data.cassandra.core.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.PrimaryKeyColumn;
import org.springframework.data.cassandra.core.mapping.SASI;
import org.springframework.data.cassandra.core.mapping.SASI.NonTokenizingAnalyzed;
import org.springframework.data.cassandra.core.mapping.SASI.Normalization;
import org.springframework.data.cassandra.core.mapping.SASI.StandardAnalyzed;
import org.springframework.data.cassandra.core.mapping.SaiIndexed;
import org.springframework.data.cassandra.core.mapping.SimilarityFunction;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.CqlVector;

/**
 * Unit tests for {@link IndexSpecificationFactory}.
 *
 * @author Mark Paluch
 */
class IndexSpecificationFactoryUnitTests {

	private CassandraMappingContext mappingContext = new CassandraMappingContext();

	@Test // DATACASS-213
	void createIndexShouldConsiderAnnotatedProperties() {

		CreateIndexSpecification firstname = createIndexFor(IndexedType.class, "firstname");

		assertThat(firstname.getColumnName()).isEqualTo(CqlIdentifier.fromInternal("first_name"));
		assertThat(firstname.getTableName()).isNull();
		assertThat(firstname.getName()).isEqualTo(CqlIdentifier.fromInternal("my_index"));
		assertThat(firstname.getColumnFunction()).isEqualTo(ColumnFunction.NONE);

		CreateIndexSpecification entries = createIndexFor(IndexedType.class, "entries");

		assertThat(entries.getColumnName()).isEqualTo(CqlIdentifier.fromInternal("entries"));
		assertThat(entries.getTableName()).isNull();
		assertThat(entries.getName()).isNull();
		assertThat(entries.getColumnFunction()).isEqualTo(ColumnFunction.ENTRIES);
	}

	@Test // DATACASS-213
	void createMapKeyIndexShouldConsiderAnnotatedAccessors() {

		CreateIndexSpecification entries = createIndexFor(IndexedMapKeyProperty.class, "entries");

		assertThat(entries.getColumnName()).isEqualTo(CqlIdentifier.fromInternal("entries"));
		assertThat(entries.getTableName()).isNull();
		assertThat(entries.getName()).isNull();
		assertThat(entries.getColumnFunction()).isEqualTo(ColumnFunction.KEYS);
	}

	@Test // DATACASS-213
	void createMapValueIndexShouldConsiderAnnotatedAccessors() {

		CreateIndexSpecification entries = createIndexFor(MapValueIndexProperty.class, "entries");

		assertThat(entries.getColumnName()).isEqualTo(CqlIdentifier.fromInternal("entries"));
		assertThat(entries.getTableName()).isNull();
		assertThat(entries.getName()).isNull();
		assertThat(entries.getColumnFunction()).isEqualTo(ColumnFunction.VALUES);
	}

	@Test // GH-1505
	void createSaiIndexShouldCreateCreateIndexSpecification() {

		CreateIndexSpecification simpleSasi = createIndexFor(IndexedType.class, "simpleSai");

		assertThat(simpleSasi.getColumnName()).isEqualTo(CqlIdentifier.fromInternal("simplesai"));
		assertThat(simpleSasi.getTableName()).isNull();
		assertThat(simpleSasi.isCustom()).isFalse();
		assertThat(simpleSasi.getUsing()).isEqualTo("sai");
		assertThat(simpleSasi.getColumnFunction()).isEqualTo(ColumnFunction.NONE);
		assertThat(simpleSasi.getOptions()).containsEntry("case_sensitive", "true").containsEntry("normalize", "false")
				.containsEntry("ascii", "false").doesNotContainKey("similarity_function");
	}

	@Test // GH-1505
	void createSaiIndexShouldApplyIndexOptions() {

		CreateIndexSpecification simpleSai = createIndexFor(IndexedType.class, "customSai");

		assertThat(simpleSai.getName()).isEqualTo(CqlIdentifier.fromInternal("foo"));
		assertThat(simpleSai.getColumnName()).isEqualTo(CqlIdentifier.fromInternal("customsai"));
		assertThat(simpleSai.getTableName()).isNull();
		assertThat(simpleSai.isCustom()).isFalse();
		assertThat(simpleSai.getUsing()).isEqualTo("sai");
		assertThat(simpleSai.getColumnFunction()).isEqualTo(ColumnFunction.NONE);
		assertThat(simpleSai.getOptions()).containsEntry("case_sensitive", "false").containsEntry("normalize", "true")
				.containsEntry("ascii", "true").doesNotContainKey("similarity_function");
	}

	@Test // GH-1504
	void createSaiIndexShouldApplyVectorIndexOptions() {

		CreateIndexSpecification vector = createIndexFor(IndexedType.class, "someVector");

		assertThat(vector.getName()).isNull();
		assertThat(vector.getColumnName()).isEqualTo(CqlIdentifier.fromInternal("somevector"));
		assertThat(vector.getTableName()).isNull();
		assertThat(vector.isCustom()).isFalse();
		assertThat(vector.getUsing()).isEqualTo("sai");
		assertThat(vector.getColumnFunction()).isEqualTo(ColumnFunction.NONE);
		assertThat(vector.getOptions()).hasSize(1).containsEntry("similarity_function", "COSINE");
	}

	@Test // GH-1505
	void createSaiIndexMapKeyShouldCreateCreateIndexSpecification() {

		CreateIndexSpecification simpleSasi = createIndexFor(SaiIndexedMapKeyProperty.class, "entries");

		assertThat(simpleSasi.getColumnName()).isEqualTo(CqlIdentifier.fromInternal("entries"));
		assertThat(simpleSasi.getUsing()).isEqualTo("sai");
		assertThat(simpleSasi.getColumnFunction()).isEqualTo(ColumnFunction.KEYS);
	}

	@Test // DATACASS-306
	void createIndexForSimpleSasiShouldApplyIndexOptions() {

		CreateIndexSpecification simpleSasi = createIndexFor(IndexedType.class, "simpleSasi");

		assertThat(simpleSasi.getColumnName()).isEqualTo(CqlIdentifier.fromInternal("simplesasi"));
		assertThat(simpleSasi.getTableName()).isNull();
		assertThat(simpleSasi.isCustom()).isTrue();
		assertThat(simpleSasi.getUsing()).isEqualTo("org.apache.cassandra.index.sasi.SASIIndex");
		assertThat(simpleSasi.getColumnFunction()).isEqualTo(ColumnFunction.NONE);
		assertThat(simpleSasi.getOptions()).containsEntry("mode", "PREFIX").doesNotContainKeys("analyzed",
				"analyzer_class");
	}

	@Test // DATACASS-306
	void createIndexForStandardAnalyzedSasiShouldApplyIndexOptions() {

		CreateIndexSpecification simpleSasi = createIndexFor(IndexedType.class, "sasiStandard");

		assertThat(simpleSasi.getColumnFunction()).isEqualTo(ColumnFunction.NONE);
		assertThat(simpleSasi.getOptions()).containsEntry("mode", "PREFIX").containsEntry("analyzed", "true")
				.containsEntry("tokenization_skip_stop_words", "false")
				.containsEntry("analyzer_class", "org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer")
				.containsEntry("tokenization_locale", "de");
	}

	@Test // DATACASS-306
	void createIndexForStandardAnalyzedSasiWithOptionsShouldApplyIndexOptions() {

		CreateIndexSpecification simpleSasi = createIndexFor(IndexedType.class, "sasiStandardWithOptions");

		assertThat(simpleSasi.getColumnFunction()).isEqualTo(ColumnFunction.NONE);
		assertThat(simpleSasi.getOptions()).containsEntry("tokenization_skip_stop_words", "true")
				.containsEntry("tokenization_locale", "de").containsEntry("tokenization_enable_stemming", "true")
				.containsEntry("tokenization_normalize_uppercase", "true")
				.doesNotContainKey("tokenization_normalize_lowercase");
	}

	@Test // DATACASS-306
	void createIndexForStandardAnalyzedSasiWithLowercaseShouldApplyIndexOptions() {

		CreateIndexSpecification simpleSasi = createIndexFor(IndexedType.class, "sasiStandardLowercase");

		assertThat(simpleSasi.getColumnFunction()).isEqualTo(ColumnFunction.NONE);
		assertThat(simpleSasi.getOptions()).containsEntry("tokenization_normalize_lowercase", "true")
				.doesNotContainKey("tokenization_normalize_uppercase");
	}

	@Test // DATACASS-306
	void createIndexForNonTokenizingAnalyzedSasiShouldApplyIndexOptions() {

		CreateIndexSpecification simpleSasi = createIndexFor(IndexedType.class, "sasiNontokenizing");

		assertThat(simpleSasi.getOptions()).containsEntry("mode", "PREFIX").containsEntry("analyzed", "true")
				.containsEntry("case_sensitive", "true")
				.containsEntry("analyzer_class", "org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer")
				.doesNotContainKeys("normalize_lowercase", "normalize_uppercase");
	}

	@Test // DATACASS-306
	void createIndexForNonTokenizingAnalyzedSasiWithLowercaseShouldApplyIndexOptions() {

		CreateIndexSpecification simpleSasi = createIndexFor(IndexedType.class, "sasiNontokenizingLowercase");

		assertThat(simpleSasi.getOptions()).containsEntry("normalize_lowercase", "true")
				.containsEntry("case_sensitive", "false").doesNotContainKey("normalize_uppercase");
	}

	private CreateIndexSpecification createIndexFor(Class<?> type, String property) {
		return createIndexesFor(type, property).stream().findFirst().get();
	}

	private List<CreateIndexSpecification> createIndexesFor(Class<?> type, String property) {

		BasicCassandraPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(type);

		return IndexSpecificationFactory.createIndexSpecifications(entity.getKeyspace(),
				entity.getRequiredPersistentProperty(property));
	}

	private static class IndexedType {

		@PrimaryKeyColumn("first_name")
		@Indexed("my_index") String firstname;

		@Indexed List<String> phoneNumbers;

		@Indexed Map<String, String> entries;

		Map<@Indexed String, String> keys;

		Map<String, @Indexed String> values;

		@SASI String simpleSasi;
		@SASI
		@StandardAnalyzed("de") String sasiStandard;
		@SASI
		@StandardAnalyzed(value = "de", enableStemming = true, normalization = Normalization.UPPERCASE,
				skipStopWords = true) String sasiStandardWithOptions;

		@SASI
		@StandardAnalyzed(normalization = Normalization.LOWERCASE) String sasiStandardLowercase;

		@SASI
		@NonTokenizingAnalyzed String sasiNontokenizing;

		@SASI
		@NonTokenizingAnalyzed(caseSensitive = false,
				normalization = Normalization.LOWERCASE) String sasiNontokenizingLowercase;

		@SaiIndexed String simpleSai;

		@SaiIndexed CqlVector<?> someVector;

		@SaiIndexed(value = "foo", caseSensitive = false, normalize = true, ascii = true,
				similarityFunction = SimilarityFunction.EUCLIDEAN) String customSai;
	}

	@AccessType(Type.PROPERTY)
	private static class IndexedMapKeyProperty {

		public Map<@Indexed String, String> getEntries() {
			return null;
		}

		public void setEntries(Map<String, String> entries) {}
	}

	@AccessType(Type.PROPERTY)
	private static class SaiIndexedMapKeyProperty {

		public Map<@SaiIndexed String, String> getEntries() {
			return null;
		}

		public void setEntries(Map<String, String> entries) {}
	}

	@AccessType(Type.PROPERTY)
	private static class MapValueIndexProperty {

		public Map<String, String> getEntries() {
			return null;
		}

		public void setEntries(Map<String, @Indexed String> entries) {}
	}
}
