/*
 * Copyright 2020 the original author or authors.
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

import java.lang.annotation.Annotation;
import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.AnnotatedType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification;
import org.springframework.data.cassandra.core.mapping.CassandraPersistentProperty;
import org.springframework.data.cassandra.core.mapping.Indexed;
import org.springframework.data.cassandra.core.mapping.SASI;
import org.springframework.data.cassandra.core.mapping.SASI.NonTokenizingAnalyzed;
import org.springframework.data.cassandra.core.mapping.SASI.Normalization;
import org.springframework.data.cassandra.core.mapping.SASI.StandardAnalyzed;
import org.springframework.data.mapping.MappingException;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Factory to create {@link org.springframework.data.cassandra.core.cql.keyspace.CreateIndexSpecification} based on
 * index-annotated {@link CassandraPersistentProperty properties}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 * @see Indexed
 * @see SASI
 */
@SuppressWarnings("unchecked")
class IndexSpecificationFactory {

	private final static Map<Class<? extends Annotation>, CreateIndexConfigurer<? super Annotation>> INDEX_CONFIGURERS;

	static {

		Map<Class<? extends Annotation>, CreateIndexConfigurer<? extends Annotation>> configurers = new HashMap<>();

		configurers.put(StandardAnalyzed.class, StandardAnalyzedConfigurer.INSTANCE);
		configurers.put(NonTokenizingAnalyzed.class, NonTokenizingAnalyzedConfigurer.INSTANCE);

		INDEX_CONFIGURERS = (Map) Collections.unmodifiableMap(configurers);
	}

	/**
	 * Create a {@link List} of {@link CreateIndexSpecification} for a {@link CassandraPersistentProperty}. The resulting
	 * specifications are configured according the index annotations but do not configure
	 * {@link CreateIndexSpecification#tableName(String)}.
	 *
	 * @param property must not be {@literal null}.
	 * @return {@link List} of {@link CreateIndexSpecification}.
	 */
	static List<CreateIndexSpecification> createIndexSpecifications(CassandraPersistentProperty property) {

		List<CreateIndexSpecification> indexes = new ArrayList<>();

		if (property.isAnnotationPresent(Indexed.class)) {

			CreateIndexSpecification index = createIndexSpecification(property.findAnnotation(Indexed.class), property);

			if (property.isMapLike()) {
				index.entries();
			}

			indexes.add(index);
		}

		if (property.isAnnotationPresent(SASI.class)) {
			indexes.add(createIndexSpecification(property.findAnnotation(SASI.class), property));
		}

		if (property.isMapLike()) {

			AnnotatedType type = property.findAnnotatedType(Indexed.class);

			if (type instanceof AnnotatedParameterizedType) {

				AnnotatedParameterizedType parameterizedType = (AnnotatedParameterizedType) type;
				AnnotatedType[] typeArgs = parameterizedType.getAnnotatedActualTypeArguments();

				Indexed keyIndex = typeArgs.length == 2 ? AnnotatedElementUtils.getMergedAnnotation(typeArgs[0], Indexed.class)
						: null;

				Indexed valueIndex = typeArgs.length == 2
						? AnnotatedElementUtils.getMergedAnnotation(typeArgs[1], Indexed.class)
						: null;

				if ((!indexes.isEmpty() && (keyIndex != null || valueIndex != null))
						|| (keyIndex != null && valueIndex != null)) {

					throw new MappingException("Multiple index declarations for " + property
							+ " found. A map index must be either declared for entries, keys or values.");
				}

				if (keyIndex != null) {
					indexes.add(createIndexSpecification(keyIndex, property).keys());
				}

				if (valueIndex != null) {
					indexes.add(createIndexSpecification(valueIndex, property).values());
				}
			}
		}

		return indexes;
	}

	static CreateIndexSpecification createIndexSpecification(Indexed annotation,
			CassandraPersistentProperty property) {

		CreateIndexSpecification index;

		if (StringUtils.hasText(annotation.value())) {
			index = CreateIndexSpecification.createIndex(annotation.value());
		} else {
			index = CreateIndexSpecification.createIndex();
		}

		return index.columnName(property.getRequiredColumnName());
	}

	private static CreateIndexSpecification createIndexSpecification(SASI annotation,
			CassandraPersistentProperty property) {

		CreateIndexSpecification index;

		if (StringUtils.hasText(annotation.value())) {
			index = CreateIndexSpecification.createIndex(annotation.value());
		} else {
			index = CreateIndexSpecification.createIndex();
		}

		index.using("org.apache.cassandra.index.sasi.SASIIndex") //
				.columnName(property.getRequiredColumnName()) //
				.withOption("mode", annotation.indexMode().name());

		long analyzerCount = INDEX_CONFIGURERS.keySet().stream().filter(property::isAnnotationPresent).count();

		if (analyzerCount > 1) {
			throw new IllegalStateException(
					String.format("SASI indexed property %s must be annotated only with a single analyzer annotation", property));
		}

		for (Class<? extends Annotation> annotationType : INDEX_CONFIGURERS.keySet()) {

			if (!property.isAnnotationPresent(annotationType)) {
				continue;
			}

			Annotation analyzed = property.findAnnotation(annotationType);

			INDEX_CONFIGURERS.get(annotationType).accept(analyzed, index);
		}

		return index;
	}

	interface CreateIndexConfigurer<T extends Annotation> extends BiConsumer<T, CreateIndexSpecification> {}

	enum StandardAnalyzedConfigurer implements CreateIndexConfigurer<StandardAnalyzed> {

		INSTANCE;

		@Override
		public void accept(StandardAnalyzed standardAnalyzed, CreateIndexSpecification index) {

			index.withOption("analyzed", "true");
			index.withOption("analyzer_class", "org.apache.cassandra.index.sasi.analyzer.StandardAnalyzer");
			index.withOption("tokenization_enable_stemming", "" + standardAnalyzed.enableStemming());

			if (standardAnalyzed.normalization() == Normalization.LOWERCASE) {
				index.withOption("tokenization_normalize_lowercase", "true");
			}

			if (standardAnalyzed.normalization() == Normalization.UPPERCASE) {
				index.withOption("tokenization_normalize_uppercase", "true");
			}

			if (StringUtils.hasText(standardAnalyzed.locale())) {
				index.withOption("tokenization_locale", standardAnalyzed.locale());
			}

			if (!ObjectUtils.isEmpty(standardAnalyzed.skipStopWords())) {
				index.withOption("tokenization_skip_stop_words", "" + standardAnalyzed.skipStopWords());
			}
		}
	}

	enum NonTokenizingAnalyzedConfigurer implements CreateIndexConfigurer<NonTokenizingAnalyzed> {

		INSTANCE;

		@Override
		public void accept(NonTokenizingAnalyzed nonTokenizingAnalyzed, CreateIndexSpecification index) {

			index.withOption("analyzed", "true");
			index.withOption("analyzer_class", "org.apache.cassandra.index.sasi.analyzer.NonTokenizingAnalyzer");
			index.withOption("case_sensitive", "" + nonTokenizingAnalyzed.caseSensitive());

			if (nonTokenizingAnalyzed.normalization() == Normalization.LOWERCASE) {
				index.withOption("normalize_lowercase", "true");
			}

			if (nonTokenizingAnalyzed.normalization() == Normalization.UPPERCASE) {
				index.withOption("normalize_uppercase", "true");
			}
		}
	}
}
