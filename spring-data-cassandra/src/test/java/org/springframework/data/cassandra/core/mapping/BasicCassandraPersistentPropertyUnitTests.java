/*
 * Copyright 2016-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.core.mapping.CassandraType.*;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.AnnotatedParameterizedType;
import java.lang.reflect.AnnotatedType;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import org.springframework.core.annotation.AliasFor;
import org.springframework.data.mapping.model.Property;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.ReflectionUtils;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Unit tests for {@link BasicCassandraPersistentProperty}.
 *
 * @author Alex Shvid
 * @author Mark Paluch
 */
class BasicCassandraPersistentPropertyUnitTests {

	@Test
	void usesAnnotatedColumnName() {
		assertThat(getPropertyFor(Timeline.class, "text").getRequiredColumnName()).hasToString("message");
	}

	@Test
	void usesReservedColumnName() {
		assertThat(getPropertyFor(Timeline.class, "keyspace").getRequiredColumnName().asCql(true))
				.isEqualTo("\"keyspace\"");
	}

	@Test
	void usesReservedAnnotatedColumnName() {
		assertThat(getPropertyFor(Timeline.class, "table").getRequiredColumnName().asCql(true)).isEqualTo("\"table\"");
	}

	@Test
	void checksIdProperty() {

		CassandraPersistentProperty property = getPropertyFor(Timeline.class, "id");

		assertThat(property.isIdProperty()).isTrue();
	}

	@Test
	void returnsPropertyNameForUnannotatedProperty() {
		assertThat(getPropertyFor(Timeline.class, "time").getRequiredColumnName()).hasToString("time");
	}

	@Test // DATACASS-259
	void shouldConsiderComposedColumnAnnotation() {

		CassandraPersistentProperty persistentProperty = getPropertyFor(TypeWithComposedColumnAnnotation.class, "column");

		assertThat(persistentProperty.getRequiredColumnName()).isEqualTo(CqlIdentifier.fromCql("mycolumn"));
	}

	@Test // DATACASS-259
	void shouldConsiderComposedPrimaryKeyAnnotation() {

		CassandraPersistentProperty persistentProperty = getPropertyFor(TypeWithComposedPrimaryKeyAnnotation.class,
				"column");

		assertThat(persistentProperty.getRequiredColumnName()).isEqualTo(CqlIdentifier.fromInternal("primary-key"));
		assertThat(persistentProperty.isIdProperty()).isTrue();
	}

	@Test // DATACASS-259
	void shouldConsiderComposedPrimaryKeyColumnAnnotation() {

		CassandraPersistentProperty persistentProperty = getPropertyFor(TypeWithComposedPrimaryKeyColumnAnnotation.class,
				"column");

		assertThat(persistentProperty.getRequiredColumnName()).isEqualTo(CqlIdentifier.fromCql("mycolumn"));
		assertThat(persistentProperty.isPrimaryKeyColumn()).isTrue();
	}

	@Test // DATACASS-568
	void shouldFindAnnotationInMapTypes() {

		assertThat(findAnnotatedType(TypeWithMaps.class, "parameterized")).isNull();
		assertThat(findAnnotatedType(TypeWithMaps.class, "parameterizedWithAnnotation")).isNotNull();
		assertThat(findAnnotatedType(TypeWithMaps.class, "parameterizedWithParameterAnnotation")).isNotNull();
		assertThat(findAnnotatedType(TypeWithMaps.class, "unparameterized")).isNull();
		assertThat(findAnnotatedType(TypeWithMaps.class, "unparameterizedWithAnnotation")).isNotNull();
		assertThat(findAnnotatedType(TypeWithMaps.class, "subtype")).isNull();
	}

	@Test // DATACASS-568
	void shouldFindAnnotationInCollectionTypes() {

		assertThat(findAnnotatedType(TypeWithCollections.class, "parameterized")).isNull();
		assertThat(findAnnotatedType(TypeWithCollections.class, "parameterizedWithAnnotation")).isNotNull();
		assertThat(findAnnotatedType(TypeWithCollections.class, "parameterizedWithParameterAnnotation")).isNotNull();
		assertThat(findAnnotatedType(TypeWithCollections.class, "unparameterized")).isNull();
		assertThat(findAnnotatedType(TypeWithCollections.class, "unparameterizedWithAnnotation")).isNotNull();
		assertThat(findAnnotatedType(TypeWithCollections.class, "subtype")).isNull();
	}

	@Test // DATACASS-259
	void shouldConsiderComposedCassandraTypeAnnotation() {

		CassandraPersistentProperty persistentProperty = getPropertyFor(TypeWithComposedCassandraTypeAnnotation.class,
				"column");

		assertThat(persistentProperty.findAnnotation(CassandraType.class)).isNotNull();
	}

	/**
	 * Demonstrates how to access annotations on type parameters.
	 */
	@Test // DATACASS-465
	void parameterAnnotations() {

		AnnotatedType annotatedType = findAnnotatedType(TypeWithMaps.class, "parameterizedWithParameterAnnotation");
		assertThat(annotatedType).isNotNull().isInstanceOf(AnnotatedParameterizedType.class);

		AnnotatedParameterizedType apt = (AnnotatedParameterizedType) annotatedType;
		AnnotatedType[] annotatedActualTypeArguments = apt.getAnnotatedActualTypeArguments();

		assertThat(annotatedActualTypeArguments)
				.extracting(AnnotatedType::getType,
						a -> Arrays.stream(a.getAnnotations()).map(Indexed.class::isInstance).findFirst())
				.containsExactly(tuple(String.class, Optional.empty()), // annotation on key type
						tuple(String.class, Optional.of(true)) // annotation on value type
				);
	}

	private AnnotatedType findAnnotatedType(Class<?> type, String parameterized) {
		return getPropertyFor(TypeWithMaps.class, parameterized).findAnnotatedType(Indexed.class);
	}

	private CassandraPersistentProperty getPropertyFor(Class<?> type, String fieldName) {

		Field field = ReflectionUtils.findField(type, fieldName);

		return new BasicCassandraPersistentProperty(Property.of(ClassTypeInformation.from(type), field), getEntity(type),
				CassandraSimpleTypeHolder.HOLDER);
	}

	private <T> BasicCassandraPersistentEntity<T> getEntity(Class<T> type) {
		return new BasicCassandraPersistentEntity<>(ClassTypeInformation.from(type));
	}

	private static class Timeline {

		@PrimaryKey String id;

		Date time;

		@Column("message") String text;

		String keyspace;

		@Column("table") String table;
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Column(forceQuote = true)
	private @interface ComposedColumnAnnotation {

		@AliasFor(annotation = Column.class)
		String value();
	}

	@Retention(RetentionPolicy.RUNTIME)
	@PrimaryKey(forceQuote = true)
	private @interface ComposedPrimaryKeyAnnotation {

		@AliasFor(annotation = PrimaryKey.class)
		String value() default "primary-key";
	}

	@Retention(RetentionPolicy.RUNTIME)
	@PrimaryKeyColumn(forceQuote = true)
	private @interface ComposedPrimaryKeyColumnAnnotation {

		@AliasFor(annotation = PrimaryKeyColumn.class)
		String value();

		@AliasFor(annotation = PrimaryKeyColumn.class)
		int ordinal() default 42;
	}

	@Retention(RetentionPolicy.RUNTIME)
	@CassandraType(type = Name.COUNTER)
	private @interface ComposedCassandraTypeAnnotation {
	}

	private static class TypeWithComposedColumnAnnotation {
		@ComposedColumnAnnotation("mycolumn") String column;
	}

	private static class TypeWithComposedPrimaryKeyAnnotation {
		@ComposedPrimaryKeyAnnotation String column;
	}

	private static class TypeWithComposedPrimaryKeyColumnAnnotation {
		@ComposedPrimaryKeyColumnAnnotation("mycolumn") String column;
	}

	private static class TypeWithComposedCassandraTypeAnnotation {
		@ComposedCassandraTypeAnnotation String column;
	}

	private static class TypeWithMaps {

		Map<String, String> parameterized;

		@Indexed Map<String, String> parameterizedWithAnnotation;

		Map<String, @Indexed String> parameterizedWithParameterAnnotation;

		Map unparameterized;

		@Indexed Map unparameterizedWithAnnotation;

		MapType subtype;
	}

	private static class TypeWithCollections {

		List<String> parameterized;

		@Indexed List<String> parameterizedWithAnnotation;

		List<@Indexed String> parameterizedWithParameterAnnotation;

		List unparameterized;

		@Indexed List unparameterizedWithAnnotation;

		ListType subtype;
	}

	interface MapType extends Map<String, String> {}

	interface ListType extends List<String> {}
}
