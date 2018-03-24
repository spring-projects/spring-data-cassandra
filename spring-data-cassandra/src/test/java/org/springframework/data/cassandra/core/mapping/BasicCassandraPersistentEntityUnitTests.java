/*
 * Copyright 2016-2018 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Optional;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AliasFor;
import org.springframework.data.cassandra.core.cql.CqlIdentifier;
import org.springframework.data.util.ClassTypeInformation;

/**
 * Unit tests for {@link BasicCassandraPersistentEntity}.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author John Blum
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner.class)
public class BasicCassandraPersistentEntityUnitTests {

	@Mock ApplicationContext context;

	@Test
	public void subclassInheritsAtDocumentAnnotation() {

		BasicCassandraPersistentEntity<Notification> entity =
				new BasicCassandraPersistentEntity<>(ClassTypeInformation.from(Notification.class));

		assertThat(entity.getTableName().toCql()).isEqualTo("messages");
	}

	@Test
	public void evaluatesSpELExpression() {

		BasicCassandraPersistentEntity<Area> entity =
				new BasicCassandraPersistentEntity<>(ClassTypeInformation.from(Area.class));

		entity.setApplicationContext(this.context);

		assertThat(entity.getTableName().toCql()).isEqualTo("a123");
	}

	@Test
	public void tableAllowsReferencingSpringBean() {

		TableNameHolderThingy bean = new TableNameHolderThingy();
		bean.tableName = "my_user_line";

		when(this.context.getBean("tableNameHolderThingy")).thenReturn(bean);
		when(this.context.containsBean("tableNameHolderThingy")).thenReturn(true);

		BasicCassandraPersistentEntity<UserLine> entity = new BasicCassandraPersistentEntity<>(
				ClassTypeInformation.from(UserLine.class));
		entity.setApplicationContext(context);

		assertThat(entity.getTableName().toCql()).isEqualTo(bean.tableName);
	}

	@Test
	public void setForceQuoteCallsSetTableName() {

		BasicCassandraPersistentEntity<Message> entitySpy =
				spy(new BasicCassandraPersistentEntity<>(ClassTypeInformation.from(Message.class)));

		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(entitySpy);

		entitySpy.setTableName(CqlIdentifier.of("Messages", false));

		assertThat(directFieldAccessor.getPropertyValue("forceQuote")).isNull();

		entitySpy.setForceQuote(true);

		assertThat(directFieldAccessor.getPropertyValue("forceQuote")).isEqualTo(true);

		verify(entitySpy, times(2)).setTableName(isA(CqlIdentifier.class));
	}

	@Test
	public void setForceQuoteDoesNothing() {

		BasicCassandraPersistentEntity<Message> entitySpy =
				spy(new BasicCassandraPersistentEntity<>(ClassTypeInformation.from(Message.class)));

		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(entitySpy);

		directFieldAccessor.setPropertyValue("forceQuote", true);
		entitySpy.setForceQuote(true);

		assertThat(directFieldAccessor.getPropertyValue("forceQuote")).isEqualTo(true);

		verify(entitySpy, never()).setTableName(isA(CqlIdentifier.class));
	}

	@Test // DATACASS-172
	public void isUserDefinedTypeShouldReturnFalse() {

		BasicCassandraPersistentEntity<UserLine> entity =
				new BasicCassandraPersistentEntity<>(ClassTypeInformation.from(UserLine.class));

		assertThat(entity.isUserDefinedType()).isFalse();
	}

	@Test // DATACASS-259
	public void shouldConsiderComposedTableAnnotation() {

		BasicCassandraPersistentEntity<TableWithComposedAnnotation> entity =
				new BasicCassandraPersistentEntity<>(ClassTypeInformation.from(TableWithComposedAnnotation.class));

		assertThat(entity.getTableName()).isEqualTo(CqlIdentifier.of("mytable", true));
	}

	@Test // DATACASS-259
	public void shouldConsiderComposedPrimaryKeyClassAnnotation() {

		BasicCassandraPersistentEntity<PrimaryKeyClassWithComposedAnnotation> entity =
				new BasicCassandraPersistentEntity<>(ClassTypeInformation.from(PrimaryKeyClassWithComposedAnnotation.class));

		assertThat(entity.isCompositePrimaryKey()).isTrue();
	}

	@Table("messages")
	static class Message {}

	static class Notification extends Message {}

	@Table("#{'a123'}")
	static class Area {}

	@Table("#{tableNameHolderThingy.tableName}")
	static class UserLine {}

	static class TableNameHolderThingy {

		String tableName;

		public String getTableName() {
			return tableName;
		}
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	@Table(forceQuote = true)
	@interface ComposedTableAnnotation {

		@AliasFor(annotation = Table.class)
		String value() default "mytable";
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	@PrimaryKeyClass
	@interface ComposedPrimaryKeyClass {
	}

	@ComposedTableAnnotation()
	static class TableWithComposedAnnotation {}

	@ComposedPrimaryKeyClass()
	static class PrimaryKeyClassWithComposedAnnotation {}
}
