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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.springframework.beans.DirectFieldAccessor;
import org.springframework.context.ApplicationContext;
import org.springframework.core.annotation.AliasFor;
import org.springframework.data.mapping.Association;
import org.springframework.data.mapping.AssociationHandler;
import org.springframework.data.util.ClassTypeInformation;

import com.datastax.oss.driver.api.core.CqlIdentifier;

/**
 * Unit tests for {@link BasicCassandraPersistentEntity}.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author John Blum
 * @author Mark Paluch
 */
@ExtendWith(MockitoExtension.class)
class BasicCassandraPersistentEntityUnitTests {

	@Mock ApplicationContext context;

	@Test
	void subclassInheritsAtDocumentAnnotation() {

		BasicCassandraPersistentEntity<Notification> entity = new BasicCassandraPersistentEntity<>(
				ClassTypeInformation.from(Notification.class));

		assertThat(entity.getTableName()).hasToString("messages");
	}

	@Test
	void evaluatesSpELExpression() {

		BasicCassandraPersistentEntity<Area> entity = new BasicCassandraPersistentEntity<>(
				ClassTypeInformation.from(Area.class));

		entity.setApplicationContext(this.context);

		assertThat(entity.getTableName()).hasToString("a123");
	}

	@Test
	void tableAllowsReferencingSpringBean() {

		TableNameHolderThingy bean = new TableNameHolderThingy();
		bean.tableName = "my_user_line";

		when(this.context.getBean("tableNameHolderThingy")).thenReturn(bean);
		when(this.context.containsBean("tableNameHolderThingy")).thenReturn(true);

		BasicCassandraPersistentEntity<UserLine> entity = new BasicCassandraPersistentEntity<>(
				ClassTypeInformation.from(UserLine.class));
		entity.setApplicationContext(context);

		assertThat(entity.getTableName()).hasToString(bean.tableName);
	}

	@Test
	void setForceQuoteCallsSetTableName() {

		BasicCassandraPersistentEntity<Message> entitySpy = spy(
				new BasicCassandraPersistentEntity<>(ClassTypeInformation.from(Message.class)));

		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(entitySpy);

		entitySpy.setTableName(CqlIdentifier.fromCql("Messages"));

		assertThat(directFieldAccessor.getPropertyValue("forceQuote")).isNull();

		entitySpy.setForceQuote(true);

		assertThat(directFieldAccessor.getPropertyValue("forceQuote")).isEqualTo(true);

		verify(entitySpy, times(2)).setTableName(isA(CqlIdentifier.class));
	}

	@Test
	void setForceQuoteDoesNothing() {

		BasicCassandraPersistentEntity<Message> entitySpy = spy(
				new BasicCassandraPersistentEntity<>(ClassTypeInformation.from(Message.class)));

		DirectFieldAccessor directFieldAccessor = new DirectFieldAccessor(entitySpy);

		directFieldAccessor.setPropertyValue("forceQuote", true);
		entitySpy.setForceQuote(true);

		assertThat(directFieldAccessor.getPropertyValue("forceQuote")).isEqualTo(true);

		verify(entitySpy, never()).setTableName(isA(CqlIdentifier.class));
	}

	@Test // DATACASS-172
	void isUserDefinedTypeShouldReturnFalse() {

		BasicCassandraPersistentEntity<UserLine> entity = new BasicCassandraPersistentEntity<>(
				ClassTypeInformation.from(UserLine.class));

		assertThat(entity.isUserDefinedType()).isFalse();
	}

	@Test // DATACASS-259
	void shouldConsiderComposedTableAnnotation() {

		BasicCassandraPersistentEntity<TableWithComposedAnnotation> entity = new BasicCassandraPersistentEntity<>(
				ClassTypeInformation.from(TableWithComposedAnnotation.class));

		assertThat(entity.getTableName()).isEqualTo(CqlIdentifier.fromCql("mytable"));
	}

	@Test // DATACASS-259
	void shouldConsiderComposedPrimaryKeyClassAnnotation() {

		BasicCassandraPersistentEntity<PrimaryKeyClassWithComposedAnnotation> entity = new BasicCassandraPersistentEntity<>(
				ClassTypeInformation.from(PrimaryKeyClassWithComposedAnnotation.class));

		assertThat(entity.isCompositePrimaryKey()).isTrue();
	}

	@Test // DATACASS-633
	@SuppressWarnings("unchecked")
	void shouldRejectAssociationCreation() {

		BasicCassandraPersistentEntity<PrimaryKeyClassWithComposedAnnotation> entity = new BasicCassandraPersistentEntity<>(
				ClassTypeInformation.from(PrimaryKeyClassWithComposedAnnotation.class));

		assertThatThrownBy(() -> entity.addAssociation(mock(Association.class)))
				.isInstanceOf(UnsupportedCassandraOperationException.class);
	}

	@Test // DATACASS-633
	@SuppressWarnings("unchecked")
	void shouldNoOpOnDoWithAssociations() {

		BasicCassandraPersistentEntity<PrimaryKeyClassWithComposedAnnotation> entity = new BasicCassandraPersistentEntity<>(
				ClassTypeInformation.from(PrimaryKeyClassWithComposedAnnotation.class));

		AssociationHandler<CassandraPersistentProperty> handlerMock = mock(AssociationHandler.class);
		entity.doWithAssociations(handlerMock);

		verifyZeroInteractions(handlerMock);
	}

	@Table("messages")
	static class Message {}

	private static class Notification extends Message {}

	@Table("#{'a123'}")
	private static class Area {}

	@Table("#{tableNameHolderThingy.tableName}")
	private static class UserLine {}

	private static class TableNameHolderThingy {

		private String tableName;

		public String getTableName() {
			return tableName;
		}
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	@Table(forceQuote = true)
	private @interface ComposedTableAnnotation {

		@AliasFor(annotation = Table.class)
		String value() default "mytable";
	}

	@Retention(RetentionPolicy.RUNTIME)
	@Target({ ElementType.TYPE })
	@PrimaryKeyClass
	private @interface ComposedPrimaryKeyClass {
	}

	@ComposedTableAnnotation()
	private static class TableWithComposedAnnotation {}

	@ComposedPrimaryKeyClass()
	private static class PrimaryKeyClassWithComposedAnnotation {}
}
