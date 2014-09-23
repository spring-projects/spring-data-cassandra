/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.integration.mapping;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.data.cassandra.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.util.ClassTypeInformation;

/**
 * Integration tests for {@link BasicCassandraPersistentEntity}.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
@RunWith(MockitoJUnitRunner.class)
public class BasicCassandraPersistentEntityIntegrationTests {

	@Mock
	ApplicationContext context;

	@Test
	public void subclassInheritsAtDocumentAnnotation() {

		BasicCassandraPersistentEntity<Notification> entity = new BasicCassandraPersistentEntity<Notification>(
				ClassTypeInformation.from(Notification.class));
		assertThat(entity.getTableName().toCql(), is("messages"));
	}

	@Test
	public void evaluatesSpELExpression() {

		BasicCassandraPersistentEntity<Area> entity = new BasicCassandraPersistentEntity<Area>(
				ClassTypeInformation.from(Area.class));
		entity.setApplicationContext(context);
		assertThat(entity.getTableName().toCql(), is("a123"));
	}

	@Test
	public void tableAllowsReferencingSpringBean() {

		TableNameHolderThingy bean = new TableNameHolderThingy();
		bean.tableName = "my_user_line";

		when(context.getBean("tableNameHolderThingy")).thenReturn(bean);
		when(context.containsBean("tableNameHolderThingy")).thenReturn(true);

		BasicCassandraPersistentEntity<UserLine> entity = new BasicCassandraPersistentEntity<UserLine>(
				ClassTypeInformation.from(UserLine.class));
		entity.setApplicationContext(context);

		assertThat(entity.getTableName().toCql(), is(bean.tableName));
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
}
