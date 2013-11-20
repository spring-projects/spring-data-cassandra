/*
 * Copyright (c) 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.test.unit.mapping;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.data.cassandra.mapping.BasicCassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.Table;
import org.springframework.data.util.ClassTypeInformation;

/**
 * Unit tests for {@link BasicCassandraPersistentEntity}.
 * 
 * @author Alex Shvid
 */
@RunWith(MockitoJUnitRunner.class)
public class BasicCassandraPersistentEntityUnitTests {

	@Mock
	ApplicationContext context;

	@BeforeClass
	public static void startCassandra() throws IOException, TTransportException, ConfigurationException,
			InterruptedException {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml");
	}

	@Test
	public void subclassInheritsAtDocumentAnnotation() {

		BasicCassandraPersistentEntity<Notification> entity = new BasicCassandraPersistentEntity<Notification>(
				ClassTypeInformation.from(Notification.class));
		assertThat(entity.getTable(), is("messages"));
	}

	@Test
	public void evaluatesSpELExpression() {

		BasicCassandraPersistentEntity<Area> entity = new BasicCassandraPersistentEntity<Area>(
				ClassTypeInformation.from(Area.class));
		assertThat(entity.getTable(), is("123"));
	}

	@Test
	public void collectionAllowsReferencingSpringBean() {

		MappingBean bean = new MappingBean();
		bean.userLine = "user_line";

		when(context.getBean("mappingBean")).thenReturn(bean);
		when(context.containsBean("mappingBean")).thenReturn(true);

		BasicCassandraPersistentEntity<UserLine> entity = new BasicCassandraPersistentEntity<UserLine>(
				ClassTypeInformation.from(UserLine.class));
		entity.setApplicationContext(context);

		assertThat(entity.getTable(), is("user_line"));
	}

	@After
	public void clearCassandra() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
	}

	@AfterClass
	public static void stopCassandra() {
		EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
	}

	@Table(name = "messages")
	class Message {

	}

	class Notification extends Message {

	}

	@Table(name = "#{123}")
	class Area {

	}

	@Table(name = "#{mappingBean.userLine}")
	class UserLine {

	}

	class MappingBean {

		String userLine;

		public String getUserLine() {
			return userLine;
		}
	}

}
