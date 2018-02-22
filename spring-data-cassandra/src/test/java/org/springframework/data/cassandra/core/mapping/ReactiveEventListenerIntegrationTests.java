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

import com.datastax.driver.core.Session;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.cassandra.core.ReactiveCassandraTemplate;
import org.springframework.data.cassandra.core.cql.session.DefaultBridgedReactiveSession;

/**
 * Integration tests for callback events with reactive Cassandra template.
 *
 * @author Lukasz Antoniak
 */
public class ReactiveEventListenerIntegrationTests extends EventListenerIntegrationTests {
	private ReactiveCassandraTemplate reactiveTemplate = null;

	@Override
	protected void setUpTemplate(Session session, ConfigurableApplicationContext context) {
		super.setUpTemplate(session, context);
		reactiveTemplate = new ReactiveCassandraTemplate(new DefaultBridgedReactiveSession(session));
		reactiveTemplate.setApplicationContext(context);
	}

	@Override
	protected void tearDownTemplate() {
		super.tearDownTemplate();
		reactiveTemplate = null;
	}

	@Override
	protected void insert(Object entity) {
		reactiveTemplate.insert(entity).block();
	}

	@Override
	protected void update(Object entity) {
		reactiveTemplate.update(entity).block();
	}

	@Override
	protected void delete(Object entity) {
		reactiveTemplate.delete(entity).block();
	}

	@Override
	protected <T> T selectOneById(String id, Class<T> entityClass) {
		return reactiveTemplate.selectOneById(id, entityClass).block();
	}
}
