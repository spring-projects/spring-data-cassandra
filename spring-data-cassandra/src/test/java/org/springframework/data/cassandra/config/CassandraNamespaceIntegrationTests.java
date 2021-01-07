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
package org.springframework.data.cassandra.config;

import static org.assertj.core.api.Assertions.*;

import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.cassandra.core.CassandraTemplate;
import org.springframework.data.cassandra.core.cql.CqlTemplate;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.SimpleUserTypeResolver;
import org.springframework.data.cassandra.repository.support.AbstractSpringDataEmbeddedCassandraIntegrationTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.util.ReflectionTestUtils;

/**
 * Integration tests for XML-based Cassandra configuration using the Cassandra namespace parsed with
 * {@link CqlNamespaceHandler}.
 *
 * @author Mark Paluch
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
@SuppressWarnings("unused")
public class CassandraNamespaceIntegrationTests extends AbstractSpringDataEmbeddedCassandraIntegrationTest {

	@Autowired private ApplicationContext applicationContext;

	@Test // DATACASS-705
	public void keyspaceShouldBeInitialized() {

		CqlTemplate cqlTemplate = this.applicationContext.getBean(CqlTemplate.class);

		List<Map<String, Object>> result = cqlTemplate.queryForList("SELECT * FROM mytable1");

		assertThat(result).isEmpty();
	}

	@Test // DATACASS-172
	public void mappingContextShouldHaveUserTypeResolverConfigured() {

		CassandraMappingContext mappingContext = this.applicationContext.getBean(CassandraMappingContext.class);

		SimpleUserTypeResolver userTypeResolver = (SimpleUserTypeResolver) ReflectionTestUtils.getField(mappingContext,
				"userTypeResolver");

		assertThat(userTypeResolver).isNotNull();
	}

	@Test // DATACASS-417
	public void mappingContextShouldCassandraTemplateConfigured() {

		CassandraTemplate cassandraTemplate = this.applicationContext.getBean(CassandraTemplate.class);

		CqlTemplate cqlTemplate = this.applicationContext.getBean(CqlTemplate.class);

		assertThat(cassandraTemplate.getCqlOperations()).isSameAs(cqlTemplate);
	}
}
