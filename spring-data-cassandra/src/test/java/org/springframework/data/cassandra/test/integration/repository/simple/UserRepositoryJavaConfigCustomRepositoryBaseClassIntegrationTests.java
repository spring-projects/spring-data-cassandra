/*
 * Copyright 2016-2017 the original author or authors.
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
package org.springframework.data.cassandra.test.integration.repository.simple;

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.aop.framework.Advised;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.core.CassandraOperations;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.repository.query.CassandraEntityInformation;
import org.springframework.data.cassandra.repository.support.SimpleCassandraRepository;
import org.springframework.data.cassandra.test.integration.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * Java config tests for {@link UserRepository}.
 *
 * @author Thomas Darimont
 */
@ContextConfiguration
@RunWith(SpringJUnit4ClassRunner.class)
public class UserRepositoryJavaConfigCustomRepositoryBaseClassIntegrationTests
		extends UserRepositoryIntegrationTestsDelegator {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = UserRepository.class,
			repositoryBaseClass = CustomCassandraRepository.class)
	public static class Config extends IntegrationTestConfig {

		@Override
		public String[] getEntityBasePackages() {
			return new String[] { User.class.getPackage().getName() };
		}
	}

	@Test // DATACASS-211
	public void targetRepositoryClassShouldBeConfiguredCustomBaseRepositoryClass() throws Exception {
		assertThat(((Advised) repository).getTargetSource().getTarget()).isInstanceOf(CustomCassandraRepository.class);
	}

	public static class CustomCassandraRepository<T, ID> extends SimpleCassandraRepository<T, ID> {

		public CustomCassandraRepository(CassandraEntityInformation<T, ID> metadata, CassandraOperations operations) {
			super(metadata, operations);
		}
	}
}
