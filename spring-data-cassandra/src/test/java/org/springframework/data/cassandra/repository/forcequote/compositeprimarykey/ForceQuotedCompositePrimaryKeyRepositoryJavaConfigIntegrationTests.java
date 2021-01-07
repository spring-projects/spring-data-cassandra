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
package org.springframework.data.cassandra.repository.forcequote.compositeprimarykey;

import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.repository.config.EnableCassandraRepositories;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.test.context.ContextConfiguration;

/**
 * @author Matthew T. Adams
 */
@ContextConfiguration
class ForceQuotedCompositePrimaryKeyRepositoryJavaConfigIntegrationTests
		extends ForceQuotedCompositePrimaryKeyRepositoryIntegrationTestsDelegator {

	@Configuration
	@EnableCassandraRepositories(basePackageClasses = ImplicitRepository.class)
	public static class Config extends IntegrationTestConfig {}

	@Test
	void testExplicit() {
		testExplicit(String.format("\"%s\"", Explicit.TABLE_NAME),
				String.format("\"%s\"", Explicit.STRING_VALUE_COLUMN_NAME),
				String.format("\"%s\"", ExplicitKey.EXPLICIT_KEY_ZERO), String.format("\"%s\"", ExplicitKey.EXPLICIT_KEY_ONE));
	}
}
