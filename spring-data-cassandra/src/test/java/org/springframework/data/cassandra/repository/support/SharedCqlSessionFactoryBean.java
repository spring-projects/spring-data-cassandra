/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.cassandra.repository.support;

import org.springframework.data.cassandra.config.CqlSessionFactoryBean;
import org.springframework.data.cassandra.test.util.CassandraExtension;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;

/**
 * @author Mark Paluch
 */
class SharedCqlSessionFactoryBean extends CqlSessionFactoryBean {

	@Override
	protected CqlSession buildSystemSession(CqlSessionBuilder sessionBuilder) {
		return CassandraExtension.currentSystemSession();
	}

	@Override
	protected CqlSession buildSession(CqlSessionBuilder sessionBuilder) {
		CqlSession session = CassandraExtension.currentCqlSession();

		session.execute("USE " + getKeyspaceName());
		return session;
	}

	@Override
	protected void closeSession() {}

	@Override
	protected void closeSystemSession() {}
}
