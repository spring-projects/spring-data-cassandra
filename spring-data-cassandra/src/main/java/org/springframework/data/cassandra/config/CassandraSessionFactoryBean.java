/*
 * Copyright 2013-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.config;

/**
 * Factory to create and configure a Cassandra {@link com.datastax.oss.driver.api.core.CqlSession} with support for
 * executing CQL and initializing the database schema (a.k.a. keyspace).
 *
 * @author Mathew Adams
 * @author David Webb
 * @author John Blum
 * @author Mark Paluch
 * @deprecated since 3.0, use {@link CqlSessionFactoryBean} directly.
 */
@Deprecated
public class CassandraSessionFactoryBean extends CqlSessionFactoryBean {

}
