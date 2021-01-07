/*
 * Copyright 2013-2021 the original author or authors.
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

import org.springframework.data.cassandra.core.cql.CassandraExceptionTranslator;
import org.springframework.data.cassandra.core.cql.CqlTemplate;

/**
 * Factory for creating and configuring a Cassandra {@link com.datastax.oss.driver.api.core.CqlSession}, which is a
 * thread-safe singleton. As such, it is sufficient to have one {@link CqlSession} per application and keyspace.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author John Blum
 * @author Mark Paluch
 * @see CqlTemplate
 * @see CassandraExceptionTranslator
 * @deprecated since 3.0, use {@link CqlSessionFactoryBean} directly.
 */
@Deprecated
public class CassandraCqlSessionFactoryBean extends CqlSessionFactoryBean {

}
