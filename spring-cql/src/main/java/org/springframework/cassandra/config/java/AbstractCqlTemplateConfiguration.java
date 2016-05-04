/*
 * Copyright 2016 the original author or authors.
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
package org.springframework.cassandra.config.java;

import org.springframework.cassandra.core.CqlTemplate;
import org.springframework.context.annotation.Bean;

/**
 * Abstract configuration class to create a {@link CqlTemplate} and inheriting {@link com.datastax.driver.core.Session}
 * and {@link com.datastax.driver.core.Cluster} creation. This class is usually extended by user configuration classes.
 *
 * @author Matthew T. Adams
 * @see org.springframework.cassandra.config.java.AbstractClusterConfiguration
 * @see org.springframework.cassandra.config.java.AbstractSessionConfiguration
 * @see com.datastax.driver.core.Session
 * @see com.datastax.driver.core.Cluster
 * @see CqlTemplate
 */
public abstract class AbstractCqlTemplateConfiguration extends AbstractSessionConfiguration {

	@Bean
	public CqlTemplate cqlTemplate() throws Exception {
		return new CqlTemplate(session().getObject());
	}
}
