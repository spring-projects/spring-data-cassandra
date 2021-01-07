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

import org.springframework.data.cassandra.core.cql.CqlTemplate;

/**
 * Abstract configuration class to create a {@link CqlTemplate} and inheriting
 * {@link com.datastax.oss.driver.api.core.CqlSession} creation. This class is usually extended by user configuration
 * classes.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @see AbstractSessionConfiguration
 * @see CqlTemplate
 * @deprecated since 3.0, use {@link AbstractSessionConfiguration}.
 */
@Deprecated
public abstract class AbstractCqlTemplateConfiguration extends AbstractSessionConfiguration {

}
