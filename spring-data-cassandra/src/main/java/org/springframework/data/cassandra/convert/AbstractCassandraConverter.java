/*
 * Copyright 2013-2014 the original author or authors
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.convert;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.convert.EntityInstantiators;

/**
 * Base class for {@link CassandraConverter} implementations. Sets up a {@link ConversionService} and populates basic
 * converters.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */
public abstract class AbstractCassandraConverter implements CassandraConverter, InitializingBean {

	protected final ConversionService conversionService;
	protected EntityInstantiators instantiators = new EntityInstantiators();

	/**
	 * Creates a new {@link AbstractCassandraConverter} using the given {@link ConversionService}.
	 */
	public AbstractCassandraConverter(ConversionService conversionService) {
		this.conversionService = conversionService == null ? new DefaultConversionService() : conversionService;
	}

	/**
	 * Registers {@link EntityInstantiators} to customize entity instantiation.
	 * 
	 * @param instantiators
	 */
	public void setInstantiators(EntityInstantiators instantiators) {
		this.instantiators = instantiators == null ? new EntityInstantiators() : instantiators;
	}

	@Override
	public ConversionService getConversionService() {
		return conversionService;
	}

	@Override
	public void afterPropertiesSet() {}
}
