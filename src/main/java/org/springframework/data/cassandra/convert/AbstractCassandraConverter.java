/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.cassandra.convert;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.convert.EntityInstantiators;

/**
 * Base class for {@link CassandraConverter} implementations. Sets up a {@link GenericConversionService} and populates basic
 * converters.
 * 
 * @author Alex Shvid
 */
public abstract class AbstractCassandraConverter implements CassandraConverter, InitializingBean  {

	protected final GenericConversionService conversionService;
	protected EntityInstantiators instantiators = new EntityInstantiators();
	
	/**
	 * Creates a new {@link AbstractMongoConverter} using the given {@link GenericConversionService}.
	 * 
	 * @param conversionService
	 */
	public AbstractCassandraConverter(GenericConversionService conversionService) {
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
	
	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.core.convert.MongoConverter#getConversionService()
	 */
	public ConversionService getConversionService() {
		return conversionService;
	}
	
	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() {
	}
	
}
