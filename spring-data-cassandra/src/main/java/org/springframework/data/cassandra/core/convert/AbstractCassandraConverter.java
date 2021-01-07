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
package org.springframework.data.cassandra.core.convert;


import java.util.Collections;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.GenericConversionService;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.model.EntityInstantiators;
import org.springframework.util.Assert;

/**
 * Base class for {@link CassandraConverter} implementations. Sets up a {@link ConversionService} and populates basic
 * converters.
 *
 * @author Alex Shvid
 * @author Matthew T. Adams
 * @author Mark Paluch
 * @see org.springframework.beans.factory.InitializingBean
 * @see org.springframework.data.cassandra.core.convert.CassandraConverter
 */
public abstract class AbstractCassandraConverter implements CassandraConverter, InitializingBean {

	private final ConversionService conversionService;

	private CustomConversions conversions = new CassandraCustomConversions(Collections.emptyList());

	EntityInstantiators instantiators = new EntityInstantiators();

	/**
	 * Create a new {@link AbstractCassandraConverter} using the given {@link ConversionService}.
	 */
	protected AbstractCassandraConverter(ConversionService conversionService) {

		Assert.notNull(conversionService, "ConversionService must not be null");

		this.conversionService = conversionService;
	}

	/**
	 * Registers {@link EntityInstantiators} to customize entity instantiation.
	 *
	 * @param instantiators must not be {@literal null}.
	 */
	public void setInstantiators(EntityInstantiators instantiators) {

		Assert.notNull(instantiators, "EntityInstantiators must not be null");

		this.instantiators = instantiators;
	}


	@Override
	public ConversionService getConversionService() {
		return this.conversionService;
	}

	/**
	 * Registers the given custom conversions with the converter.
	 */
	public void setCustomConversions(CustomConversions conversions) {
		this.conversions = conversions;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.convert.CassandraConverter#getCustomConversions()
	 */
	@Override
	public CustomConversions getCustomConversions() {
		return this.conversions;
	}

	/* (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {
		initializeConverters();
	}

	/**
	 * Registers additional converters that will be available when using the {@link ConversionService} directly (e.g. for
	 * id conversion). These converters are not custom conversions as they'd introduce unwanted conversions.
	 */
	private void initializeConverters() {

		ConversionService conversionService = getConversionService();

		if (conversionService instanceof GenericConversionService) {
			getCustomConversions().registerConvertersIn((GenericConversionService) conversionService);
		}
	}
}
