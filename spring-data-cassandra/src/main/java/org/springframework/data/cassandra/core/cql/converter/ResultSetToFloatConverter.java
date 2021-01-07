/*
 * Copyright 2017-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.cql.converter;

import com.datastax.oss.driver.api.core.cql.ResultSet;

import org.springframework.core.convert.converter.Converter;

/**
 * {@link Converter} from {@link ResultSet} to a single {@link Float} value.
 *
 * @author Mark Paluch
 */
public class ResultSetToFloatConverter extends AbstractResultSetToBasicFixedTypeConverter<Float> {

	public static final ResultSetToFloatConverter INSTANCE = new ResultSetToFloatConverter();

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.converter.AbstractResultSetConverter#doConvertSingleValue(java.lang.Object)
	 */
	@Override
	protected Float doConvertSingleValue(Object object) {
		return CONVERTER.convert(object, Float.class);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.cql.converter.AbstractResultSetConverter#getType()
	 */
	@Override
	protected Class<?> getType() {
		return Float.class;
	}
}
