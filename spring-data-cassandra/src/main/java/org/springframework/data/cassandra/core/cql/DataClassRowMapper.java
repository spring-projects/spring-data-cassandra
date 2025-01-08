/*
 * Copyright 2020-2025 the original author or authors.
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
package org.springframework.data.cassandra.core.cql;

import java.lang.reflect.Constructor;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.TypeConverter;
import org.springframework.core.MethodParameter;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.TypeDescriptor;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.cql.Row;

/**
 * {@link RowMapper} implementation that converts a row into a new instance of the specified mapped target class. The
 * mapped target class must be a top-level class and may either expose a data class constructor with named parameters
 * corresponding to column names or classic bean property setters (or even a combination of both).
 * <p>
 * Note that this class extends {@link BeanPropertyRowMapper} and can therefore serve as a common choice for any mapped
 * target class, flexibly adapting to constructor style versus setter methods in the mapped class.
 *
 * @author Mark Paluch
 * @since 3.1
 * @param <T> the result type
 */
public class DataClassRowMapper<T> extends BeanPropertyRowMapper<T> {

	private @Nullable Constructor<T> mappedConstructor;

	private @Nullable String[] constructorParameterNames;

	private @Nullable TypeDescriptor[] constructorParameterTypes;

	/**
	 * Create a new {@code DataClassRowMapper}.
	 *
	 * @param mappedClass the class that each row should be mapped to.
	 */
	public DataClassRowMapper(Class<T> mappedClass) {
		super(mappedClass);
	}

	@Override
	protected void initialize(Class<T> mappedClass) {

		super.initialize(mappedClass);

		this.mappedConstructor = BeanUtils.getResolvableConstructor(mappedClass);
		int paramCount = this.mappedConstructor.getParameterCount();
		if (paramCount > 0) {
			this.constructorParameterNames = BeanUtils.getParameterNames(this.mappedConstructor);
			this.constructorParameterTypes = new TypeDescriptor[paramCount];
			for (int i = 0; i < paramCount; i++) {
				this.constructorParameterTypes[i] = new TypeDescriptor(new MethodParameter(this.mappedConstructor, i));
			}
		}
	}

	@Override
	protected T constructMappedInstance(Row row, TypeConverter tc) {

		Assert.state(this.mappedConstructor != null, "Mapped constructor was not initialized");

		Object[] args;
		if (this.constructorParameterNames != null && this.constructorParameterTypes != null) {
			args = new Object[this.constructorParameterNames.length];
			for (int i = 0; i < args.length; i++) {
				String name = underscoreName(this.constructorParameterNames[i]);
				TypeDescriptor td = this.constructorParameterTypes[i];
				Object value = getColumnValue(row, row.getColumnDefinitions().firstIndexOf(name), td.getType());
				args[i] = tc.convertIfNecessary(value, td.getType(), td);
			}
		} else {
			args = new Object[0];
		}

		return BeanUtils.instantiateClass(this.mappedConstructor, args);
	}

	/**
	 * Static factory method to create a new {@code DataClassRowMapper}.
	 *
	 * @param mappedClass the class that each row should be mapped to.
	 * @see #newInstance(Class, ConversionService)
	 */
	public static <T> DataClassRowMapper<T> newInstance(Class<T> mappedClass) {
		return new DataClassRowMapper<>(mappedClass);
	}

	/**
	 * Static factory method to create a new {@code DataClassRowMapper}.
	 *
	 * @param mappedClass the class that each row should be mapped to.
	 * @param conversionService the {@link ConversionService} for binding Cassandra values to bean properties, or
	 *          {@code null} for none.
	 * @see #newInstance(Class)
	 * @see #setConversionService
	 */
	public static <T> DataClassRowMapper<T> newInstance(Class<T> mappedClass,
			@Nullable ConversionService conversionService) {

		DataClassRowMapper<T> rowMapper = newInstance(mappedClass);
		rowMapper.setConversionService(conversionService);
		return rowMapper;
	}

}
