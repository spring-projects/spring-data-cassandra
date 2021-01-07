/*
 * Copyright 2020-2021 the original author or authors.
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

import java.beans.PropertyDescriptor;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.beans.NotWritablePropertyException;
import org.springframework.beans.TypeConverter;
import org.springframework.beans.TypeMismatchException;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.core.convert.CassandraJsr310Converters;
import org.springframework.data.convert.Jsr310Converters;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.datastax.oss.driver.api.core.cql.Row;

/**
 * {@link RowMapper} implementation that converts a {@link Row} into a new instance of the specified mapped target
 * class. The mapped target class must be a top-level class and it must have a default or no-arg constructor.
 * <p>
 * Column values are mapped based on matching the column name as obtained from result set meta-data to public setters
 * for the corresponding properties. The names are matched either directly or by transforming a name separating the
 * parts with underscores to the same name using "camel" case.
 * <p>
 * Mapping is provided for fields in the target class for many common types, e.g.: String, boolean, Boolean, byte, Byte,
 * short, Short, int, Integer, long, Long, float, Float, double, Double, BigDecimal, {@code java.util.Date}, etc.
 * <p>
 * To facilitate mapping between columns and fields that don't have matching names, try using column aliases in the CQL
 * statement like "select fname as first_name from customer".
 * <p>
 * For 'null' values read from the database, we will attempt to call the setter, but in the case of Java primitives,
 * this causes a {@link TypeMismatchException}. This class can be configured (using the
 * {@code primitivesDefaultedForNullValue} property) to trap this exception and use the primitives default value. Be
 * aware that if you use the values from the generated bean to update the database the primitive value will have been
 * set to the primitive's default value instead of null.
 * <p>
 * Please note that this class is designed to provide convenience rather than high performance. For best performance,
 * consider using a custom {@link RowMapper} implementation.
 *
 * @author Mark Paluch
 * @since 3.1
 * @param <T> the result type
 */
public class BeanPropertyRowMapper<T> implements RowMapper<T> {

	/** Logger available to subclasses. */
	protected final Log logger = LogFactory.getLog(getClass());

	/** The class we are mapping to. */
	private @Nullable Class<T> mappedClass;

	/** Whether we're strictly validating. */
	private boolean checkFullyPopulated = false;

	/** Whether we're defaulting primitives when mapping a null value. */
	private boolean primitivesDefaultedForNullValue = false;

	/** ConversionService for binding values to bean properties. */
	private @Nullable ConversionService conversionService;

	/** Map of the fields we provide mapping for. */
	private @Nullable Map<String, PropertyDescriptor> mappedFields;

	/** Set of bean properties we provide mapping for. */
	private @Nullable Set<String> mappedProperties;

	/**
	 * Create a new {@code BeanPropertyRowMapper} for bean-style configuration.
	 *
	 * @see #setMappedClass
	 * @see #setCheckFullyPopulated
	 */
	public BeanPropertyRowMapper() {}

	/**
	 * Create a new {@code BeanPropertyRowMapper}, accepting unpopulated properties in the target bean.
	 *
	 * @param mappedClass the class that each row should be mapped to
	 */
	public BeanPropertyRowMapper(Class<T> mappedClass) {
		setMappedClass(mappedClass);
	}

	/**
	 * Create a new {@code BeanPropertyRowMapper}.
	 *
	 * @param mappedClass the class that each row should be mapped to
	 * @param checkFullyPopulated whether we're strictly validating that all bean properties have been mapped from
	 *          corresponding database fields
	 */
	public BeanPropertyRowMapper(Class<T> mappedClass, boolean checkFullyPopulated) {
		initialize(mappedClass);
		this.checkFullyPopulated = checkFullyPopulated;
	}

	{
		DefaultConversionService conversionService = new DefaultConversionService();
		Jsr310Converters.getConvertersToRegister().forEach(conversionService::addConverter);
		CassandraJsr310Converters.getConvertersToRegister().forEach(conversionService::addConverter);
		this.conversionService = conversionService;
	}

	/**
	 * Set the class that each row should be mapped to.
	 */
	public void setMappedClass(Class<T> mappedClass) {
		if (this.mappedClass == null) {
			initialize(mappedClass);
		} else {
			if (this.mappedClass != mappedClass) {
				throw new InvalidDataAccessApiUsageException("The mapped class can not be reassigned to map to " + mappedClass
						+ " since it is already providing mapping for " + this.mappedClass);
			}
		}
	}

	/**
	 * Get the class that we are mapping to.
	 */
	@Nullable
	public final Class<T> getMappedClass() {
		return this.mappedClass;
	}

	/**
	 * Set whether we're strictly validating that all bean properties have been mapped from corresponding database fields.
	 * <p>
	 * Default is {@literal false}, accepting unpopulated properties in the target bean.
	 */
	public void setCheckFullyPopulated(boolean checkFullyPopulated) {
		this.checkFullyPopulated = checkFullyPopulated;
	}

	/**
	 * Return whether we're strictly validating that all bean properties have been mapped from corresponding database
	 * fields.
	 */
	public boolean isCheckFullyPopulated() {
		return this.checkFullyPopulated;
	}

	/**
	 * Set whether we're defaulting Java primitives in the case of mapping a null value from corresponding database
	 * fields.
	 * <p>
	 * Default is {@literal false}, throwing an exception when nulls are mapped to Java primitives.
	 */
	public void setPrimitivesDefaultedForNullValue(boolean primitivesDefaultedForNullValue) {
		this.primitivesDefaultedForNullValue = primitivesDefaultedForNullValue;
	}

	/**
	 * Return whether we're defaulting Java primitives in the case of mapping a null value from corresponding database
	 * fields.
	 */
	public boolean isPrimitivesDefaultedForNullValue() {
		return this.primitivesDefaultedForNullValue;
	}

	/**
	 * Set a {@link ConversionService} for binding Cassandra values to bean properties, or {@literal null} for none.
	 * <p>
	 * Default is a {@link DefaultConversionService}. This provides support for {@code java.time} conversion and other
	 * special types.
	 *
	 * @see #initBeanWrapper(BeanWrapper)
	 */
	public void setConversionService(@Nullable ConversionService conversionService) {
		this.conversionService = conversionService;
	}

	/**
	 * Return a {@link ConversionService} for binding Cassandra values to bean properties, or {@literal null} if none.
	 */
	@Nullable
	public ConversionService getConversionService() {
		return this.conversionService;
	}

	/**
	 * Initialize the mapping meta-data for the given class.
	 *
	 * @param mappedClass the mapped class
	 */
	protected void initialize(Class<T> mappedClass) {
		this.mappedClass = mappedClass;
		this.mappedFields = new HashMap<>();
		this.mappedProperties = new HashSet<>();

		for (PropertyDescriptor pd : BeanUtils.getPropertyDescriptors(mappedClass)) {
			if (pd.getWriteMethod() != null) {
				this.mappedFields.put(lowerCaseName(pd.getName()), pd);
				String underscoredName = underscoreName(pd.getName());
				if (!lowerCaseName(pd.getName()).equals(underscoredName)) {
					this.mappedFields.put(underscoredName, pd);
				}
				this.mappedProperties.add(pd.getName());
			}
		}
	}

	/**
	 * Convert a name in camelCase to an underscored name in lower case. Any upper case letters are converted to lower
	 * case with a preceding underscore.
	 *
	 * @param name the original name
	 * @return the converted name
	 * @see #lowerCaseName
	 */
	protected String underscoreName(String name) {
		if (!StringUtils.hasLength(name)) {
			return "";
		}

		StringBuilder result = new StringBuilder();
		for (int i = 0; i < name.length(); i++) {
			char c = name.charAt(i);
			if (Character.isUpperCase(c)) {
				result.append('_').append(Character.toLowerCase(c));
			} else {
				result.append(c);
			}
		}
		return result.toString();
	}

	/**
	 * Convert the given name to lower case. By default, conversions will happen within the US locale.
	 *
	 * @param name the original name
	 * @return the converted name
	 */
	protected String lowerCaseName(String name) {
		return name.toLowerCase(Locale.US);
	}

	/**
	 * Extract the values for all columns in the current row.
	 * <p>
	 * Utilizes public setters and result set meta-data.
	 */
	@Override
	public T mapRow(Row row, int rowNumber) {

		BeanWrapperImpl bw = new BeanWrapperImpl();
		initBeanWrapper(bw);

		T mappedObject = constructMappedInstance(row, bw);
		bw.setBeanInstance(mappedObject);
		int columnCount = row.getColumnDefinitions().size();
		Set<String> populatedProperties = (isCheckFullyPopulated() ? new HashSet<>() : null);

		for (int index = 0; index < columnCount; index++) {
			String column = row.getColumnDefinitions().get(index).getName().toString();
			String field = lowerCaseName(StringUtils.delete(column, " "));
			PropertyDescriptor pd = (this.mappedFields != null ? this.mappedFields.get(field) : null);
			if (pd != null) {
				try {
					Object value = getColumnValue(row, index, pd);
					if (rowNumber == 0 && logger.isDebugEnabled()) {
						logger.debug("Mapping column '" + column + "' to property '" + pd.getName() + "' of type '"
								+ ClassUtils.getQualifiedName(pd.getPropertyType()) + "'");
					}
					try {
						bw.setPropertyValue(pd.getName(), value);
					} catch (TypeMismatchException ex) {
						if (value == null && this.primitivesDefaultedForNullValue) {
							if (logger.isDebugEnabled()) {
								logger.debug("Intercepted TypeMismatchException for row " + rowNumber + " and column '" + column
										+ "' with null value when setting property '" + pd.getName() + "' of type '"
										+ ClassUtils.getQualifiedName(pd.getPropertyType()) + "' on object: " + mappedObject, ex);
							}
						} else {
							throw ex;
						}
					}
					if (populatedProperties != null) {
						populatedProperties.add(pd.getName());
					}
				} catch (NotWritablePropertyException ex) {
					throw new DataRetrievalFailureException(
							"Unable to map column '" + column + "' to property '" + pd.getName() + "'", ex);
				}
			} else {
				// No PropertyDescriptor found
				if (rowNumber == 0 && logger.isDebugEnabled()) {
					logger.debug("No property found for column '" + column + "' mapped to field '" + field + "'");
				}
			}
		}

		if (populatedProperties != null && !populatedProperties.equals(this.mappedProperties)) {
			throw new InvalidDataAccessApiUsageException("Given Row does not contain all columns "
					+ "necessary to populate object of " + this.mappedClass + ": " + this.mappedProperties);
		}

		return mappedObject;
	}

	/**
	 * Construct an instance of the mapped class for the current row.
	 *
	 * @param row the row to map (pre-initialized for the current row)
	 * @param tc a TypeConverter with this RowMapper's conversion service
	 * @return a corresponding instance of the mapped class
	 */
	protected T constructMappedInstance(Row row, TypeConverter tc) {

		Assert.state(this.mappedClass != null, "Mapped class was not specified");

		return BeanUtils.instantiateClass(this.mappedClass);
	}

	/**
	 * Initialize the given {@link BeanWrapper} to be used for row mapping. To be called for each row.
	 * <p>
	 * The default implementation applies the configured {@link ConversionService}, if any. Can be overridden in
	 * subclasses.
	 *
	 * @param bw the BeanWrapper to initialize
	 * @see #getConversionService()
	 * @see BeanWrapper#setConversionService
	 */
	protected void initBeanWrapper(BeanWrapper bw) {

		ConversionService cs = getConversionService();

		if (cs != null) {
			bw.setConversionService(cs);
		}
	}

	/**
	 * Retrieve a Cassandra object value for the specified column.
	 * <p>
	 * The default implementation delegates to {@link Row#get(int, Class)}.
	 *
	 * @param row is the row holding the data
	 * @param index is the column index
	 * @param pd the bean property that each result object is expected to match
	 * @return the Object value
	 * @see #getColumnValue(Row, int, Class)
	 */
	@Nullable
	protected Object getColumnValue(Row row, int index, PropertyDescriptor pd) {
		return row.get(index, pd.getPropertyType());
	}

	/**
	 * Retrieve a Cassandra object value for the specified column.
	 * <p>
	 * The default implementation calls {@link Row#get(int, Class)}. Subclasses may override this to check specific value
	 * types upfront, or to post-process values return from {@code get}.
	 *
	 * @param row is the row holding the data.
	 * @param index is the column index.
	 * @param paramType the target parameter type.
	 * @return the Object value.
	 */
	@Nullable
	protected Object getColumnValue(Row row, int index, Class<?> paramType) {
		return row.get(index, paramType);
	}

	/**
	 * Static factory method to create a new {@code BeanPropertyRowMapper}.
	 *
	 * @param mappedClass the class that each row should be mapped to.
	 * @see #newInstance(Class, ConversionService)
	 */
	public static <T> BeanPropertyRowMapper<T> newInstance(Class<T> mappedClass) {
		return new BeanPropertyRowMapper<>(mappedClass);
	}

	/**
	 * Static factory method to create a new {@code BeanPropertyRowMapper}.
	 *
	 * @param mappedClass the class that each row should be mapped to.
	 * @param conversionService the {@link ConversionService} for binding Cassandra values to bean properties, or
	 *          {@literal null} for none.
	 * @see #newInstance(Class)
	 * @see #setConversionService
	 */
	public static <T> BeanPropertyRowMapper<T> newInstance(Class<T> mappedClass,
			@Nullable ConversionService conversionService) {

		BeanPropertyRowMapper<T> rowMapper = newInstance(mappedClass);
		rowMapper.setConversionService(conversionService);
		return rowMapper;
	}

}
