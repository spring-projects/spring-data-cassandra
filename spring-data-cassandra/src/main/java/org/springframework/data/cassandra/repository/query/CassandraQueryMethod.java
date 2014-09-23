package org.springframework.data.cassandra.repository.query;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.repository.Query;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.ResultSet;

public class CassandraQueryMethod extends QueryMethod {

	// TODO: double-check this list
	public static final List<Class<?>> ALLOWED_PARAMETER_TYPES = Collections.unmodifiableList(Arrays
			.asList(new Class<?>[] { String.class, CharSequence.class, char.class, Character.class, char[].class, long.class,
					Long.class, boolean.class, Boolean.class, BigDecimal.class, BigInteger.class, double.class, Double.class,
					float.class, Float.class, InetAddress.class, Date.class, UUID.class, int.class, Integer.class }));

	public static final List<Class<?>> STRING_LIKE_PARAMETER_TYPES = Collections.unmodifiableList(Arrays
			.asList(new Class<?>[] { CharSequence.class, char.class, Character.class, char[].class }));

	public static final List<Class<?>> DATE_PARAMETER_TYPES = Collections.unmodifiableList(Arrays
			.asList(new Class<?>[] { Date.class }));

	public static boolean isMapOfCharSequenceToObject(TypeInformation<?> type) {

		if (!type.isMap()) {
			return false;
		}

		TypeInformation<?> keyType = type.getComponentType();
		TypeInformation<?> valueType = type.getMapValueType();

		return ClassUtils.isAssignable(CharSequence.class, keyType.getType()) && Object.class.equals(valueType.getType());
	}

	protected Method method;
	protected CassandraMappingContext mappingContext;
	protected Query query;
	protected String queryString;
	protected boolean queryCached = false;
	protected Set<Integer> stringLikeParameterIndexes = new HashSet<Integer>();
	protected Set<Integer> dateParameterIndexes = new HashSet<Integer>();

	public CassandraQueryMethod(Method method, RepositoryMetadata metadata, CassandraMappingContext mappingContext) {

		super(method, metadata);

		verify(method, metadata);

		this.method = method;

		Assert.notNull(mappingContext, "MappingContext must not be null!");
		this.mappingContext = mappingContext;
	}

	public void verify(Method method, RepositoryMetadata metadata) {

		// TODO: support Page & Slice queries
		if (isSliceQuery() || isPageQuery()) {
			throw new InvalidDataAccessApiUsageException("neither slice nor page queries are supported yet");
		}

		Set<Class<?>> offendingTypes = new HashSet<Class<?>>();

		int i = 0;
		for (Class<?> type : method.getParameterTypes()) {
			if (!ALLOWED_PARAMETER_TYPES.contains(type)) {
				offendingTypes.add(type);
			}
			for (Class<?> quotedType : STRING_LIKE_PARAMETER_TYPES) {
				if (quotedType.isAssignableFrom(type)) {
					stringLikeParameterIndexes.add(i);
				}
			}
			for (Class<?> quotedType : DATE_PARAMETER_TYPES) {
				if (quotedType.isAssignableFrom(type)) {
					dateParameterIndexes.add(i);
				}
			}
			i++;
		}

		if (offendingTypes.size() > 0) {
			throw new IllegalArgumentException(String.format(
					"encountered unsupported query parameter type%s [%s] in method %s", offendingTypes.size() == 1 ? "" : "s",
					StringUtils.arrayToCommaDelimitedString(new ArrayList<Class<?>>(offendingTypes).toArray()), method));
		}
	}

	@Override
	protected CassandraParameters createParameters(Method method) {
		return new CassandraParameters(method);
	}

	/**
	 * Returns the {@link Query} annotation that is applied to the method or {@code null} if none available.
	 */
	Query getQueryAnnotation() {
		if (query == null) {
			query = method.getAnnotation(Query.class);
			queryCached = true;
		}
		return query;
	}

	/**
	 * Returns whether the method has an annotated query.
	 */
	public boolean hasAnnotatedQuery() {
		return getAnnotatedQuery() != null;
	}

	/**
	 * Returns the query string declared in a {@link Query} annotation or {@literal null} if neither the annotation found
	 * nor the attribute was specified.
	 */
	public String getAnnotatedQuery() {

		if (!queryCached) {
			queryString = (String) AnnotationUtils.getValue(getQueryAnnotation());
			queryString = StringUtils.hasText(queryString) ? queryString : null;
		}

		return queryString;
	}

	public TypeInformation<?> getReturnType() {
		return ClassTypeInformation.fromReturnTypeOf(method);
	}

	public boolean isResultSetQuery() {
		return ResultSet.class.isAssignableFrom(method.getReturnType());
	}

	public boolean isSingleEntityQuery() {
		return ClassUtils.isAssignable(getDomainClass(), method.getReturnType());
	}

	public boolean isCollectionOfEntityQuery() {
		return isQueryForEntity() && isCollectionQuery();
	}

	public boolean isMapOfCharSequenceToObjectQuery() {

		return isMapOfCharSequenceToObject(getReturnType());
	}

	public boolean isListOfMapOfCharSequenceToObject() {

		TypeInformation<?> type = getReturnType();
		if (!ClassUtils.isAssignable(List.class, type.getType())) {
			return false;
		}

		return isMapOfCharSequenceToObject(type.getComponentType());
	}

	public boolean isStringLikeParameter(int parameterIndex) {
		return stringLikeParameterIndexes.contains(parameterIndex);
	}

	public boolean isDateParameter(int parameterIndex) {
		return dateParameterIndexes.contains(parameterIndex);
	}
}
