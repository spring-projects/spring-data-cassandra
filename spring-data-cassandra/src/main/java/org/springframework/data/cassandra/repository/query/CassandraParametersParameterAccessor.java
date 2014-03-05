package org.springframework.data.cassandra.repository.query;

import org.springframework.data.repository.query.ParametersParameterAccessor;

public class CassandraParametersParameterAccessor extends ParametersParameterAccessor implements
		CassandraParameterAccessor {

	/**
	 * Creates a new {@link CassandraParametersParameterAccessor}.
	 * 
	 * @param method must not be {@literal null}.
	 * @param values must not be {@@iteral null}.
	 */
	public CassandraParametersParameterAccessor(CassandraQueryMethod method, Object... values) {
		super(method.getParameters(), values);
	}
}
