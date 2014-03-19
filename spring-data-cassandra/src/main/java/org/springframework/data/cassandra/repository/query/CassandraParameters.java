package org.springframework.data.cassandra.repository.query;

import java.lang.reflect.Method;
import java.util.List;

import org.springframework.core.MethodParameter;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;

public class CassandraParameters extends Parameters<CassandraParameters, Parameter> {

	public CassandraParameters(List<Parameter> originals) {
		super(originals);
	}

	public CassandraParameters(Method method) {
		super(method);
	}

	@Override
	protected CassandraParameter createParameter(MethodParameter parameter) {
		return new CassandraParameter(parameter);
	}

	@Override
	protected CassandraParameters createFrom(List<Parameter> parameters) {
		return new CassandraParameters(parameters);
	}

	class CassandraParameter extends Parameter {

		protected CassandraParameter(MethodParameter parameter) {
			super(parameter);
		}

	}
}
