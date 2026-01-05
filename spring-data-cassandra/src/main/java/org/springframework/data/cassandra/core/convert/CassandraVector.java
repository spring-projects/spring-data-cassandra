/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.cassandra.core.convert;

import org.springframework.data.domain.Vector;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.data.CqlVector;

/**
 * Vector implementation for Cassandra's {@link CqlVector}.
 *
 * @author Mark Paluch
 * @since 4.5
 */
public class CassandraVector implements Vector {

	private final CqlVector<?> cqlVector;

	private CassandraVector(CqlVector<?> cqlVector) {
		this.cqlVector = cqlVector;
	}

	/**
	 * Creates a new CassandraVector for the given {@link CqlVector}.
	 *
	 * @param cqlVector must not be {@literal null}.
	 * @return a new CassandraVector for the given {@link CqlVector}
	 */
	public static CassandraVector of(CqlVector<?> cqlVector) {

		Assert.notNull(cqlVector, "CqlVector must not be null");

		return new CassandraVector(cqlVector);
	}

	@Override
	public Class<? extends Number> getType() {

		if (!cqlVector.isEmpty()) {

			Object o = cqlVector.get(0);

			if (o instanceof Float) {
				return Float.class;
			}

			if (o instanceof Double) {
				return Double.class;
			}
		}

		return Number.class;
	}

	@Override
	public CqlVector<?> getSource() {
		return cqlVector;
	}

	@Override
	public int size() {
		return cqlVector.size();
	}

	@Override
	public float[] toFloatArray() {

		float[] v = new float[cqlVector.size()];
		for (int i = 0; i < cqlVector.size(); i++) {
			v[i] = ((Number) cqlVector.get(i)).floatValue();
		}

		return v;
	}

	@Override
	public double[] toDoubleArray() {

		double[] v = new double[cqlVector.size()];
		for (int i = 0; i < cqlVector.size(); i++) {
			v[i] = ((Number) cqlVector.get(i)).doubleValue();
		}

		return v;
	}

	@Override
	public String toString() {
		return cqlVector.toString();
	}

}
