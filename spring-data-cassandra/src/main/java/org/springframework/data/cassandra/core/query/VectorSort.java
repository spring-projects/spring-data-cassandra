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
package org.springframework.data.cassandra.core.query;

import java.io.Serial;
import java.util.List;

import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Vector;
import org.springframework.util.Assert;

import com.datastax.oss.driver.api.core.data.CqlVector;

/**
 * Sort option for queries that applies vector sorting.
 *
 * @author Mark Paluch
 * @since 4.5
 */
public class VectorSort extends Sort {

	private static final @Serial long serialVersionUID = 1L;

	private final Object vector;

	public VectorSort(String column, Object vector) {
		super(List.of(Order.by(column)));
		this.vector = vector;
	}

	public VectorSort(List<Order> orders, Object vector) {
		super(orders);
		Assert.isTrue(orders.size() == 1, "Orders must contain a single element");
		this.vector = vector;
	}

	/**
	 * Creates a new {@link VectorSort} for the given attributes with the default sort direction.
	 */
	public static VectorSort ann(String column, CqlVector<? extends Number> vector) {
		return new VectorSort(column, vector);
	}

	/**
	 * Creates a new {@link VectorSort} for the given attributes with the default sort direction.
	 */
	public static VectorSort ann(String column, float... vector) {
		return new VectorSort(column, Vector.of(vector));
	}

	/**
	 * Creates a new {@link VectorSort} for the given attributes with the default sort direction.
	 */
	public static VectorSort ann(String column, double... vector) {
		return new VectorSort(column, Vector.of(vector));
	}

	/**
	 * Creates a new {@link VectorSort} for the given attributes with the default sort direction.
	 */
	public static VectorSort ann(String column, Vector vector) {
		return new VectorSort(column, vector);
	}

	public Object getVector() {
		return vector;
	}
}
