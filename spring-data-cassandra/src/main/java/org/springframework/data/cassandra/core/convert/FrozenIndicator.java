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
package org.springframework.data.cassandra.core.convert;

import java.util.ArrayList;
import java.util.List;

/**
 * Information about if a type or its elements should be frozen or not.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 3.0
 */
class FrozenIndicator {

	public static final FrozenIndicator NOT_FROZEN = new FrozenIndicator();

	private final boolean frozen;
	private final List<FrozenIndicator> nested = new ArrayList<>();

	public static FrozenIndicator frozen(boolean frozen) {
		return new FrozenIndicator(frozen);
	}

	private FrozenIndicator() {
		this(false);
	}

	private FrozenIndicator(boolean frozen) {
		this.frozen = frozen;
	}

	public void addNested(FrozenIndicator frozenIndicator) {
		this.nested.add(frozenIndicator);
	}

	boolean isFrozen() {
		return frozen;
	}

	FrozenIndicator getFrozen(int parameterIndex) {
		return parameterIndex < this.nested.size() ? this.nested.get(parameterIndex) : NOT_FROZEN;
	}
}
