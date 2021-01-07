/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.cassandra.core.mapping;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.core.util.Strings;

/**
 * Factory for {@link CqlIdentifier}.
 *
 * @author Mark Paluch
 */
class IdentifierFactory {

	static CqlIdentifier create(String simpleName, boolean forceQuote) {

		if (forceQuote) {
			return CqlIdentifier.fromCql("\"" + simpleName + "\"");
		}

		if (Strings.needsDoubleQuotes(simpleName.toLowerCase())) {
			return CqlIdentifier.fromCql("\"" + simpleName.toLowerCase() + "\"");
		}

		return CqlIdentifier.fromInternal(simpleName.toLowerCase());
	}
}
