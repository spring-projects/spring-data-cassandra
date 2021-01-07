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
package org.springframework.data.cassandra.core.cql.session.init;

import org.springframework.core.io.support.EncodedResource;
import org.springframework.lang.Nullable;

/**
 * Thrown by {@link ScriptUtils} if a CQL script cannot be properly parsed.
 *
 * @author Mark Paluch
 * @since 3.0
 */
@SuppressWarnings("serial")
public class ScriptParseException extends ScriptException {

	/**
	 * Construct a new {@link ScriptParseException}.
	 *
	 * @param message detailed message.
	 * @param resource the resource from which the CQL script was read.
	 */
	public ScriptParseException(String message, @Nullable EncodedResource resource) {
		super(buildMessage(message, resource));
	}

	/**
	 * Construct a new {@link ScriptParseException}.
	 *
	 * @param message detailed message.
	 * @param resource the resource from which the CQL script was read.
	 * @param cause the root cause.
	 */
	public ScriptParseException(String message, @Nullable EncodedResource resource, @Nullable Throwable cause) {
		super(buildMessage(message, resource), cause);
	}

	private static String buildMessage(String message, @Nullable EncodedResource resource) {
		return String.format("Failed to parse CQL script from resource [%s]: %s",
				(resource == null ? "<unknown>" : resource), message);
	}
}
