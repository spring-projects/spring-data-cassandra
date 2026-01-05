/*
 * Copyright 2022-present the original author or authors.
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
package org.springframework.data.cassandra.config;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.internal.core.util.concurrent.CompletableFutures;

/**
 * Utility methods for executing schema actions.
 *
 * @author Ammar Khaku
 * @author Mark Paluch
 * @since 2.7
 */
class SchemaUtils {

	/**
	 * Programmatically disables schema refresh on the session and runs the provided {@link Runnable}. Takes care to
	 * restore the previous state of schema refresh on the provided session. Note that the session could have had schema
	 * refreshes enabled/disabled either programmatically or via config.
	 *
	 * @param session the session to use.
	 * @param schemaAction the runnable code block.
	 */
	static void withSuspendedSchemaRefresh(Session session, Runnable schemaAction) {
		CompletableFutures.getUninterruptibly(withSuspendedAsyncSchemaRefresh(session, schemaAction));
	}

	/**
	 * Programmatically disables schema refresh on the session and runs the provided {@link Runnable}. Takes care to
	 * restore the previous state of schema refresh on the provided session. Note that the session could have had schema
	 * refreshes enabled/disabled either programmatically or via config.
	 *
	 * @param session the session to use.
	 * @param schemaAction the runnable code block.
	 * @return a {@link CompletionStage} providing a handle to the schema refresh completion.
	 */
	static CompletionStage<?> withSuspendedAsyncSchemaRefresh(Session session, Runnable schemaAction) {

		boolean schemaEnabledPreviously = session.isSchemaMetadataEnabled();
		if (schemaEnabledPreviously) {
			session.setSchemaMetadataEnabled(false);
		}

		CompletionStage<?> schemaRefresh;

		try {
			schemaAction.run();
		} finally {
			if (schemaEnabledPreviously) {
				// user may have set it programmatically so set it back programmatically
				schemaRefresh = session.setSchemaMetadataEnabled(null);
			} else {
				schemaRefresh = CompletableFuture.completedFuture(null);
			}
		}

		return schemaRefresh;
	}

	/**
	 * Run a {@link Runnable} and refresh the schema after finishing the runnable.
	 *
	 * @param session the session to use.
	 * @param schemaAction the runnable code block.
	 */
	static void withSchemaRefresh(Session session, Runnable schemaAction) {
		CompletableFutures.getUninterruptibly(withAsyncSchemaRefresh(session, schemaAction));
	}

	/**
	 * Run a {@link Runnable} and refresh the schema after finishing the runnable.
	 *
	 * @param session the session to use.
	 * @param schemaAction the runnable code block.
	 * @return a {@link CompletionStage} providing a handle to the schema refresh completion.
	 */
	static CompletionStage<?> withAsyncSchemaRefresh(Session session, Runnable schemaAction) {

		schemaAction.run();

		if (session.isSchemaMetadataEnabled()) {
			return session.refreshSchemaAsync();
		}

		return CompletableFuture.completedFuture(null);
	}

}
