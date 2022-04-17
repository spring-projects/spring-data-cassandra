/*
 * Copyright 2022 the original author or authors.
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

import com.datastax.oss.driver.api.core.session.Session;

/**
 * Utility methods for executing schema actions with refresh disabled.
 *
 * @author Ammar Khaku
 */
class SchemaRefreshUtils {
	@FunctionalInterface
	interface ThrowingRunnable {
		void run() throws Exception;
	}

	/**
	 * Programmatically disables schema refreshes on the session and runs the provided Runnable,
	 * taking care to restore the previous state of schema refresh config on the provided session.
	 * Note that the session could have had schema refreshes enabled/disabled either
	 * programmatically or via config.
	 */
	static void withDisabledSchema(Session session, ThrowingRunnable r) throws Exception {
		boolean schemaEnabledPreviously = session.isSchemaMetadataEnabled();
		session.setSchemaMetadataEnabled(false);
		r.run();
		session.setSchemaMetadataEnabled(null); // triggers schema refresh if results in true
		if (schemaEnabledPreviously != session.isSchemaMetadataEnabled()) {
			// user may have set it programmatically so set it back programmatically
			session.setSchemaMetadataEnabled(schemaEnabledPreviously);
		}
	}
}
