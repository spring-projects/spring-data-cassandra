/*
 * Copyright 2013-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.cassandra.config;

import com.datastax.driver.core.Cluster;

/**
 * Configuration callback class to allow a user to apply additional configuration logic to the {@link Cluster.Builder}.
 *
 * @author John Blum
 * @author Mark Paluch
 * @since 1.5
 * @see com.datastax.driver.core.Cluster
 */
@FunctionalInterface
public interface ClusterBuilderConfigurer {

	/**
	 * Apply addition configuration to the {@link com.datastax.driver.core.Cluster.Builder}.
	 *
	 * @param clusterBuilder {@link Cluster.Builder} to configure.
	 * @return the argument to the {@code clusterBuilder} parameter.
	 * @see com.datastax.driver.core.Cluster.Builder
	 */
	Cluster.Builder configure(Cluster.Builder clusterBuilder);
}
