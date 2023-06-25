/*
 * Copyright 2013-2023 the original author or authors.
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
package org.springframework.data.cassandra;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.util.CollectionUtils;

import com.datastax.oss.driver.api.core.metadata.Node;

/**
 * Spring data access exception for Cassandra when no host is available.
 *
 * @author Matthew T. Adams
 */
public class CassandraConnectionFailureException extends DataAccessResourceFailureException {

	private static final long serialVersionUID = 6299912054261646552L;

	private final Map<Node, List<Throwable>> messagesByHost = new HashMap<>();

	public CassandraConnectionFailureException(Map<Node, Throwable> map, String msg, Throwable cause) {
		super(msg, cause);
		map.forEach((node, throwable) -> messagesByHost.put(node, Collections.singletonList(throwable)));
	}

	public CassandraConnectionFailureException(String msg, Map<Node, List<Throwable>> map, Throwable cause) {
		super(msg, cause);
		this.messagesByHost.putAll(map);
	}

	@Deprecated(forRemoval = true)
	public Map<Node, Throwable> getMessagesByHost() {
		HashMap<Node, Throwable> singleMessageByHost = new HashMap<>();
		this.messagesByHost.forEach((node, throwables) -> singleMessageByHost.put(node, CollectionUtils.firstElement(throwables)));
		return singleMessageByHost;
	}

	public Map<Node, List<Throwable>> getAllMessagesByHost() {
		return Collections.unmodifiableMap(messagesByHost);
	}
}
