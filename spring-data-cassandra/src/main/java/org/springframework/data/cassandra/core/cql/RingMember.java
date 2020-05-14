/*
 * Copyright 2013-2020 the original author or authors.
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
package org.springframework.data.cassandra.core.cql;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.InetSocketAddress;

import com.datastax.oss.driver.api.core.metadata.Node;

import org.springframework.util.Assert;

/**
 * Domain object representing a Cassandra host.
 *
 * @author David Webb
 * @author Mark Paluch
 */
public final class RingMember implements Serializable {

	private static final long serialVersionUID = -2582309141903132916L;

	/*
	 * Ring attributes
	 */
	private final String hostName;

	private final String address;

	private final String dc;

	private final String rack;

	/**
	 * Creates a new {@link RingMember} given {@link Host}.
	 *
	 * @param host
	 * @return
	 */
	public static RingMember from(Node host) {
		return new RingMember(host);
	}

	private RingMember(Node host) {

		Assert.notNull(host, "Host must not be null");

		this.hostName = host.getListenAddress().map(InetSocketAddress::getHostName).orElse("unknown");
		this.address = host.getListenAddress().map(InetSocketAddress::getAddress).map(InetAddress::getHostAddress)
				.orElse("unknown");
		this.dc = host.getDatacenter();
		this.rack = host.getRack();
	}

	public String getHostName() {
		return hostName;
	}

	public String getAddress() {
		return address;
	}

	public String getDataCenter() {
		return dc;
	}

	public String getRack() {
		return rack;
	}
}
