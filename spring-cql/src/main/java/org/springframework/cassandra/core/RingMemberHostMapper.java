/*
 * Copyright 2013-2014 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cassandra.core;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.springframework.util.Assert;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * @author David Webb
 * @param <T>
 */
public class RingMemberHostMapper implements HostMapper<RingMember> {

	/* (non-Javadoc)
	 * @see org.springframework.cassandra.core.HostMapper#mapHosts(java.util.Set)
	 */
	@Override
	public List<RingMember> mapHosts(Set<Host> hosts) throws DriverException {

		List<RingMember> members = new ArrayList<RingMember>();

		Assert.notNull(hosts);
		Assert.notEmpty(hosts);

		RingMember r = null;
		for (Host host : hosts) {
			r = new RingMember(host);
			members.add(r);
		}

		return members;

	}
}
