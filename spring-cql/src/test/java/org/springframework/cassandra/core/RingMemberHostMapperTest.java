/*
 * Copyright 2017 the original author or authors.
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

package org.springframework.cassandra.core;

import java.util.LinkedHashSet;
import java.util.Set;

import com.datastax.driver.core.Host;
import org.junit.Test;

/**
 * Unit tests for class {@link RingMemberHostMapper org.springframework.cassandra.core.RingMemberHostMapper}.
 *
 * @author Michael Hausegger, hausegger.michael@googlemail.com
 */
public class RingMemberHostMapperTest {


	@Test(expected = NullPointerException.class)  //DATACASS-405
	public void testMapHostsThrowsNullPointerExceptionIfNullHostProvidedInParameterSet() throws Exception {

		Set<Host> hosts = new LinkedHashSet<Host>();
		RingMemberHostMapper ringMemberHostMapper = new RingMemberHostMapper();
		hosts.add(null);

		ringMemberHostMapper.mapHosts(hosts);
	}


	@Test(expected = IllegalArgumentException.class)  //DATACASS-405
	public void testMapHostsThrowsIllegalArgumentExceptionIfGivenParameterSetIsNull() throws Exception {

		RingMemberHostMapper ringMemberHostMapper = new RingMemberHostMapper();

		ringMemberHostMapper.mapHosts(null);
	}


	@Test(expected = IllegalArgumentException.class)  //DATACASS-405
	public void testMapHostsThrowsIllegalArgumentExceptionIfGivenParameterSetIsEmpty() throws Exception {

		RingMemberHostMapper ringMemberHostMapper = new RingMemberHostMapper();
		Set<Host> hosts = new LinkedHashSet<Host>();

		ringMemberHostMapper.mapHosts(hosts);
	}
}