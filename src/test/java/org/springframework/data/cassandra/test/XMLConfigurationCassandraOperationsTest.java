/*
 * Copyright 2010-2013 the original author or authors.
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
package org.springframework.data.cassandra.test;

import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * This class extends the main test class, and uses the XML Configuration files/classes
 * in order to test everything with that configuration path.
 * 
 * @author David Webb
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration (locations = "classpath:application-context.xml")
public class XMLConfigurationCassandraOperationsTest extends
		CassandraOperationsTest {

	/**
	 * 
	 */
	public XMLConfigurationCassandraOperationsTest() {
	}

}
