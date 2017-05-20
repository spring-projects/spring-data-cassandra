/*
 * Copyright 2016-2017 the original author or authors.
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

package org.springframework.data.cassandra.repository.support;

import static org.assertj.core.api.Assertions.*;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

/**
 * Unit tests for {@link BasicMapId}.
 *
 * @author Matthew T. Adams
 * @author Michael Hausegger, hausegger.michael@googlemail.com
 */
public class BasicMapIdUnitTests {


	@Test  //DATACASS-405
	public void testMapConstructor() {

		Map<String, Serializable> map = new HashMap<String, Serializable>();
		map.put("field1", "value1");
		map.put("field2", 2);

		BasicMapId basicMapId = new BasicMapId(map);

		assertThat(map.get("field1")).isEqualTo(basicMapId.get("field1"));
		assertThat(map.get("field2")).isEqualTo(basicMapId.get("field2"));
	}


	@Test  //DATACASS-405
	public void testPutAllProvidingItselfEmptyAsInputParameter() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		basicMapId.putAll(basicMapId);

		assertThat(basicMapId.size()).isEqualTo(0);
		assertThat(basicMapId.isEmpty()).isTrue();
	}


	@Test  //DATACASS-405
	public void testNewEmptyBasicMapIdDoesNotContainAnyEntries() throws Exception {

		BasicMapId basicMapId = new BasicMapId();

		assertThat(basicMapId.size()).isEqualTo(0);
		assertThat(basicMapId.isEmpty()).isTrue();

	}


	@Test  //DATACASS-405
	public void testFactoryMethodConstructsObjectCorrectly() throws Exception {

		BasicMapId basicMapId = (BasicMapId) BasicMapId.id("a", "a");

		assertThat(basicMapId.size()).isEqualTo(1);
		assertThat(basicMapId.isEmpty()).isFalse();
		assertThat( (String) basicMapId.get("a")).isEqualTo("a");

	}


	@Test(expected = IllegalArgumentException.class)  //DATACASS-405
	public void testStaticInstantiationRaisesIllegalArgumentExceptionIfTriedWithNull() throws Exception {

		BasicMapId.id(null);

	}


	@Test(expected = IllegalArgumentException.class)  //DATACASS-405
	public void testInstantiationRaisesIllegalArgumentExceptionIfTriedWithNull() throws Exception {

		new BasicMapId((Map<String, Serializable>) null);

	}


	@Test  //DATACASS-405
	public void testCanReturnEmptyEntrySetIfEmpty() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		Set<Map.Entry<String, Serializable>> set = (Set<Map.Entry<String, Serializable>>) basicMapId.entrySet();

		assertThat(basicMapId.isEmpty()).isTrue();
		assertThat(basicMapId.size()).isEqualTo(0);

		assertThat(set.isEmpty()).isTrue();
		assertThat(set.size()).isEqualTo(0);

	}


	@Test  //DATACASS-405
	public void testPutCallableSubsequentlyCorrectly() throws Exception {

		BasicMapId basicMapId = new BasicMapId();

		basicMapId.put("a", "a");

		assertThat(basicMapId.isEmpty()).isFalse();
		assertThat(basicMapId.size()).isEqualTo(1);

		basicMapId.put("", "");

		assertThat(basicMapId.isEmpty()).isFalse();
		assertThat(basicMapId.size()).isEqualTo(2);

		assertThat(basicMapId.isEmpty()).isFalse();
		assertThat(basicMapId.size()).isEqualTo(2);

		assertThat(basicMapId.toString()).isNotNull();
		assertThat(basicMapId.toString()).isEqualTo("{  : , a : a }");

	}


	@Test  //DATACASS-405
	public void testCanWorkWithHashMapObjects() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		basicMapId.put("", new HashMap<String, String>());

		assertThat(basicMapId.isEmpty()).isFalse();
		assertThat(basicMapId.size()).isEqualTo(1);

		assertThat(basicMapId.isEmpty()).isFalse();
		assertThat(basicMapId.size()).isEqualTo(1);

		assertThat(basicMapId.isEmpty()).isFalse();

	}


	@Test  //DATACASS-405
	public void testEmptyBasicMapIdDoesNotConsiderItselfToBeEqualToBasicJavaObject() throws Exception {

		BasicMapId basicMapId = new BasicMapId();

		assertThat(basicMapId.isEmpty()).isTrue();
		assertThat(basicMapId.size()).isEqualTo(0);

		assertThat(basicMapId.equals(new Object())).isFalse();

	}


	@Test  //DATACASS-405
	public void testEqualsMethod() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		BasicMapId basicMapIdTwo = new BasicMapId((Map<String, Serializable>) basicMapId);

		assertThat(basicMapId.isEmpty()).isTrue();
		assertThat(basicMapId.size()).isEqualTo(0);

		assertThat(basicMapIdTwo.isEmpty()).isTrue();
		assertThat(basicMapIdTwo.size()).isEqualTo(0);

		assertThat(basicMapId.equals(basicMapIdTwo)).isTrue();
		assertThat(basicMapIdTwo.equals(basicMapId)).isTrue();

		assertThat(basicMapId).isNotSameAs(basicMapIdTwo);
		assertThat(basicMapIdTwo).isNotSameAs(basicMapId);

		assertThat(basicMapIdTwo.equals(basicMapId)).isTrue();
		assertThat(basicMapId.equals(basicMapId)).isTrue();

		assertThat(basicMapId.equals(null)).isFalse();


	}


	@Test  //DATACASS-405
	public void testContainsValue() throws Exception {

		BasicMapId basicMapId = new BasicMapId();

		assertThat(basicMapId.isEmpty()).isTrue();
		assertThat(basicMapId.size()).isEqualTo(0);

		assertThat(basicMapId.containsValue(basicMapId)).isFalse();

		basicMapId.put("",basicMapId);

		assertThat(basicMapId.containsValue(basicMapId)).isTrue();

	}


	@Test  //DATACASS-405
	public void testContainsKeyUsingString() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		basicMapId.put("a\"", basicMapId);

		assertThat(basicMapId.size()).isEqualTo(1);
		assertThat(basicMapId.isEmpty()).isFalse();

		assertThat(basicMapId.containsKey("a\"")).isTrue();

		assertThat(basicMapId.size()).isEqualTo(1);
		assertThat(basicMapId.isEmpty()).isFalse();

	}


	@Test  //DATACASS-405
	public void testContainsKeyUsingNegativeInteger() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		Integer integer = new Integer((-766));

		assertThat(basicMapId.size()).isEqualTo(0);
		assertThat(basicMapId.isEmpty()).isTrue();

		assertThat(basicMapId.containsKey(integer)).isFalse();
	}


	@Test  //DATACASS-405
	public void testValuesReturnsObjectEvenIfBasicMapIdDoesNotContainAnyValues() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		Collection<Serializable> collection = basicMapId.values();

		assertThat(basicMapId.size()).isEqualTo(0);
		assertThat(basicMapId.isEmpty()).isTrue();

		assertThat(collection).isNotNull();
	}


	@Test  //DATACASS-405
	public void testClearWorksEvenIfNoInternalValuesArePresent() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		basicMapId.clear();

		assertThat(basicMapId.size()).isEqualTo(0);
		assertThat(basicMapId.isEmpty()).isTrue();
	}


	@Test  //DATACASS-405
	public void testEmptySetGetsReturnedEvenIfObjectDoesNotContainInternalValues() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		Set<String> set = basicMapId.keySet();

		assertThat(basicMapId.size()).isEqualTo(0);
		assertThat(basicMapId.isEmpty()).isTrue();

		assertThat(set.isEmpty()).isTrue();
		assertThat(set.size()).isEqualTo(0);
	}


	@Test  //DATACASS-405
	public void testRemoveWorksEvenIfObjectDoesNotContainAnyValues() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		Serializable serializable = basicMapId.remove(basicMapId);

		assertThat(basicMapId.isEmpty()).isTrue();
		assertThat(basicMapId.size()).isEqualTo(0);

		assertThat(serializable).isNull();
	}


	@Test(expected = NullPointerException.class)  //DATACASS-405
	public void testTryingToPutAllNullRaisesNullPointerException() throws Exception {

		BasicMapId basicMapId = new BasicMapId();

		basicMapId.putAll((Map<? extends String, ? extends Serializable>) null);
	}


	@Test  //DATACASS-405
	public void testWith() throws Exception {

		BasicMapId basicMapId = (BasicMapId) BasicMapId.id();

		assertThat(basicMapId.isEmpty()).isTrue();
		assertThat(basicMapId.size()).isEqualTo(0);

		BasicMapId basicMapIdTwo = basicMapId.with(", ", ", ");

		assertThat(basicMapId.isEmpty()).isFalse();
		assertThat(basicMapId.size()).isEqualTo(1);

		assertThat(basicMapIdTwo.isEmpty()).isFalse();
		assertThat(basicMapIdTwo.size()).isEqualTo(1);

		assertThat(basicMapId).isSameAs(basicMapIdTwo);
		assertThat(basicMapIdTwo).isSameAs(basicMapId);

	}


	@Test  //DATACASS-405
	public void testRemove() throws Exception {

		BasicMapId basicMapId = (BasicMapId) BasicMapId.id();

		assertThat(basicMapId.isEmpty()).isTrue();
		assertThat(basicMapId.size()).isEqualTo(0);

		BasicMapId basicMapIdTwo = basicMapId.with(", ", ", ");

		String string = (String) basicMapIdTwo.remove(", ");

		assertThat(basicMapId.isEmpty()).isTrue();
		assertThat(basicMapId.size()).isEqualTo(0);

		assertThat(basicMapIdTwo.size()).isEqualTo(0);
		assertThat(basicMapIdTwo.isEmpty()).isTrue();

		assertThat(string).isNotNull();
		assertThat(basicMapId).isSameAs(basicMapIdTwo);

		assertThat(basicMapIdTwo).isSameAs(basicMapId);
		assertThat(string).isEqualTo(", ");
	}


}
