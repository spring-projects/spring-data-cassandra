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

import org.junit.Test;
import org.springframework.data.cassandra.repository.MapId;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertSame;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

/**
 * Unit tests for {@link BasicMapId}.
 *
 * @author Matthew T. Adams
 * @author Michael Hausegger
 */
public class BasicMapIdUnitTests {


	@Test
	public void testMapConstructor() {
		Map<String, Serializable> map = new HashMap<String, Serializable>();
		map.put("field1", "value1");
		map.put("field2", 2);

		BasicMapId basicMapId = new BasicMapId(map);

		assertThat(map.get("field1")).isEqualTo(basicMapId.get("field1"));
		assertThat(map.get("field2")).isEqualTo(basicMapId.get("field2"));
	}


	@Test
	public void testOne() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		basicMapId.putAll(basicMapId);

		assertEquals(0, basicMapId.size());
		assertTrue(basicMapId.isEmpty());

	}


	@Test
	public void testTwo() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		int intOne = basicMapId.size();

		assertEquals(0, basicMapId.size());
		assertTrue(basicMapId.isEmpty());

		assertEquals(0, intOne);

	}


	@Test
	public void testThree() throws Exception {

		BasicMapId basicMapId = (BasicMapId)BasicMapId.id("#xAD]-}", (Serializable) "#xAD]-}");

		assertEquals(1, basicMapId.size());
		assertFalse(basicMapId.isEmpty());

		Serializable serializable = basicMapId.put("", (Serializable) "#xAD]-}");

		assertEquals(2, basicMapId.size());
		assertFalse(basicMapId.isEmpty());

		assertNull(serializable);

		String string = (String)basicMapId.put("", (Serializable) "#xAD]-}");

		assertEquals(2, basicMapId.size());
		assertFalse(basicMapId.isEmpty());

		assertNotNull(string);
		assertEquals("#xAD]-}", string);

	}


	@Test
	public void testFour() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		HashMap<String, String> hashMap = new HashMap<String, String>();
		Serializable serializable = basicMapId.put("", (Serializable) hashMap);

		assertEquals(1, basicMapId.size());
		assertFalse(basicMapId.isEmpty());

		assertEquals(0, hashMap.size());
		assertTrue(hashMap.isEmpty());

		assertNull(serializable);

		HashMap hashMapTwo = (HashMap)basicMapId.get("");

		assertEquals(1, basicMapId.size());
		assertFalse(basicMapId.isEmpty());

		assertTrue(hashMapTwo.isEmpty());
		assertEquals(0, hashMapTwo.size());

	}


	@Test(expected = IllegalArgumentException.class)
	public void testFiveRaisesIllegalArgumentException() throws Exception {

		BasicMapId.id((MapId) null);

	}


	@Test(expected = IllegalArgumentException.class)
	public void testSevenRaisesIllegalArgumentException() throws Exception {

		BasicMapId basicMapId = new BasicMapId((Map<String, Serializable>) null);

	}


	@Test
	public void testEight() throws Exception {

		BasicMapId basicMapId = (BasicMapId)BasicMapId.id("#xAD]-}", (Serializable) "#xAD]-}");

		assertFalse(basicMapId.isEmpty());
		assertEquals(1, basicMapId.size());

		int intOne = basicMapId.size();

		assertFalse(basicMapId.isEmpty());
		assertEquals(1, basicMapId.size());

		assertEquals(1, intOne);

	}


	@Test
	public void testNine() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		basicMapId.hashCode();

		assertEquals(0, basicMapId.size());
		assertTrue(basicMapId.isEmpty());

	}


	@Test
	public void testTen() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		Set<Map.Entry<String, Serializable>> set = (Set<Map.Entry<String, Serializable>>)basicMapId.entrySet();

		assertTrue(basicMapId.isEmpty());
		assertEquals(0, basicMapId.size());

		assertTrue(set.isEmpty());
		assertEquals(0, set.size());

	}


	@Test
	public void testEleven() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		Serializable serializable = basicMapId.put("4/i}1evv*I,Jc~", (Serializable) "4/i}1evv*I,Jc~");

		assertFalse(basicMapId.isEmpty());
		assertEquals(1, basicMapId.size());

		assertNull(serializable);

		Serializable serializableTwo = basicMapId.put("", (Serializable) "");

		assertFalse(basicMapId.isEmpty());
		assertEquals(2, basicMapId.size());

		assertNull(serializableTwo);

		String string = basicMapId.toString();

		assertFalse(basicMapId.isEmpty());
		assertEquals(2, basicMapId.size());

		assertNotNull(string);
		assertEquals("{  : , 4/i}1evv*I,Jc~ : 4/i}1evv*I,Jc~ }", string);

	}


	@Test
	public void testTwelve() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		boolean booleanOne = basicMapId.isEmpty();

		assertEquals(0, basicMapId.size());
		assertTrue(basicMapId.isEmpty());

		assertTrue(booleanOne);

	}


	@Test
	public void testThirteen() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		HashMap<String, String> hashMap = new HashMap<String, String>();
		Serializable serializable = basicMapId.put("", (Serializable) hashMap);

		assertFalse(basicMapId.isEmpty());
		assertEquals(1, basicMapId.size());

		assertEquals(0, hashMap.size());
		assertTrue(hashMap.isEmpty());

		assertNull(serializable);

		boolean booleanOne = basicMapId.isEmpty();

		assertFalse(basicMapId.isEmpty());
		assertEquals(1, basicMapId.size());

		assertFalse(booleanOne);

	}


	@Test
	public void testFourteen() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		Object object = new Object();
		boolean booleanOne = basicMapId.equals(object);

		assertTrue(basicMapId.isEmpty());
		assertEquals(0, basicMapId.size());

		assertFalse(booleanOne);

	}


	@Test
	public void testFifteen() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		BasicMapId basicMapIdTwo = new BasicMapId((Map<String, Serializable>) basicMapId);
		boolean booleanOne = basicMapIdTwo.equals(basicMapId);

		assertTrue(basicMapId.isEmpty());
		assertEquals(0, basicMapId.size());

		assertTrue(basicMapIdTwo.isEmpty());
		assertEquals(0, basicMapIdTwo.size());

		assertTrue(basicMapId.equals((Object)basicMapIdTwo));
		assertTrue(basicMapIdTwo.equals((Object)basicMapId));

		assertNotSame(basicMapId, basicMapIdTwo);
		assertNotSame(basicMapIdTwo, basicMapId);

		assertTrue(booleanOne);

	}


	@Test
	public void testSixteen() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		boolean booleanOne = basicMapId.equals(basicMapId);

		assertEquals(0, basicMapId.size());
		assertTrue(basicMapId.isEmpty());

		assertTrue(booleanOne);

	}


	@Test
	public void testSeventeen() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		boolean booleanOne = basicMapId.equals((Object) null);

		assertEquals(0, basicMapId.size());
		assertTrue(basicMapId.isEmpty());

		assertFalse(booleanOne);

	}


	@Test
	public void testEighteen() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		boolean booleanOne = basicMapId.containsValue(basicMapId);

		assertTrue(basicMapId.isEmpty());
		assertEquals(0, basicMapId.size());

		assertFalse(booleanOne);

	}


	@Test
	public void testNineteen() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		Serializable serializable = basicMapId.put("null=:z%uGo,\"", (Serializable) basicMapId);

		assertEquals(1, basicMapId.size());
		assertFalse(basicMapId.isEmpty());

		assertNull(serializable);

		boolean booleanOne = basicMapId.containsKey("null=:z%uGo,\"");

		assertEquals(1, basicMapId.size());
		assertFalse(basicMapId.isEmpty());

		assertTrue(booleanOne);

	}


	@Test
	public void testTwenty() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		Integer integer = new Integer((-766));
		boolean booleanOne = basicMapId.containsKey(integer);

		assertEquals(0, basicMapId.size());
		assertTrue(basicMapId.isEmpty());

		assertFalse(booleanOne);

	}


	@Test
	public void testTwentyOne() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		Collection<Serializable> collection = basicMapId.values();

		assertEquals(0, basicMapId.size());
		assertTrue(basicMapId.isEmpty());

		assertNotNull(collection);

	}


	@Test
	public void testTwentyTwo() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		basicMapId.clear();

		assertEquals(0, basicMapId.size());
		assertTrue(basicMapId.isEmpty());

	}


	@Test
	public void testTwentyThree() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		Set<String> set = basicMapId.keySet();

		assertEquals(0, basicMapId.size());
		assertTrue(basicMapId.isEmpty());

		assertTrue(set.isEmpty());
		assertEquals(0, set.size());

	}


	@Test
	public void testTwentyFour() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		Serializable serializable = basicMapId.remove((Object) basicMapId);

		assertTrue(basicMapId.isEmpty());
		assertEquals(0, basicMapId.size());

		assertNull(serializable);

	}


	@Test(expected = NullPointerException.class)
	public void testTwentyFiveRaisesNullPointerException() throws Exception {

		BasicMapId basicMapId = new BasicMapId();

		basicMapId.putAll((Map<? extends String, ? extends Serializable>) null);

	}


	@Test
	public void testTwentySix() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		Integer integer = new Integer((-766));
		Serializable serializable = basicMapId.get(integer);

		assertEquals(0, basicMapId.size());
		assertTrue(basicMapId.isEmpty());

		assertNull(serializable);

	}


	@Test
	public void testTwentySeven() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		BasicMapId basicMapIdTwo = basicMapId.with("8zE>gM!ei&~0<[j0z9", "8zE>gM!ei&~0<[j0z9");

		assertEquals(1, basicMapId.size());
		assertFalse(basicMapId.isEmpty());

		assertFalse(basicMapIdTwo.isEmpty());
		assertEquals(1, basicMapIdTwo.size());

		assertSame(basicMapId, basicMapIdTwo);
		assertSame(basicMapIdTwo, basicMapId);

		BasicMapId basicMapIdThree = (BasicMapId)BasicMapId.id((MapId) basicMapId);

		assertEquals(1, basicMapId.size());
		assertFalse(basicMapId.isEmpty());

		assertFalse(basicMapIdThree.isEmpty());
		assertEquals(1, basicMapIdThree.size());

		assertTrue(basicMapIdThree.equals((Object)basicMapIdTwo));
		assertTrue(basicMapIdThree.equals((Object)basicMapId));

		assertSame(basicMapId, basicMapIdTwo);
		assertNotSame(basicMapId, basicMapIdThree);

		assertNotSame(basicMapIdThree, basicMapIdTwo);
		assertNotSame(basicMapIdThree, basicMapId);

	}


	@Test
	public void testTwentyEight() throws Exception {

		BasicMapId basicMapId = new BasicMapId();
		BasicMapId basicMapIdTwo = (BasicMapId)BasicMapId.id((MapId) basicMapId);

		assertEquals(0, basicMapId.size());
		assertTrue(basicMapId.isEmpty());

		assertEquals(0, basicMapIdTwo.size());
		assertTrue(basicMapIdTwo.isEmpty());

		assertTrue(basicMapIdTwo.equals((Object)basicMapId));
		assertNotSame(basicMapId, basicMapIdTwo);

		assertNotSame(basicMapIdTwo, basicMapId);

	}


	@Test
	public void testTwentyNine() throws Exception {

		BasicMapId basicMapId = (BasicMapId)BasicMapId.id();

		assertTrue(basicMapId.isEmpty());
		assertEquals(0, basicMapId.size());

		BasicMapId basicMapIdTwo = basicMapId.with(", ", ", ");

		assertFalse(basicMapId.isEmpty());
		assertEquals(1, basicMapId.size());

		assertFalse(basicMapIdTwo.isEmpty());
		assertEquals(1, basicMapIdTwo.size());

		assertSame(basicMapId, basicMapIdTwo);
		assertSame(basicMapIdTwo, basicMapId);

		String string = (String)basicMapIdTwo.remove((Object) ", ");

		assertTrue(basicMapId.isEmpty());
		assertEquals(0, basicMapId.size());

		assertEquals(0, basicMapIdTwo.size());
		assertTrue(basicMapIdTwo.isEmpty());

		assertNotNull(string);
		assertSame(basicMapId, basicMapIdTwo);

		assertSame(basicMapIdTwo, basicMapId);
		assertEquals(", ", string);

	}


}
