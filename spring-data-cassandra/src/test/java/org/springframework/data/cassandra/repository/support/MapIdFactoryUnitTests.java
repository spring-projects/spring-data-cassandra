/*
 * Copyright 2016 the original author or authors.
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

import static org.junit.Assert.*;
import static org.springframework.data.cassandra.repository.support.IdInterfaceValidator.*;
import static org.springframework.data.cassandra.repository.support.MapIdFactory.*;

import java.io.Serializable;
import java.util.Random;

import org.junit.Test;
import org.springframework.data.cassandra.repository.MapId;

/**
 * Unit tests for {@link MapIdFactory}.
 *
 * @author Matthew T. Adams
 */
public class MapIdFactoryUnitTests {

	interface HappyExtendingMapIdAndSerializable extends MapId, Serializable {
		HappyExtendingMapIdAndSerializable string(String s);

		void setString(String s);

		HappyExtendingMapIdAndSerializable withString(String s);

		String string();

		String getString();

		HappyExtendingMapIdAndSerializable number(Integer i);

		void setNumber(Integer i);

		Integer number();

		Integer getNumber();
	}

	@Test
	public void testHappyExtendingMapId() {
		Random r = new Random();
		String s = "" + r.nextInt();
		Integer i = new Integer(r.nextInt());

		HappyExtendingMapIdAndSerializable id = id(HappyExtendingMapIdAndSerializable.class);

		assertNull(id.string());
		assertNull(id.number());
		assertNull(id.getString());
		assertNull(id.getNumber());

		id.setNumber(i);
		assertEquals(i, id.getNumber());
		assertEquals(i, id.number());
		assertEquals(i, id.get("number"));

		HappyExtendingMapIdAndSerializable returned = null;

		returned = id.number(i = r.nextInt());
		assertSame(returned, id);
		assertEquals(i, id.getNumber());
		assertEquals(i, id.number());
		assertEquals(i, id.get("number"));

		id.put("number", i = r.nextInt());
		assertEquals(i, id.getNumber());
		assertEquals(i, id.number());
		assertEquals(i, id.get("number"));

		id.setString(s);
		assertEquals(s, id.getString());
		assertEquals(s, id.string());
		assertEquals(s, id.get("string"));

		returned = id.string(s = "" + r.nextInt());
		assertSame(returned, id);
		assertEquals(s, id.getString());
		assertEquals(s, id.string());
		assertEquals(s, id.get("string"));

		returned = id.withString(s = "" + r.nextInt());
		assertSame(returned, id);
		assertEquals(s, id.getString());
		assertEquals(s, id.string());
		assertEquals(s, id.get("string"));

		id.put("string", s = "" + r.nextInt());
		assertEquals(s, id.getString());
		assertEquals(s, id.string());
		assertEquals(s, id.get("string"));

		id.setString(null);
		assertNull(id.getString());
		assertNull(id.string());
		assertNull(id.get("string"));

		id.setNumber(null);
		assertNull(id.getNumber());
		assertNull(id.number());
		assertNull(id.get("number"));
	}

	interface HappyExtendingNothing {
		HappyExtendingNothing string(String s);

		void setString(String s);

		HappyExtendingNothing withString(String s);

		String string();

		String getString();

		HappyExtendingNothing number(Integer i);

		void setNumber(Integer i);

		Integer number();

		Integer getNumber();
	}

	@Test
	public void testHappyExtendingNothing() {
		Random r = new Random();
		String s = "" + r.nextInt();
		Integer i = new Integer(r.nextInt());

		HappyExtendingNothing id = id(HappyExtendingNothing.class);

		assertTrue(id instanceof Serializable);
		assertTrue(id instanceof MapId);
		MapId mapid = (MapId) id;

		assertNull(id.string());
		assertNull(id.number());
		assertNull(id.getString());
		assertNull(id.getNumber());

		id.setNumber(i);
		assertEquals(i, id.getNumber());
		assertEquals(i, id.number());
		assertEquals(i, mapid.get("number"));

		HappyExtendingNothing returned = null;

		returned = id.number(i = r.nextInt());
		assertSame(returned, id);
		assertEquals(i, id.getNumber());
		assertEquals(i, id.number());
		assertEquals(i, mapid.get("number"));

		mapid.put("number", i = r.nextInt());
		assertEquals(i, id.getNumber());
		assertEquals(i, id.number());
		assertEquals(i, mapid.get("number"));

		id.setString(s);
		assertEquals(s, id.getString());
		assertEquals(s, id.string());
		assertEquals(s, mapid.get("string"));

		returned = id.string(s = "" + r.nextInt());
		assertSame(returned, id);
		assertEquals(s, id.getString());
		assertEquals(s, id.string());
		assertEquals(s, mapid.get("string"));

		returned = id.withString(s = "" + r.nextInt());
		assertSame(returned, id);
		assertEquals(s, id.getString());
		assertEquals(s, id.string());
		assertEquals(s, mapid.get("string"));

		mapid.put("string", s = "" + r.nextInt());
		assertEquals(s, id.getString());
		assertEquals(s, id.string());
		assertEquals(s, mapid.get("string"));

		id.setString(null);
		assertNull(id.getString());
		assertNull(id.string());
		assertNull(mapid.get("string"));

		id.setNumber(null);
		assertNull(id.getNumber());
		assertNull(id.number());
		assertNull(mapid.get("number"));
	}

	class IdClass {}

	interface Foo {}

	interface IdExtendingNotMapId extends Foo {}

	interface LiteralGet {
		String get();
	}

	interface GetterReturningVoid {
		void getString();
	}

	interface GetReturningVoid {
		void string();
	}

	interface GetReturningNonSerializable {
		Object getFoo();
	}

	interface MethodWithMoreThanOneArgument {
		void foo(Object a, Object b);
	}

	interface LiteralSet {
		void set(String s);
	}

	interface LiteralWith {
		void with(String s);
	}

	interface SetterMethodNotReturningVoidOrThis {
		String string(String s);
	}

	interface SetMethodNotReturningVoidOrThis {
		String setString(String s);
	}

	interface WithMethodNotReturningVoidOrThis {
		String withString(String s);
	}

	interface SetterMethodTakingNonSerializable {
		void string(Object o);
	}

	interface SetMethodTakingNonSerializable {
		void string(Object o);
	}

	interface WithMethodTakingNonSerializable {
		void string(Object o);
	}

	@Test
	public void testUnhappies() {
		Class<?>[] interfaces = new Class<?>[] { IdClass.class, IdExtendingNotMapId.class, LiteralGet.class,
				GetterReturningVoid.class, GetReturningVoid.class, GetReturningNonSerializable.class,
				MethodWithMoreThanOneArgument.class, LiteralSet.class, LiteralWith.class,
				SetterMethodNotReturningVoidOrThis.class, SetMethodNotReturningVoidOrThis.class,
				WithMethodNotReturningVoidOrThis.class, SetterMethodTakingNonSerializable.class,
				SetMethodTakingNonSerializable.class, WithMethodTakingNonSerializable.class };
		for (Class<?> i : interfaces) {
			try {
				validate(i);
				fail("should've caught IdInterfaceException validating interface " + i);
			} catch (IdInterfaceExceptions e) {
				assertEquals(1, e.getCount());
			}
		}
	}
}
