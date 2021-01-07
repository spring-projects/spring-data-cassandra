/*
 * Copyright 2016-2021 the original author or authors.
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

package org.springframework.data.cassandra.core.mapping;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.cassandra.core.mapping.IdInterfaceValidator.*;
import static org.springframework.data.cassandra.core.mapping.MapIdFactory.*;

import java.io.Serializable;
import java.util.Random;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link MapIdFactory}.
 *
 * @author Matthew T. Adams
 * @author Mark Paluch
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
	void testHappyExtendingMapId() {
		Random r = new Random();
		String s = "" + r.nextInt();
		Integer i = new Integer(r.nextInt());

		HappyExtendingMapIdAndSerializable id = id(HappyExtendingMapIdAndSerializable.class);

		assertThat(id.string()).isNull();
		assertThat(id.number()).isNull();
		assertThat(id.getString()).isNull();
		assertThat(id.getNumber()).isNull();

		id.setNumber(i);
		assertThat(id.getNumber()).isEqualTo(i);
		assertThat(id.number()).isEqualTo(i);
		assertThat(id.get("number")).isEqualTo(i);

		HappyExtendingMapIdAndSerializable returned = null;

		returned = id.number(i = r.nextInt());
		assertThat(id).isSameAs(returned);
		assertThat(id.getNumber()).isEqualTo(i);
		assertThat(id.number()).isEqualTo(i);
		assertThat(id.get("number")).isEqualTo(i);

		id.put("number", i = r.nextInt());
		assertThat(id.getNumber()).isEqualTo(i);
		assertThat(id.number()).isEqualTo(i);
		assertThat(id.get("number")).isEqualTo(i);

		id.setString(s);
		assertThat(id.getString()).isEqualTo(s);
		assertThat(id.string()).isEqualTo(s);
		assertThat(id.get("string")).isEqualTo(s);

		returned = id.string(s = "" + r.nextInt());
		assertThat(id).isSameAs(returned);
		assertThat(id.getString()).isEqualTo(s);
		assertThat(id.string()).isEqualTo(s);
		assertThat(id.get("string")).isEqualTo(s);

		returned = id.withString(s = "" + r.nextInt());
		assertThat(id).isSameAs(returned);
		assertThat(id.getString()).isEqualTo(s);
		assertThat(id.string()).isEqualTo(s);
		assertThat(id.get("string")).isEqualTo(s);

		id.put("string", s = "" + r.nextInt());
		assertThat(id.getString()).isEqualTo(s);
		assertThat(id.string()).isEqualTo(s);
		assertThat(id.get("string")).isEqualTo(s);

		id.setString(null);
		assertThat(id.getString()).isNull();
		assertThat(id.string()).isNull();
		assertThat(id.get("string")).isNull();

		id.setNumber(null);
		assertThat(id.getNumber()).isNull();
		assertThat(id.number()).isNull();
		assertThat(id.get("number")).isNull();
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
	void testHappyExtendingNothing() {
		Random r = new Random();
		String s = "" + r.nextInt();
		Integer i = new Integer(r.nextInt());

		HappyExtendingNothing id = id(HappyExtendingNothing.class);

		assertThat(id instanceof Serializable).isTrue();
		assertThat(id instanceof MapId).isTrue();
		MapId mapid = (MapId) id;

		assertThat(id.string()).isNull();
		assertThat(id.number()).isNull();
		assertThat(id.getString()).isNull();
		assertThat(id.getNumber()).isNull();

		id.setNumber(i);
		assertThat(id.getNumber()).isEqualTo(i);
		assertThat(id.number()).isEqualTo(i);
		assertThat(mapid.get("number")).isEqualTo(i);

		HappyExtendingNothing returned = null;

		returned = id.number(i = r.nextInt());
		assertThat(id).isSameAs(returned);
		assertThat(id.getNumber()).isEqualTo(i);
		assertThat(id.number()).isEqualTo(i);
		assertThat(mapid.get("number")).isEqualTo(i);

		mapid.put("number", i = r.nextInt());
		assertThat(id.getNumber()).isEqualTo(i);
		assertThat(id.number()).isEqualTo(i);
		assertThat(mapid.get("number")).isEqualTo(i);

		id.setString(s);
		assertThat(id.getString()).isEqualTo(s);
		assertThat(id.string()).isEqualTo(s);
		assertThat(mapid.get("string")).isEqualTo(s);

		returned = id.string(s = "" + r.nextInt());
		assertThat(id).isSameAs(returned);
		assertThat(id.getString()).isEqualTo(s);
		assertThat(id.string()).isEqualTo(s);
		assertThat(mapid.get("string")).isEqualTo(s);

		returned = id.withString(s = "" + r.nextInt());
		assertThat(id).isSameAs(returned);
		assertThat(id.getString()).isEqualTo(s);
		assertThat(id.string()).isEqualTo(s);
		assertThat(mapid.get("string")).isEqualTo(s);

		mapid.put("string", s = "" + r.nextInt());
		assertThat(id.getString()).isEqualTo(s);
		assertThat(id.string()).isEqualTo(s);
		assertThat(mapid.get("string")).isEqualTo(s);

		id.setString(null);
		assertThat(id.getString()).isNull();
		assertThat(id.string()).isNull();
		assertThat(mapid.get("string")).isNull();

		id.setNumber(null);
		assertThat(id.getNumber()).isNull();
		assertThat(id.number()).isNull();
		assertThat(mapid.get("number")).isNull();
	}

	private class IdClass {}

	interface Foo {}

	private interface LiteralGet {
		String get();
	}

	private interface GetterReturningVoid {
		void getString();
	}

	private interface GetReturningVoid {
		void string();
	}

	private interface MethodWithMoreThanOneArgument {
		void foo(Object a, Object b);
	}

	private interface LiteralSet {
		void set(String s);
	}

	private interface LiteralWith {
		void with(String s);
	}

	private interface SetterMethodNotReturningVoidOrThis {
		String string(String s);
	}

	private interface SetMethodNotReturningVoidOrThis {
		String setString(String s);
	}

	private interface WithMethodNotReturningVoidOrThis {
		String withString(String s);
	}

	@Test
	void testUnhappies() {

		Class<?>[] interfaces = new Class<?>[] { IdClass.class, LiteralGet.class, GetterReturningVoid.class,
				GetReturningVoid.class, MethodWithMoreThanOneArgument.class, LiteralSet.class, LiteralWith.class,
				SetterMethodNotReturningVoidOrThis.class, SetMethodNotReturningVoidOrThis.class,
				WithMethodNotReturningVoidOrThis.class };

		for (Class<?> idInterface : interfaces) {
			try {
				validate(idInterface);
				fail("should've caught IdInterfaceException validating interface " + idInterface);
			} catch (IdInterfaceExceptions e) {
				assertThat(e.getCount()).isEqualTo(1);
			}
		}
	}
}
