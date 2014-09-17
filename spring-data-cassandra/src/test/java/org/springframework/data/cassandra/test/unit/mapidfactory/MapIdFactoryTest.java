package org.springframework.data.cassandra.test.unit.mapidfactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.springframework.data.cassandra.repository.support.MapIdFactory.id;

import java.util.Random;

import org.junit.Test;
import org.springframework.data.cassandra.repository.MapId;

public class MapIdFactoryTest {

	static interface MyId extends MapId {
		MyId string(String s);

		void setString(String s);

		MyId withString(String s);

		String string();

		String getString();

		MyId number(Integer i);

		void setNumber(Integer i);

		Integer number();

		Integer getNumber();
	}

	@Test
	public void test() {
		Random r = new Random();
		String s = "" + r.nextInt();
		Integer i = new Integer(r.nextInt());

		MyId id = id(MyId.class);

		assertNull(id.string());
		assertNull(id.number());
		assertNull(id.getString());
		assertNull(id.getNumber());

		id.setNumber(i);
		assertEquals(i, id.getNumber());
		assertEquals(i, id.number());
		assertEquals(i, id.get("number"));

		MyId returned = null;

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
}
