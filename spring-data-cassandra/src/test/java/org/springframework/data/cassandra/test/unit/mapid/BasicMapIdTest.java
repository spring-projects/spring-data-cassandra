package org.springframework.data.cassandra.test.unit.mapid;

import org.junit.Test;
import org.springframework.data.cassandra.repository.support.BasicMapId;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class BasicMapIdTest {
    @Test
    public void testMapConstructor() {
        Map<String, Serializable> map = new HashMap<String, Serializable>();
        map.put("field1", "value1");
        map.put("field2", 2);

        BasicMapId basicMapId = new BasicMapId(map);

        assertEquals(basicMapId.get("field1"), map.get("field1"));
        assertEquals(basicMapId.get("field2"), map.get("field2"));
    }
}
