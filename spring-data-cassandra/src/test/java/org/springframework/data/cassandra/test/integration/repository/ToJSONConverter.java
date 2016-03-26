package org.springframework.data.cassandra.test.integration.repository;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.core.convert.converter.Converter;

public class ToJSONConverter implements Converter<Object, String> {

    ObjectMapper om = new ObjectMapper();

    @Override
    public String convert(Object source) {
        try {
            return om.writeValueAsString(source);
        } catch (Exception e) {
            throw new RuntimeException("Cannot serialize to JSON", e);
        }
    }

}
