package org.springframework.data.cassandra.test.integration.repository;

import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.ConverterFactory;

public class FromJSONConverter implements ConverterFactory<String, Object> {

    ObjectMapper om = new ObjectMapper();

    @Override
    public <T> Converter<String, T> getConverter(final Class<T> targetType) {
        return new Converter<String, T>() {

            @Override
            public T convert(String source) {
                try {
                    return om.readValue(source, targetType);
                } catch (Exception e) {
                    throw new RuntimeException("Cannot deserialize JSON :"+source, e);
                }
            }
        };
    }

}
