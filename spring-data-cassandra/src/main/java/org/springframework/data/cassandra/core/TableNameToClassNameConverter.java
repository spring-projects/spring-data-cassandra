package org.springframework.data.cassandra.core;

import org.springframework.core.convert.converter.Converter;

public interface TableNameToClassNameConverter extends Converter<String, String> {
}
