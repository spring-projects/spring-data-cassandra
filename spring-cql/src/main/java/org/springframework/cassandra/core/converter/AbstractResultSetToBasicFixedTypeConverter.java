package org.springframework.cassandra.core.converter;

import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

/**
 * Thin wrapper that allows subclasses to delegate conversion of the given value to a {@link DefaultConversionService}.
 * 
 * @author Matthew T. Adams
 * @param <T>
 */
public abstract class AbstractResultSetToBasicFixedTypeConverter<T> extends AbstractResultSetConverter<T> {

	protected final static ConversionService CONVERTER = new DefaultConversionService();
}
