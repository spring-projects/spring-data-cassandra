package org.springframework.data.cassandra.test.integration.repository;

import org.springframework.beans.factory.FactoryBean;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;

public class ConversionServiceFactoryBean implements FactoryBean<ConversionService> {

    FromJSONConverter fromJSONConverter = new FromJSONConverter();
    ToJSONConverter toJSONConverter = new ToJSONConverter();

    @Override
    public ConversionService getObject() throws Exception {
        DefaultConversionService svc = new DefaultConversionService();
        svc.addConverter(String.class, Address.class, fromJSONConverter.getConverter(Address.class));
        svc.addConverter(Address.class, String.class, toJSONConverter);
        return svc;
    }

    @Override
    public Class<?> getObjectType() {
        return ConversionService.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

}
