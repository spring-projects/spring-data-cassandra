package org.springframework.data.cassandra.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.UUID;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.annotation.Id;
import org.springframework.data.cassandra.CassandraSchemaValidationException;
import org.springframework.data.cassandra.core.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.core.mapping.Column;
import org.springframework.data.cassandra.core.mapping.Table;
import org.springframework.data.cassandra.repository.support.IntegrationTestConfig;
import org.springframework.data.cassandra.support.CqlDataSet;
import org.springframework.data.cassandra.test.util.CassandraExtension;

/**
 * @author Mikhail Polivakha
 */
class CassandraSchemaValidatorTest {

    @RegisterExtension
    static CassandraExtension cassandraExtension = new CassandraExtension(
      CqlDataSet.fromClassPath("org/springframework/data/cassandra/core/cql/session/init/schema-validation.cql")
    );

    @Configuration
    static class Config extends IntegrationTestConfig {

        @Bean("cassandraSchemaValidator")
        CassandraSchemaValidator cassandraSchemaValidator(
          CqlSessionFactoryBean cqlSessionFactoryBean,
          CassandraMappingContext cassandraMappingContext,
          @Value("${validation.mode.strict:true}") Boolean validationModeStrict
        ) {
            return new CassandraSchemaValidator(
              cqlSessionFactoryBean,
              cassandraMappingContext,
              validationModeStrict
            );
        }

        @Override
        public SchemaAction getSchemaAction() {
            return SchemaAction.NONE;
        }

        @Override
        protected String getKeyspaceName() {
            return "validation_keyspace";
        }

        @Bean
        CassandraMappingContext cassandraMappingContext() {
            return new CassandraMappingContext();
        }
    }

    @Configuration
    static class WithAutoCreationConfig extends Config {

        @Override
        public SchemaAction getSchemaAction() {
            return SchemaAction.CREATE;
        }
    }

    @Table(value = "should_pass")
    static class ShouldPass {

        @Id
        private UUID id;

        @Column(value = "name")
        private String name;

        @Column(value = "some_type")
        private String type;

        private Integer status;

        private Integer precision;
    }

    @Table(value = "should_fail")
    static class ShouldFail {

        @Id
        private UUID id;

        @Column(value = "name")
        private String name;

        @Column(value = "some_type")
        private String type;

        private Integer status;

        private Integer precision;

        private Integer noSuchColumn;
    }

    @Table(value = "no_such_table")
    static class NoSuchTable {

        @Id
        private UUID id;
    }

    @Test
    void testValidationFailedWithNoSchemaAction() {
        try {
            new AnnotationConfigApplicationContext(Config.class);
            Assertions.fail(); // Context should not boot
        } catch (CassandraSchemaValidationException exception) {
            String message = exception.getMessage();
            assertThat(message).contains("Expected table name is 'no_such_table', but no such table in keyspace");
            assertThat(message).contains("Expected 'TEXT' data type for 'name' property");
            assertThat(message).contains("Unable to locate target column for persistent property 'noSuchColumn'");
        }
    }

    @Test
    void testValidationPassedWithSchemaAutoCreation() {
        try {
            System.setProperty("validation.mode.strict", "false");
            new AnnotationConfigApplicationContext(Config.class);
            System.clearProperty("validation.mode.strict");
        } catch (CassandraSchemaValidationException exception) {
            Assertions.fail(); // Context should load successfully, no exception should be thrown
        }
    }

    @Test
    void testWhenSchemaIsAutoCreatedThenValidationShouldPass() {
        try {
            var applicationContext = new AnnotationConfigApplicationContext(WithAutoCreationConfig.class);
            CassandraSchemaValidator cassandraSchemaValidator = applicationContext.getBean("cassandraSchemaValidator", CassandraSchemaValidator.class);
            Assertions.assertTrue(cassandraSchemaValidator.isStrictValidation());
        } catch (CassandraSchemaValidationException exception) {
            Assertions.fail(); // Context should load successfully, no exception should be thrown
        }
    }
}