package org.springframework.data.cassandra.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * @author Mikhail Polivakha
 */
class CassandraSchemaValidationProfileUnitTest {

    @Test
    void testRenderingValidationErrorMessageOnSuccessfulValidation() {
        CassandraSchemaValidationProfile empty = CassandraSchemaValidationProfile.empty();

        assertThatThrownBy(empty::renderExceptionMessage).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testRenderingValidationErrorMessageOnFailedValidation() {
        CassandraSchemaValidationProfile empty = CassandraSchemaValidationProfile.empty();

        empty.addValidationErrors(List.of("Something went wrong"));

        assertThat(empty.renderExceptionMessage()).contains("- Something went wrong");
    }
}