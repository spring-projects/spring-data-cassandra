package org.springframework.data.cassandra.config;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

/**
 * Class that encapsulates all the problems encountered during cassandra schema validation
 *
 * @author Mikhail Polivakha
 */
public class CassandraSchemaValidationProfile {

    private final List<ValidationError> validationErrors;

    public CassandraSchemaValidationProfile(List<ValidationError> validationErrors) {
        this.validationErrors = validationErrors;
    }

    public static CassandraSchemaValidationProfile empty() {
        return new CassandraSchemaValidationProfile(new LinkedList<>());
    }

    public void addValidationErrors(List<String> message) {
        if (!CollectionUtils.isEmpty(message)) {
            this.validationErrors.addAll(message.stream().map(ValidationError::new).collect(Collectors.toSet()));
        }
    }

    public record ValidationError(String errorMessage) { }

    public boolean validationFailed() {
        return !validationErrors.isEmpty();
    }

    public String renderExceptionMessage() {

        Assert.state(validationFailed(), "Schema validation was successful but error message rendering requested");

        StringBuilder constructedMessage = new StringBuilder("The following errors were encountered during cassandra schema validation:\n");
        validationErrors.forEach(validationError -> constructedMessage.append("\t- %s\n".formatted(validationError.errorMessage())));
        return constructedMessage.toString();
    }
}
