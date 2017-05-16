package org.springframework.data.cassandra.mapping;

import org.springframework.data.annotation.Persistent;

import java.lang.annotation.*;

/**
 * Identifies an entity property to be stored in a text field as JSON.
 */
@Persistent
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.ANNOTATION_TYPE, ElementType.FIELD, ElementType.METHOD })
public @interface Embedded {
}
