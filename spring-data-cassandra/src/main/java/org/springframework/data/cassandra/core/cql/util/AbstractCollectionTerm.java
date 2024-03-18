package org.springframework.data.cassandra.core.cql.util;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

import org.jetbrains.annotations.NotNull;
import org.springframework.lang.NonNull;

import com.datastax.oss.driver.api.querybuilder.term.Term;
import com.datastax.oss.driver.internal.querybuilder.CqlHelper;

/**
 * Represents an abstract collection like {@link Term} such as Set, List, Tuple in CQL
 *
 * @author Mikhail Polivakha
 */
public abstract class AbstractCollectionTerm implements Term {

    @NonNull
    private final Collection<? extends Term> components;

    /**
     * @return EnclosingLiterals that are used to render the collection of terms
     */
    public abstract EnclosingLiterals enclosingLiterals();

    public AbstractCollectionTerm(Collection<? extends Term> components) {
        this.components = Optional.ofNullable(components).orElse(Collections.emptySet());
    }

    @Override
    public boolean isIdempotent() {
        return components.stream().allMatch(Term::isIdempotent);
    }

    @Override
    public void appendTo(@NotNull StringBuilder builder) {
        EnclosingLiterals literals = this.enclosingLiterals();

        if (components.isEmpty()) {
            builder.append(literals.prefix).append(literals.postfix);
        } else {
            CqlHelper.append(components, builder, literals.prefix, ",", literals.postfix);
        }
    }

    protected static class EnclosingLiterals {

        private final String prefix;
        private final String postfix;

        protected EnclosingLiterals(String prefix, String postfix) {
            this.prefix = prefix;
            this.postfix = postfix;
        }

        protected static EnclosingLiterals of(String prefix, String postfix) {
            return new EnclosingLiterals(prefix, postfix);
        }
    }
}
