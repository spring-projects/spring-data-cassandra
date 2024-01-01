package org.springframework.data.cassandra.core.cql.util;

import java.util.Collection;

import com.datastax.oss.driver.api.querybuilder.term.Term;

public class ListTerm extends AbstractCollectionTerm {

    public ListTerm(Collection<? extends Term> components) {
        super(components);
    }

    @Override
    public EnclosingLiterals enclosingLiterals() {
        return EnclosingLiterals.of("[", "]");
    }
}
