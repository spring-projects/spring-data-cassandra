/*
 * Copyright 2016-2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.data.cassandra.util;

import org.junit.Test;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.SpelCompilerMode;
import org.springframework.expression.spel.SpelParserConfiguration;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;

import javax.swing.text.Segment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Unit tests for class {@link SpelUtils org.springframework.data.cassandra.util.SpelUtils}.
 *
 * @author Michael Hausegger
 * @date 20.02.2017
 * @see SpelUtils
 **/
public class SpelUtilsTest {


    @Test
    public void testOne() throws Exception {

        SpelExpressionParser spelExpressionParser = SpelUtils.DEFAULT_PARSER;
        String string = SpelUtils.evaluate((CharSequence) "cellEditor", (EvaluationContext) null, (ExpressionParser) spelExpressionParser);

        assertEquals("cellEditor", string);

    }


    @Test
    public void testTwo() throws Exception {

        SpelExpressionParser spelExpressionParser = new SpelExpressionParser();
        String string = SpelUtils.evaluate((CharSequence) "", (EvaluationContext) null, (ExpressionParser) spelExpressionParser);

        assertEquals("", string);

    }


    @Test
    public void testThree() throws Exception {

        Class<String> clasz = String.class;
        SpelParserConfiguration spelParserConfiguration = new SpelParserConfiguration(false, false);
        SpelExpressionParser spelExpressionParser = new SpelExpressionParser(spelParserConfiguration);
        String string = SpelUtils.evaluate((CharSequence) "+18:00", (EvaluationContext) null, clasz, (ExpressionParser) spelExpressionParser);

        assertFalse(spelParserConfiguration.isAutoGrowNullReferences());
        assertFalse(spelParserConfiguration.isAutoGrowCollections());

        assertEquals(SpelCompilerMode.OFF, spelParserConfiguration.getCompilerMode());
        assertEquals(2147483647, spelParserConfiguration.getMaximumAutoGrowSize());

        assertEquals("+18:00", string);

    }


    @Test
    public void testFour() throws Exception {

        Class<Object> clasz = Object.class;
        String string = (String) SpelUtils.evaluate((CharSequence) "caret-begin-word", (EvaluationContext) null, clasz);

        assertEquals("caret-begin-word", string);

    }


    @Test
    public void testFive() throws Exception {

        String string = SpelUtils.evaluate((CharSequence) "", (EvaluationContext) null);

        assertEquals("", string);

    }


    @Test
    public void testSix() throws Exception {

        String string = SpelUtils.evaluate((CharSequence) "caret-begin-word", (EvaluationContext) null);

        assertEquals("caret-begin-word", string);

    }


    @Test(expected = StringIndexOutOfBoundsException.class)
    public void testSevenRaisesStringIndexOutOfBoundsException() throws Exception {

        char[] charArray = new char[2];
        Segment segment = new Segment(charArray, (-2826), 0);
        StandardEvaluationContext standardEvaluationContext = new StandardEvaluationContext((Object) charArray);
        Class<Object> clasz = Object.class;

        SpelUtils.evaluate((CharSequence) segment, (EvaluationContext) standardEvaluationContext, clasz);

    }


}