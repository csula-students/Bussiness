package edu.csula.datascience.acquisition;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

/**
 * A test case to show how to use Collector and Source
 */
public class CollectorTest {
    private Collector<SimpleModel, MockData> collector;
    private Source<MockData> source;

    @Before
    public void setup() {
        collector = new MockCollector();
        source = new MockSource();
    }

    @Test
    public void mungee() throws Exception {
        Collection<MockData> list = source.provide();
        Collection<SimpleModel> expectedList = ((MockCollector) collector).mungee(list);
   
        Assert.assertTrue(list.size() > expectedList.size());
    }
}