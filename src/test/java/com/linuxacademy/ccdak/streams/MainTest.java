package com.linuxacademy.ccdak.streams;

import org.junit.Test;
import static org.junit.Assert.*;

public class MainTest {
    @Test public void testAppHasAGreeting() {
        StreamsMain classUnderTest = new StreamsMain();
        assertNotNull("app should have a greeting", classUnderTest);
    }
}
