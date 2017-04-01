package com.refactorlabs.cs378.utils;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Created by DilanHira on 9/24/16.
 */
public class Utils {
    public static final String MAPPER_COUNTER_GROUP = "some string";
    public static final String REDUCER_COUNTER_GROUP = "some other string";
    public static void printClassPath() {
        ClassLoader cl = ClassLoader.getSystemClassLoader(); URL[] urls = ((URLClassLoader) cl).getURLs(); System.out.println("classpath BEGIN"); for (URL url : urls) {
            System.out.println(url.getFile());
        } System.out.println("classpath END"); System.out.flush();
    }
}
