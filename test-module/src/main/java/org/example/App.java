package org.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class App {

    private static final Logger logger = LogManager.getLogger(App.class);

    public static void main( String[] args ) throws InterruptedException {
        for (int i = 1; i <= 10; i++) {
            logger.info("Test log {}", i);
            Thread.sleep(1000);
        }
    }
}
