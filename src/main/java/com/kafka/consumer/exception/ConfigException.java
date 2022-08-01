package com.kafka.consumer.exception;

public class ConfigException extends RuntimeException {

    public ConfigException(String message, Throwable ex) {
        super(message, ex);
    }
}
