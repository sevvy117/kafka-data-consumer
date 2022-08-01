package com.kafka.consumer.exception;

public class FileException extends RuntimeException {

    public FileException(String message, Throwable ex) {
        super(message, ex);
    }
}
