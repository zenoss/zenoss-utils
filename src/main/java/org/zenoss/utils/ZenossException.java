/*
 * Copyright (C) 2011, Zenoss Inc.  All Rights Reserved.
 */
package org.zenoss.utils;

/**
 * Base exception for Zenoss.
 */
public class ZenossException extends Exception {
    /**
     * Creates an exception with no message or cause.
     */
    public ZenossException() {
        super();
    }

    /**
     * Creates an exception with the specified message and cause.
     *
     * @param message
     *            Exception message.
     * @param cause
     *            Exception cause.
     */
    public ZenossException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates an exception with the specified message and no cause.
     *
     * @param message
     *            Exception message.
     */
    public ZenossException(String message) {
        super(message);
    }

    /**
     * Creates an exception with the specified cause.
     *
     * @param cause
     *            Exception cause.
     */
    public ZenossException(Throwable cause) {
        super(cause);
    }
}
