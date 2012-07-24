/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.utils;

import java.io.File;

/**
 * Provides basic utility methods for working with a Zenoss instance.
 */
public final class Zenoss {

    private static String _zenhome = null;

    private Zenoss() {
    }

    /**
     * Returns the path of the directory of the Zenoss instance configured for the current environment.
     *
     * @return A file path, guaranteed to exist
     * @throws ZenossException If no valid ZENHOME is found
     */
    public static String getZenHome() throws ZenossException {
        if (_zenhome == null) {
            String zenhome = System.getProperty("ZENHOME");
            if (zenhome == null) {
                zenhome = System.getenv("ZENHOME");
            }
            if (zenhome == null || !new File(zenhome).isDirectory()) {
                throw new ZenossException("No valid ZENHOME could be found.");
            }
            // Strip trailing slash
            _zenhome = new File(zenhome).getAbsolutePath();
        }
        return _zenhome;
    }

    /**
     * Get an absolute path underneath ZENHOME.
     *
     * @param segments The segments of the path relative to ZENHOME
     * @return A file path, NOT guaranteed to exist
     * @throws ZenossException If no valid ZENHOME is found
     */
    public static String zenPath(String... segments) throws ZenossException {
        StringBuilder builder = new StringBuilder();
        builder.append(getZenHome());
        for (String s : segments) {
            builder.append(File.separator);
            builder.append(s);
        }
        return new File(builder.toString()).getAbsolutePath();
    }
}
