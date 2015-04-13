/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.utils;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.io.Files;
import org.zenoss.utils.impl.ZenPackImpl;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Provides utility methods for working with ZenPacks.
 */
public final class ZenPacks {

    private static Collection<String> zenpackDirectories = null;

    // Statically define the set of directories where easy-install.pth files can be as
    // $ZENHOME/ZenPacks and /var/zenoss/ZenPacks.
    static {
        zenpackDirectories = new HashSet<String>();
        try {
            zenpackDirectories.add(Zenoss.zenPath("ZenPacks"));
        } catch(Exception e) {
            // Assume the exception was due to $ZENHOME being undefined and assume ZENHOME is /opt/zenoss.
            zenpackDirectories.add("/opt/zenoss/ZenPacks");
        }
        zenpackDirectories.add("/var/zenoss/ZenPacks");
    }

    private ZenPacks() {
    }

    /**
     * Returns a collection of directory paths parsed from the given Python .pth file.
     */
    private static Collection<String> parsePthFile(File pthFile) throws ZenossException {
        List<String> paths;
        try {
            paths = Files.readLines(pthFile, Charset.defaultCharset());
        } catch (IOException e) {
            throw new ZenossException("Unable to parse ZenPack path file.", e);
        }
        // Make all the paths absolute
        Collection<String> absolutepaths = Collections2.transform(paths, new Function<String, String>() {
            @Override
            public String apply(String s) {
                s = s.trim();
                if (s.length() == 0) {
                    // Blank line
                    return null;
                }
                if (!s.startsWith("/")) {
                    // Path relative to easy-install.pth; make it absolute
                    try {
                        s = new File(Zenoss.zenPath("ZenPacks", s)).getCanonicalPath();
                    } catch (ZenossException ignored) {
                    } catch (IOException ignored) {
                    }
                }
                return s;
            }
        });
        return Collections2.filter(absolutepaths, new com.google.common.base.Predicate<String>() {
            public boolean apply(String input) {
                return input != null && new File(input).isDirectory();
            }
        });
    }

    /**
     * Returns a collection of ZenPack paths parsed from easy-install.pth files.
     *
     * By default, $ZENHOME/ZenPacks/easy-install.pth and /var/zenoss/ZenPacks/easy-install.pth are the pth files
     * that parsed.
     *
     * @return Paths to ZenPack roots, guaranteed to exist.
     * @throws ZenossException If ZENHOME does not exist or the path file can't be parsed.
     */
    private static Collection<String> getZenPackPaths() throws ZenossException {
        Collection<String> zenpackPaths = new HashSet<String>();
        for (String zenpackDirectory : zenpackDirectories) {
            File pathFile = new File(zenpackDirectory + "/easy-install.pth");
            if (pathFile.isFile()) {
                zenpackPaths.addAll(parsePthFile(pathFile));
            }
        }
        return zenpackPaths;
    }

    /**
     * Set the collection of directories where easy-install.pth files can be found.
     */
    public static void setZenPackDirectories(Collection<String> directories) {
        zenpackDirectories = directories;
    }

    /**
     * Get a ZenPack by name.
     *
     * @param name The dotted name of the ZenPack. If not fully qualified (i.e., ZenPacks.*.*),
     *             the ZenPacks.zenoss namespace will be assumed.
     * @return The ZenPack object or null if no pack with that name is installed
     * @throws ZenossException If the ZenPack list couldn't be loaded
     */
    public static ZenPack getZenPack(String name) throws ZenossException {
        // If no namespace is specified, assume ZenPacks.zenoss
        if (!name.startsWith("ZenPacks")) {
            name = "ZenPacks.zenoss." + name;
        }
        for (ZenPack pack : getAllZenPacks()) {
            if (name.equals(pack.getName())) {
                return pack;
            }
        }
        return null;
    }

    /**
     * Get all installed ZenPacks.
     *
     * @return A list of ZenPack objects
     * @throws ZenossException If the list of ZenPacks can't be parsed.
     */
    public static List<ZenPack> getAllZenPacks() throws ZenossException {
        Collection<String> paths = getZenPackPaths();
        List<ZenPack> zenpacks = new ArrayList<ZenPack>(paths.size());
        for (String path : paths) {
            ZenPack zp = new ZenPackImpl(path);
            zenpacks.add(zp);
        }
        return zenpacks;
    }

    /**
     * Get the paths to all queue config files defined in ZenPacks.
     *
     * @return A collection of paths
     * @throws ZenossException If the list of ZenPacks can't be parsed.
     */
    public static Collection<String> getQueueConfigPaths() throws ZenossException {
        Set<String> paths = new HashSet<String>();
        for (ZenPack pack : getAllZenPacks()) {
            paths.addAll(pack.getQueueConfigPaths());
        }
        return paths;
    }
}
