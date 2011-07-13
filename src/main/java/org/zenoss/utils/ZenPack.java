package org.zenoss.utils;

import java.util.List;

public interface ZenPack {

    /**
     * Get the name of the ZenPack.
     *
     * @return The fully qualified dotted name of the ZenPack.
     */
    public String getName();

    /**
     * Get the path to the ZenPack root directory.
     *
     * @return An absolute path
     */
    public String getPath();

    /**
     * Get the version of the ZenPack.
     *
     * @return A string representing the ZenPack version.
     */
    public String getVersion();

    /**
     * Get the paths to any queue config files included in the ZenPack (*.qjs)
     * @return A list of absolute paths.
     */
    public List<String> getQueueConfigPaths();

    /**
     * Get a path relative to a ZenPack's source directory, e.g. ZenPacks.zenoss.MyPack/ZenPacks/zenoss/MyPack
     *
     * @param segments The segments of the path relative to the ZenPack
     * @return An absolute file path NOT guaranteed to exist
     */
    public String packPath(String... segments);

}
