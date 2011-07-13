package org.zenoss.utils.impl;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.io.Files;
import org.zenoss.utils.ZenPack;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZenPackImpl implements ZenPack {

    private String _name;
    private String _rootPath;
    private String _version;

    private static final Pattern NAME_REGEXP = Pattern.compile("^NAME\\s*=\\s*[\"\'](.*)[\"\']");
    private static final Pattern VERSION_REGEXP = Pattern.compile("^VERSION\\s*=\\s*[\"\'](.*)[\"\']");

    public ZenPackImpl(String path) {
        _rootPath = path.replaceAll("/$", "");
        parseSetupPy();
    }

    /**
     * Get the name of the ZenPack. This is read from setup.py.
     *
     * @return The fully qualified dotted name of the ZenPack.
     */
    public String getName() {
        return _name;
    }

    /**
     * Return the path to the ZenPack root directory.
     *
     * @return
     */
    public String getPath() {
        return _rootPath;
    }

    @Override
    public String getVersion() {
        return _version;
    }

    @Override
    public String packPath(String... segments) {
        List<String> parts = new ArrayList<String>();
        parts.add(_rootPath);
        if (_name != null) {
            for (String s : Splitter.on(".").split(_name)) {
                parts.add(s);
            }
        }
        parts.addAll(Arrays.asList(segments));
        return Joiner.on(File.separator).join(parts);
    }

    /**
     * Parse the ZenPack's setup.py to find the name and version.
     */
    private void parseSetupPy() {
        String setuppy = new StringBuilder()
                .append(_rootPath)
                .append(File.separator)
                .append("setup.py").toString();
        try {
            List<String> lines = Files.readLines(new File(setuppy), Charset.defaultCharset());
            Matcher matcher;
            for (String line : lines) {
                if (_name == null && (matcher = NAME_REGEXP.matcher(line)).matches()) {
                    _name = matcher.group(1);
                } else if (_version == null && (matcher = VERSION_REGEXP.matcher(line)).matches()) {
                    _version = matcher.group(1);
                }
                if (_name != null && _version != null) {
                    break;
                }
            }
        } catch (IOException e) {
            // Not much we can do, we'll just set stuff to null
            // TODO: Log
        }
    }

    @Override
    public List<String> getQueueConfigPaths() {
        // TODO: Get the paths
        return Collections.emptyList();
    }

}
