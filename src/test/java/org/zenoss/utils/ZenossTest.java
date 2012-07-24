/*****************************************************************************
 * 
 * Copyright (C) Zenoss, Inc. 2011, all rights reserved.
 * 
 * This content is made available according to terms specified in
 * License.zenoss under the directory where your Zenoss product is installed.
 * 
 ****************************************************************************/


package org.zenoss.utils;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;
import static org.powermock.api.easymock.PowerMock.*;

/**
 * Unit test for {@link Zenoss}.
 */
@PrepareForTest(Zenoss.class)
@RunWith(PowerMockRunner.class)
public class ZenossTest {

    private static String origZenHome;
    private List<File> _tempdirs = new LinkedList<File>();

    private String getTempDirPath() {
        File tmpdir = Files.createTempDir();
        _tempdirs.add(tmpdir);
        return tmpdir.getAbsolutePath();
    }

    private void resetZenHome() throws Exception {
        // Reset the cached value
        Field field = Zenoss.class.getDeclaredField("_zenhome");
        field.setAccessible(true);
        field.set(null, null);
    }

    @Before
    public void setUp() {
        origZenHome = System.getProperty("ZENHOME");
    }

    @After
    public void tearDown() throws Exception {
        if (origZenHome == null) {
            System.getProperties().remove("ZENHOME");
        } else {
            System.setProperty("ZENHOME", origZenHome);
        }
        resetZenHome();
        for (File f : _tempdirs) {
            f.delete();
        }
    }

    @Test
    public void testZenHome() throws Exception {
        String tmpdir1 = getTempDirPath();
        String tmpdir2 = getTempDirPath();

        mockStaticPartial(System.class, "getenv");
        expect(System.getenv("ZENHOME")).andReturn(tmpdir1);
        replayAll();
        assertEquals(tmpdir1, Zenoss.getZenHome());
        verifyAll();

        resetZenHome();

        System.setProperty("ZENHOME", tmpdir2);
        assertEquals(tmpdir2, Zenoss.getZenHome());
    }

    @Test(expected = ZenossException.class)
    public void testInvalidZenHome() throws Exception {
        System.setProperty("ZENHOME", "/not/a/valid/zenhome/of/any/kind");
        String zenhome = Zenoss.getZenHome();
    }

    @Test
    public void testZenPath() throws Exception {
        String zenhome = getTempDirPath();
        System.setProperty("ZENHOME", zenhome);
        String path = Zenoss.zenPath("a", "b");
        assertEquals(zenhome + "/a/b", path);
    }

}
