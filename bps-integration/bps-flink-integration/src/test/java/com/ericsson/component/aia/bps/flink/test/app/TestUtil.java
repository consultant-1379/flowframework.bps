package com.ericsson.component.aia.bps.flink.test.app;

import static com.ericsson.component.aia.bps.core.common.Constants.SEPARATOR;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.Velocity;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;

/**
 * TestUtil is a utility class for creating flow xmls.
 */
public class TestUtil {

    private TestUtil() {

    }

    /**
     * This method creates flow xml based on passed configurations parameters.
     *
     * @param input
     *            the input
     * @param outputFolder
     *            the output folder
     * @param attributeMap
     *            the attribute map
     * @param file
     *            the file
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static void createXml(final String input, final String outputFolder, final Map<String, Map<String, String>> attributeMap, final String file)
            throws IOException {

        createFolder(Paths.get(outputFolder));

        new File(outputFolder + SEPARATOR + file).createNewFile();

        Velocity.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS, "org.apache.velocity.runtime.log.Log4JLogChute");
        Velocity.setProperty("runtime.log.logsystem.log4j.logger", "velocity");

        /* first, get and initialize an engine */
        final VelocityEngine ve = new VelocityEngine();

        ve.init();
        /* next, get the Template */
        final Template template = ve.getTemplate(input);
        /* create a context and add data */
        final VelocityContext context = new VelocityContext();

        // loop a Map
        for (final Map.Entry<String, Map<String, String>> entry : attributeMap.entrySet()) {
            context.put(entry.getKey(), entry.getValue());
        }

        final BufferedWriter writer = new BufferedWriter(new FileWriter(outputFolder + SEPARATOR + file));

        if (template != null) {
            template.merge(context, writer);
        }

        /*
         * flush and cleanup
         */
        writer.flush();
        writer.close();
    }

    /**
     * Creates the folder.
     *
     * @param tmpDir
     *            the tmp dir
     * @return the file
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static File createFolder(final Path tmpDir) throws IOException {
        final File file = new File(tmpDir.toFile().getAbsolutePath());
        if (!file.exists()) {
            file.mkdir();
        }
        return file;
    }
}
