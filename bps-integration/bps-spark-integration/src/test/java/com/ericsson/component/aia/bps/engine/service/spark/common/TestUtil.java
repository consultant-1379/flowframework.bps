package com.ericsson.component.aia.bps.engine.service.spark.common;

import static com.ericsson.component.aia.bps.core.common.Constants.SEPARATOR;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.commons.io.FileDeleteStrategy;
import org.apache.commons.io.IOUtils;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
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
    public static void createXml(final String input, final String outputFolder, final Map<String, Map<String, String>> attributeMap,
                                 final String file)
            throws IOException {

        createFolder(Paths.get(outputFolder));

        new File(outputFolder + SEPARATOR + file).createNewFile();

        /* first, get and initialize an engine */
        final VelocityEngine ve = new VelocityEngine();
        ve.setProperty(RuntimeConstants.RUNTIME_LOG_LOGSYSTEM_CLASS, "org.apache.velocity.runtime.log.Log4JLogChute");
        ve.setProperty("runtime.log.logsystem.log4j.logger", "velocity");
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
        if (tmpDir.toFile().exists()) {
            FileDeleteStrategy.FORCE.delete(tmpDir.toFile());
        }
        final File f = new File(tmpDir.toFile().getAbsolutePath());
        f.mkdir();
        return f;
    }

    /**
     * Join files.
     *
     * @param destination
     *            the destination
     * @param sources
     *            the sources
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static void joinFiles(final File destination, final File[] sources) throws IOException {
        OutputStream output = null;
        try {
            output = createAppendableStream(destination);
            for (final File source : sources) {
                appendFile(output, source);
            }
        } finally {
            IOUtils.closeQuietly(output);
        }
    }

    /**
     * Creates the appendable stream.
     *
     * @param destination
     *            the destination
     * @return the buffered output stream
     * @throws FileNotFoundException
     *             the file not found exception
     */
    private static BufferedOutputStream createAppendableStream(final File destination) throws FileNotFoundException {
        return new BufferedOutputStream(new FileOutputStream(destination, true));
    }

    /**
     * Append file.
     *
     * @param output
     *            the output
     * @param source
     *            the source
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private static void appendFile(final OutputStream output, final File source) throws IOException {
        InputStream input = null;
        try {
            input = new BufferedInputStream(new FileInputStream(source));
            IOUtils.copy(input, output);
        } finally {
            IOUtils.closeQuietly(input);
        }
    }
}
