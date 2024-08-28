/*------------------------------------------------------------------------------
 *******************************************************************************
 * COPYRIGHT Ericsson 2016
 *
 * The copyright to the computer program(s) herein is the property of
 * Ericsson Inc. The programs may be used and/or copied only with written
 * permission from Ericsson Inc. or in accordance with the terms and
 * conditions stipulated in the agreement/contract under which the
 * program(s) have been supplied.
 *******************************************************************************
 *----------------------------------------------------------------------------*/
package com.ericsson.component.aia.bps.spark.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.StringUtils;

/**
 * FileUtils is a utility class for all file-processing util methods.
 */
public class FileUtils {

    /**
     * The Constant SYMLINK_NO_PRIVILEGE. The error code is defined in winutils to indicate insufficient privilege to create symbolic links. This
     * value need to keep in sync with the constant of the same name in: "src\winutils\common.h"
     */
    public static final int SYMLINK_NO_PRIVILEGE = 2;

    /** Logger. */
    private static final Log LOG = LogFactory.getLog(FileUtils.class);

    private FileUtils() {

    }

    /**
     * Delete a directory and all its contents. If we return false, the directory may be partially-deleted. (1) If dir is symlink to a file, the
     * symlink is deleted. The file pointed to by the symlink is not deleted. (2) If dir is symlink to a directory, symlink is deleted. The directory
     * pointed to by symlink is not deleted. (3) If dir is a normal file, it is deleted. (4) If dir is a normal directory, then dir and all its
     * contents recursively are deleted.
     *
     * @param dir
     *            the dir
     * @return true, if successful
     */
    public static boolean fullyDelete(final File dir) {
        return fullyDelete(dir, false);
    }

    /**
     * Delete a directory and all its contents. If we return false, the directory may be partially-deleted. (1) If dir is symlink to a file, the
     * symlink is deleted. The file pointed to by the symlink is not deleted. (2) If dir is symlink to a directory, symlink is deleted. The directory
     * pointed to by symlink is not deleted. (3) If dir is a normal file, it is deleted. (4) If dir is a normal directory, then dir and all its
     * contents recursively are deleted.
     *
     * @param dir
     *            the file or directory to be deleted
     * @param tryGrantPermissions
     *            true if permissions should be modified to delete a file.
     * @return true on success false on failure.
     */
    public static boolean fullyDelete(final File dir, final boolean tryGrantPermissions) {
        if (tryGrantPermissions) {
            // try to chmod +rwx the parent folder of the 'dir':
            final File parent = dir.getParentFile();
            grantPermissions(parent);
        }
        if (deleteImpl(dir, false)) {
            // dir is (a) normal file, (b) symlink to a file, (c) empty
            // directory or
            // (d) symlink to a directory
            return true;
        }
        // handle nonempty directory deletion
        if (!fullyDeleteContents(dir, tryGrantPermissions)) {
            return false;
        }
        return deleteImpl(dir, true);
    }

    /**
     * Returns the target of the given symlink. Returns the empty string if the given path does not refer to a symlink or there is an error accessing
     * the symlink.
     *
     * @param file
     *            File representing the symbolic link.
     * @return The target of the symbolic link, empty string on error or if not a symlink.
     */
    public static String readLink(final File file) {
        try {
            return Shell.execCommand(Shell.getReadlinkCommand(file.toString())).trim();
        } catch (final IOException x) {
            return "";
        }
    }

    /**
     * Platform independent implementation for {@link File#setExecutable(boolean)} File#setExecutable does not work as expected on Windows. Note:
     * revoking execute permission on folders does not have the same behavior on Windows as on Unix platforms. Creating, deleting or renaming a file
     * within that folder will still succeed on Windows.
     *
     * @param file
     *            input file
     * @param executable
     *            the executable
     * @return true on success, false otherwise
     */
    public static boolean setExecutable(final File file, final boolean executable) {
        if (Shell.WINDOWS) {
            try {
                final String permission = executable ? "u+x" : "u-x";
                chmod(file.getCanonicalPath(), permission, false);
                return true;
            } catch (final IOException ex) {
                return false;
            }
        } else {
            return file.setExecutable(executable);
        }
    }

    /**
     * Grant permissions.
     *
     * @param file
     *            the f
     */
    /*
     * Pure-Java implementation of "chmod +rwx f".
     */
    private static void grantPermissions(final File file) {
        setExecutable(file, true);
        setReadable(file, true);
        setWritable(file, true);
    }

    /**
     * Delete impl.
     *
     * @param file
     *            the f
     * @param doLog
     *            the do log
     * @return true, if successful
     */
    private static boolean deleteImpl(final File file, final boolean doLog) {
        if (file == null) {
            LOG.warn("null file argument.");
            return false;
        }
        final boolean wasDeleted = file.delete();
        if (wasDeleted) {
            return true;
        }
        final boolean doFileExists = file.exists();
        if (doLog && doFileExists) {
            LOG.warn("Failed to delete file or dir [" + file.getAbsolutePath() + "]: it still exists.");
        }
        return !doFileExists;
    }

    /**
     * Delete the contents of a directory, not the directory itself. If we return false, the directory may be partially-deleted. If dir is a symlink
     * to a directory, all the contents of the actual directory pointed to by dir will be deleted.
     *
     * @param dir
     *            the dir
     * @return true, if successful
     */
    public static boolean fullyDeleteContents(final File dir) {
        return fullyDeleteContents(dir, false);
    }

    /**
     * Delete the contents of a directory, not the directory itself. If we return false, the directory may be partially-deleted. If dir is a symlink
     * to a directory, all the contents of the actual directory pointed to by dir will be deleted.
     *
     * @param dir
     *            the dir
     * @param tryGrantPermissions
     *            if 'true', try grant +rwx permissions to this and all the underlying directories before trying to delete their contents.
     * @return true, if successful
     */
    public static boolean fullyDeleteContents(final File dir, final boolean tryGrantPermissions) {
        if (tryGrantPermissions) {
            grantPermissions(dir);
        }
        boolean deletionSucceeded = true;
        final File[] contents = dir.listFiles();
        if (contents != null) {
            for (int i = 0; i < contents.length; i++) {
                if (contents[i].isFile()) {
                    if (!deleteImpl(contents[i], true)) {
                        deletionSucceeded = false;
                        continue;
                    }
                } else {
                    boolean isSuccessful = false;
                    isSuccessful = deleteImpl(contents[i], false);
                    if (isSuccessful) {
                        continue;
                    }
                    if (!fullyDelete(contents[i], tryGrantPermissions)) {
                        deletionSucceeded = false;
                    }
                }
            }
        }
        return deletionSucceeded;
    }

    /**
     * Convert a os-native filename to a path that works for the shell.
     *
     * @param filename
     *            The filename to convert
     * @return The unix pathname
     * @throws IOException
     *             on windows, there can be problems with the subprocess
     */
    public static String makeShellPath(final String filename) throws IOException {
        return filename;
    }

    /**
     * Convert a os-native filename to a path that works for the shell.
     *
     * @param file
     *            The filename to convert
     * @return The unix pathname
     * @throws IOException
     *             on windows, there can be problems with the subprocess
     */
    public static String makeShellPath(final File file) throws IOException {
        return makeShellPath(file, false);
    }

    /**
     * Convert a os-native filename to a path that works for the shell.
     *
     * @param file
     *            The filename to convert
     * @param makeCanonicalPath
     *            Whether to make canonical path for the file passed
     * @return The unix pathname
     * @throws IOException
     *             on windows, there can be problems with the subprocess
     */
    public static String makeShellPath(final File file, final boolean makeCanonicalPath) throws IOException {
        if (makeCanonicalPath) {
            return makeShellPath(file.getCanonicalPath());
        } else {
            return makeShellPath(file.toString());
        }
    }

    /**
     * Takes an input dir and returns the du on that local directory. Very basic implementation.
     *
     * @param dir
     *            The input dir to get the disk space of this local dir
     * @return The total disk space of the input local directory
     */
    public static long getDU(final File dir) {
        long size = 0;
        if (!dir.exists()) {
            return 0;
        }
        if (!dir.isDirectory()) {
            return dir.length();
        } else {
            final File[] allFiles = dir.listFiles();
            if (allFiles != null) {
                for (int i = 0; i < allFiles.length; i++) {
                    boolean isSymLink;
                    try {
                        isSymLink = org.apache.commons.io.FileUtils.isSymlink(allFiles[i]);
                    } catch (final IOException ioe) {
                        isSymLink = true;
                    }
                    if (!isSymLink) {
                        size += getDU(allFiles[i]);
                    }
                }
            }
            return size;
        }
    }

    /**
     * Given a File input it will unzip the file in a the unzip directory passed as the second parameter.
     *
     * @param inFile
     *            The zip file as input
     * @param unzipDir
     *            The unzip directory where to unzip the zip file.
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static void unZip(final File inFile, final File unzipDir) throws IOException {
        final Enumeration<? extends ZipEntry> entries;
        final ZipFile zipFile = new ZipFile(inFile);

        try {
            entries = zipFile.entries();
            while (entries.hasMoreElements()) {
                final ZipEntry entry = entries.nextElement();
                if (!entry.isDirectory()) {
                    final InputStream inputStream = zipFile.getInputStream(entry);
                    try {
                        final File file = new File(unzipDir, entry.getName());
                        if (!file.getParentFile().mkdirs()) {
                            if (!file.getParentFile().isDirectory()) {
                                throw new IOException("Mkdirs failed to create " + file.getParentFile().toString());
                            }
                        }
                        final OutputStream out = new FileOutputStream(file);
                        try {
                            final byte[] buffer = new byte[8192];
                            int index;
                            while ((index = inputStream.read(buffer)) != -1) {
                                out.write(buffer, 0, index);
                            }
                        } finally {
                            out.close();
                        }
                    } finally {
                        inputStream.close();
                    }
                }
            }
        } finally {
            zipFile.close();
        }
    }

    /**
     * Given a Tar File as input it will untar the file in a the untar directory passed as the second parameter
     *
     * This utility will untar ".tar" files and ".tar.gz","tgz" files.
     *
     *
     * @param inFile
     *            The tar file as input.
     * @param untarDir
     *            The untar directory where to untar the tar file.
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static void unTar(final File inFile, final File untarDir) throws IOException {
        if (!untarDir.mkdirs()) {
            if (!untarDir.isDirectory()) {
                throw new IOException("Mkdirs failed to create " + untarDir);
            }
        }

        final boolean gzipped = inFile.toString().endsWith("gz");
        if (Shell.WINDOWS) {
            // Tar is not native to Windows. Use simple Java based
            // implementation for
            // tests and simple tar archives
            unTarUsingJava(inFile, untarDir, gzipped);
        } else {
            // spawn tar utility to untar archive for full fledged unix behavior
            // such
            // as resolving symlinks in tar archives
            unTarUsingTar(inFile, untarDir, gzipped);
        }
    }

    /**
     * Un tar using tar.
     *
     * @param inFile
     *            the in file
     * @param untarDir
     *            the untar dir
     * @param gzipped
     *            the gzipped
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private static void unTarUsingTar(final File inFile, final File untarDir, final boolean gzipped) throws IOException {
        final StringBuffer untarCommand = new StringBuffer();
        if (gzipped) {
            untarCommand.append(" gzip -dc '");
            untarCommand.append(makeShellPath(inFile));
            untarCommand.append("' | (");
        }
        untarCommand.append("cd '");
        untarCommand.append(makeShellPath(untarDir));
        untarCommand.append("' ; ");
        untarCommand.append("tar -xf ");

        if (gzipped) {
            untarCommand.append(" -)");
        } else {
            untarCommand.append(makeShellPath(inFile));
        }
        final String[] shellCmd = { "bash", "-c", untarCommand.toString() };
        final ShellCommandExecutor shexec = new ShellCommandExecutor(shellCmd);
        shexec.execute();
        final int exitcode = shexec.getExitCode();
        if (exitcode != 0) {
            throw new IOException("Error untarring file " + inFile + ". Tar process exited with exit code " + exitcode);
        }
    }

    /**
     * Un tar using java.
     *
     * @param inFile
     *            the in file
     * @param untarDir
     *            the untar dir
     * @param gzipped
     *            the gzipped
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private static void unTarUsingJava(final File inFile, final File untarDir, final boolean gzipped) throws IOException {
        InputStream inputStream = null;
        TarArchiveInputStream tis = null;
        try {
            if (gzipped) {
                inputStream = new BufferedInputStream(new GZIPInputStream(new FileInputStream(inFile)));
            } else {
                inputStream = new BufferedInputStream(new FileInputStream(inFile));
            }

            tis = new TarArchiveInputStream(inputStream);

            for (TarArchiveEntry entry = tis.getNextTarEntry(); entry != null;) {
                unpackEntries(tis, entry, untarDir);
                entry = tis.getNextTarEntry();
            }
        } finally {
            IOUtils.cleanup(LOG, tis, inputStream);
        }
    }

    /**
     * Unpack entries.
     *
     * @param tis
     *            the tis
     * @param entry
     *            the entry
     * @param outputDir
     *            the output dir
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private static void unpackEntries(final TarArchiveInputStream tis, final TarArchiveEntry entry, final File outputDir) throws IOException {
        if (entry.isDirectory()) {
            final File subDir = new File(outputDir, entry.getName());
            if (!subDir.mkdir() && !subDir.isDirectory()) {
                throw new IOException("Mkdirs failed to create tar internal dir " + outputDir);
            }

            for (final TarArchiveEntry e : entry.getDirectoryEntries()) {
                unpackEntries(tis, e, subDir);
            }

            return;
        }

        final File outputFile = new File(outputDir, entry.getName());
        if (!outputDir.exists()) {
            if (!outputDir.mkdirs()) {
                throw new IOException("Mkdirs failed to create tar internal dir " + outputDir);
            }
        }

        int count;
        final byte data[] = new byte[2048];
        final BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(outputFile));

        while ((count = tis.read(data)) != -1) {
            outputStream.write(data, 0, count);
        }

        outputStream.flush();
        outputStream.close();
    }

    /**
     * Change the permissions on a filename.
     *
     * @param filename
     *            the name of the file to change
     * @param perm
     *            the permission string
     * @return the exit code from the command
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws InterruptedException
     *             the interrupted exception
     */
    public static int chmod(final String filename, final String perm) throws IOException, InterruptedException {
        return chmod(filename, perm, false);
    }

    /**
     * Change the permissions on a file / directory, recursively, if needed.
     *
     * @param filename
     *            name of the file whose permissions are to change
     * @param perm
     *            permission string
     * @param recursive
     *            true, if permissions should be changed recursively
     * @return the exit code from the command.
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static int chmod(final String filename, final String perm, final boolean recursive) throws IOException {
        final String[] cmd = Shell.getSetPermissionCommand(perm, recursive);
        final String[] args = new String[cmd.length + 1];
        System.arraycopy(cmd, 0, args, 0, cmd.length);
        args[cmd.length] = new File(filename).getPath();
        final ShellCommandExecutor shExec = new ShellCommandExecutor(args);
        try {
            shExec.execute();
        } catch (final IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error while changing permission : " + filename + " Exception: " + StringUtils.stringifyException(e));
            }
        }
        return shExec.getExitCode();
    }

    /**
     * Set the ownership on a file / directory. User name and group name cannot both be null.
     *
     * @param file
     *            the file to change
     * @param username
     *            the new user owner name
     * @param groupname
     *            the new group owner name
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static void setOwner(final File file, final String username, final String groupname) throws IOException {
        if (username == null && groupname == null) {
            throw new IOException("username == null && groupname == null");
        }
        final String arg = (username == null ? "" : username) + (groupname == null ? "" : ":" + groupname);
        final String[] cmd = Shell.getSetOwnerCommand(arg);
        execCommand(file, cmd);
    }

    /**
     * Platform independent implementation for {@link File#setReadable(boolean)} File#setReadable does not work as expected on Windows.
     *
     * @param file
     *            input file
     * @param readable
     *            the readable
     * @return true on success, false otherwise
     */
    public static boolean setReadable(final File file, final boolean readable) {
        if (Shell.WINDOWS) {
            try {
                final String permission = readable ? "u+r" : "u-r";
                chmod(file.getCanonicalPath(), permission, false);
                return true;
            } catch (final IOException ex) {
                return false;
            }
        } else {
            return file.setReadable(readable);
        }
    }

    /**
     * Platform independent implementation for {@link File#setWritable(boolean)} File#setWritable does not work as expected on Windows.
     *
     * @param file
     *            input file
     * @param writable
     *            the writable
     * @return true on success, false otherwise
     */
    public static boolean setWritable(final File file, final boolean writable) {
        if (Shell.WINDOWS) {
            try {
                final String permission = writable ? "u+w" : "u-w";
                chmod(file.getCanonicalPath(), permission, false);
                return true;
            } catch (final IOException ex) {
                return false;
            }
        } else {
            return file.setWritable(writable);
        }
    }

    /**
     * Set permissions to the required value. Uses the java primitives instead of forking if group == other.
     *
     * @param file
     *            the file to change
     * @param permission
     *            the new permissions
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static void setPermission(final File file, final FsPermission permission) throws IOException {
        final FsAction user = permission.getUserAction();
        final FsAction group = permission.getGroupAction();
        final FsAction other = permission.getOtherAction();

        // use the native/fork if the group/other permissions are different
        // or if the native is available or on Windows
        if (group != other || NativeIO.isAvailable() || Shell.WINDOWS) {
            execSetPermission(file, permission);
            return;
        }

        boolean resultValue = true;

        // read perms
        resultValue = file.setReadable(group.implies(FsAction.READ), false);
        checkReturnValue(resultValue, file, permission);
        if (group.implies(FsAction.READ) != user.implies(FsAction.READ)) {
            resultValue = file.setReadable(user.implies(FsAction.READ), true);
            checkReturnValue(resultValue, file, permission);
        }

        // write perms
        resultValue = file.setWritable(group.implies(FsAction.WRITE), false);
        checkReturnValue(resultValue, file, permission);
        if (group.implies(FsAction.WRITE) != user.implies(FsAction.WRITE)) {
            resultValue = file.setWritable(user.implies(FsAction.WRITE), true);
            checkReturnValue(resultValue, file, permission);
        }

        // exec perms
        resultValue = file.setExecutable(group.implies(FsAction.EXECUTE), false);
        checkReturnValue(resultValue, file, permission);
        if (group.implies(FsAction.EXECUTE) != user.implies(FsAction.EXECUTE)) {
            resultValue = file.setExecutable(user.implies(FsAction.EXECUTE), true);
            checkReturnValue(resultValue, file, permission);
        }
    }

    /**
     * Check return value.
     *
     * @param returnValue
     *            the rv
     * @param path
     *            the p
     * @param permission
     *            the permission
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private static void checkReturnValue(final boolean returnValue, final File path, final FsPermission permission) throws IOException {
        if (!returnValue) {
            throw new IOException("Failed to set permissions of path: " + path + " to " + String.format("%04o", permission.toShort()));
        }
    }

    /**
     * Exec set permission.
     *
     * @param file
     *            the f
     * @param permission
     *            the permission
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private static void execSetPermission(final File file, final FsPermission permission) throws IOException {
        if (NativeIO.isAvailable()) {
            NativeIO.POSIX.chmod(file.getCanonicalPath(), permission.toShort());
        } else {
            execCommand(file, Shell.getSetPermissionCommand(String.format("%04o", permission.toShort()), false));
        }
    }

    /**
     * Exec command.
     *
     * @param file
     *            the f
     * @param cmd
     *            the cmd
     * @return the string
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    static String execCommand(final File file, final String... cmd) throws IOException {
        final String[] args = new String[cmd.length + 1];
        System.arraycopy(cmd, 0, args, 0, cmd.length);
        args[cmd.length] = file.getCanonicalPath();
        final String output = Shell.execCommand(args);
        return output;
    }

    /**
     * Create a tmp file for a base file.
     *
     * @param basefile
     *            the base file of the tmp
     * @param prefix
     *            file name prefix of tmp
     * @param isDeleteOnExit
     *            if true, the tmp will be deleted when the VM exits
     * @return a newly created tmp file
     * @see java.io.File#createTempFile(String, String, File)
     * @see java.io.File#deleteOnExit()
     * @exception IOException
     *                If a tmp file cannot created
     */
    public static final File createLocalTempFile(final File basefile, final String prefix, final boolean isDeleteOnExit) throws IOException {
        final File tmp = File.createTempFile(prefix + basefile.getName(), "", basefile.getParentFile());
        if (isDeleteOnExit) {
            tmp.deleteOnExit();
        }
        return tmp;
    }

    /**
     * Move the src file to the name specified by target.
     *
     * @param src
     *            the source file
     * @param target
     *            the target file
     * @exception IOException
     *                If this operation fails
     */
    public static void replaceFile(final File src, final File target) throws IOException {
        /*
         * renameTo() has two limitations on Windows platform. src.renameTo(target) fails if 1) If target already exists OR 2) If target is already
         * open for reading/writing.
         */
        if (!src.renameTo(target)) {
            int retries = 5;
            while (target.exists() && !target.delete() && retries-- >= 0) {
                try {
                    Thread.sleep(1000);
                } catch (final InterruptedException e) {
                    throw new IOException("replaceFile interrupted.");
                }
            }
            if (!src.renameTo(target)) {
                throw new IOException("Unable to rename " + src + " to " + target);
            }
        }
    }

    /**
     * A wrapper for {@link File#listFiles()}. This java.io API returns null when a dir is not a directory or for any I/O error. Instead of having
     * null check everywhere File#listFiles() is used, we will add utility API to get around this problem. For the majority of cases where we prefer
     * an IOException to be thrown.
     *
     * @param dir
     *            directory for which listing should be performed
     * @return list of files or empty list
     * @exception IOException
     *                for invalid directory or for a bad disk.
     */
    public static File[] listFiles(final File dir) throws IOException {
        final File[] files = dir.listFiles();
        if (files == null) {
            throw new IOException("Invalid directory or I/O error occurred for dir: " + dir.toString());
        }
        return files;
    }

    /**
     * A wrapper for {@link File#list()}. This java.io API returns null when a dir is not a directory or for any I/O error. Instead of having null
     * check everywhere File#list() is used, we will add utility API to get around this problem. For the majority of cases where we prefer an
     * IOException to be thrown.
     *
     * @param dir
     *            directory for which listing should be performed
     * @return list of file names or empty string list
     * @exception IOException
     *                for invalid directory or for a bad disk.
     */
    public static String[] list(final File dir) throws IOException {
        final String[] fileNames = dir.list();
        if (fileNames == null) {
            throw new IOException("Invalid directory or I/O error occurred for dir: " + dir.toString());
        }
        return fileNames;
    }

}
