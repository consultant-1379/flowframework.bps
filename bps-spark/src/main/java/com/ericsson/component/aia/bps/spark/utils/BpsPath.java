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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Names a file or directory in a {@link FileSystem}. Path strings use slash as the directory separator. A path string is absolute if it begins with a
 * slash.
 */
public class BpsPath implements Comparable<Object> {

    /** The Constant WINDOWS. */
    public static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");

    /** The Constant SEPARATOR_CHAR. */
    public static final char SEPARATOR_CHAR = '/';

    /** The directory separator, a slash. */
    public static final String SEPARATOR = "/";

    /** The Constant CUR_DIR. */
    public static final String CUR_DIR = ".";

    /**
     * Pre-compiled regular expressions to detect path formats.
     */
    private static final Pattern hasDriveLetterSpecifier = Pattern.compile("^/?[a-zA-Z]:");

    /** Logger. */
    private static final Logger LOG = LoggerFactory.getLogger(BpsPath.class);

    /** The uri. */
    private URI uri; // a hierarchical uri

    /**
     * Resolve a child path against a parent path.
     *
     * @param parent
     *            the parent
     * @param child
     *            the child
     */
    public BpsPath(final BpsPath parent, final BpsPath child) {
        LOG.trace("Entering the BpsPath method ");
        // Add a slash to parent's path so resolution is compatible with URI's
        URI parentUri = parent.uri;
        final String parentPath = parentUri.getPath();
        if (!("/".equals(parentPath) || parentPath.isEmpty())) {
            try {
                parentUri = new URI(parentUri.getScheme(), parentUri.getAuthority(), parentUri.getPath() + "/", null, parentUri.getFragment());
            } catch (final URISyntaxException e) {
                throw new IllegalArgumentException(e);
            }
        }
        final URI resolved = parentUri.resolve(child.uri);
        initialize(resolved.getScheme(), resolved.getAuthority(), resolved.getPath(), resolved.getFragment());
        LOG.trace("Existing the BpsPath method");
    }

    /**
     * Resolve a child path against a parent path.
     *
     * @param parent
     *            the parent
     * @param child
     *            the child
     */
    public BpsPath(final BpsPath parent, final String child) {
        this(parent, new BpsPath(child));
    }

    /**
     * Construct a path from a String. Path strings are URIs, but with unescaped elements and some additional normalization.
     *
     * @param pathString
     *            the path string
     * @throws IllegalArgumentException
     *             the illegal argument exception
     */
    public BpsPath(String pathString) throws IllegalArgumentException {
        LOG.trace("Entering the BpsPath method ");
        checkPathArg(pathString);

        // We can't use 'new URI(String)' directly, since it assumes things are
        // escaped, which we don't require of Paths.

        // add a slash in front of paths with Windows drive letters
        if (hasWindowsDrive(pathString) && pathString.charAt(0) != '/') {
            pathString = "/" + pathString;
        }

        // parse uri components
        String scheme = null;
        String authority = null;

        int start = 0;

        // parse uri scheme, if any
        final int colon = pathString.indexOf(':');
        final int slash = pathString.indexOf('/');
        if ((colon != -1) && ((slash == -1) || (colon < slash))) { // has a
            // scheme
            scheme = pathString.substring(0, colon);
            start = colon + 1;
        }

        // parse uri authority, if any
        if (pathString.startsWith("//", start) && (pathString.length() - start > 2)) { // has
            // authority
            final int nextSlash = pathString.indexOf('/', start + 2);
            final int authEnd = nextSlash > 0 ? nextSlash : pathString.length();
            authority = pathString.substring(start + 2, authEnd);
            start = authEnd;
        }

        // uri path is the rest of the string -- query & fragment not supported
        final String path = pathString.substring(start, pathString.length());

        initialize(scheme, authority, path, null);
        LOG.trace("Existing the BpsPath method");
    }

    /**
     * Resolve a child path against a parent path.
     *
     * @param parent
     *            the parent
     * @param child
     *            the child
     */
    public BpsPath(final String parent, final BpsPath child) {
        this(new BpsPath(parent), child);
    }

    /**
     * Resolve a child path against a parent path.
     *
     * @param parent
     *            the parent
     * @param child
     *            the child
     */
    public BpsPath(final String parent, final String child) {
        this(new BpsPath(parent), new BpsPath(child));
    }

    /**
     * Construct a Path from components.
     *
     * @param scheme
     *            the scheme
     * @param authority
     *            the authority
     * @param path
     *            the path
     */
    public BpsPath(final String scheme, final String authority, String path) {
        LOG.trace("Entering the BpsPath method ");
        checkPathArg(path);

        // add a slash in front of paths with Windows drive letters
        if (hasWindowsDrive(path) && path.charAt(0) != '/') {
            path = "/" + path;
        }

        // add "./" in front of Linux relative paths so that a path containing
        // a colon e.q. "a:b" will not be interpreted as scheme "a".
        if (!WINDOWS && path.charAt(0) != '/') {
            path = "./" + path;
        }

        initialize(scheme, authority, path, null);
        LOG.trace("Existing the BpsPath method");
    }

    /**
     * Construct a path from a URI.
     *
     * @param aUri
     *            the a uri
     */
    public BpsPath(final URI aUri) {
        uri = aUri.normalize();
    }

    /**
     * Gets the path without scheme and authority.
     *
     * @param path
     *            the path
     * @return the path without scheme and authority
     */
    public static BpsPath getPathWithoutSchemeAndAuthority(final BpsPath path) {
        LOG.trace("Entering the getPathWithoutSchemeAndAuthority method ");
        // This code depends on Path.toString() to remove the leading slash
        // before
        // the drive specification on Windows.
        final BpsPath newPath = path.isUriPathAbsolute() ? new BpsPath(null, null, path.toUri().getPath()) : path;
        LOG.trace("Existing the getPathWithoutSchemeAndAuthority method");
        return newPath;
    }

    /**
     * Checks for windows drive.
     *
     * @param path
     *            the path
     * @return true, if successful
     */
    private static boolean hasWindowsDrive(final String path) {
        return (WINDOWS && hasDriveLetterSpecifier.matcher(path).find());
    }

    /**
     * Determine whether a given path string represents an absolute path on Windows. e.g. "C:/a/b" is an absolute path. "C:a/b" is not.
     *
     * @param pathString
     *            Supplies the path string to evaluate.
     * @param slashed
     *            true if the given path is prefixed with "/".
     * @return true if the supplied path looks like an absolute path with a Windows drive-specifier.
     */
    public static boolean isWindowsAbsolutePath(final String pathString, final boolean slashed) {
        LOG.trace("Entering the isWindowsAbsolutePath method ");
        final int start = (slashed ? 1 : 0);

        return hasWindowsDrive(pathString) && pathString.length() >= (start + 3)
                && ((pathString.charAt(start + 2) == SEPARATOR_CHAR) || (pathString.charAt(start + 2) == '\\'));

    }

    /**
     * Merge 2 paths such that the second path is appended relative to the first. The returned path has the scheme and authority of the first path. On
     * Windows, the drive specification in the second path is discarded.
     *
     * @param path1
     *            Path first path
     * @param path2
     *            Path second path, to be appended relative to path1
     * @return Path merged path
     */
    public static BpsPath mergePaths(final BpsPath path1, final BpsPath path2) {
        LOG.trace("Entering the mergePaths method ");
        String path2Str = path2.toUri().getPath();
        if (hasWindowsDrive(path2Str)) {
            path2Str = path2Str.substring(path2Str.indexOf(':') + 1);
        }
        LOG.trace("Existing the mergePaths method");
        return new BpsPath(path1 + path2Str);
    }

    /**
     * Normalize a path string to use non-duplicated forward slashes as the path separator and remove any trailing path separators.
     *
     * @param scheme
     *            Supplies the URI scheme. Used to deduce whether we should replace backslashes or not.
     * @param path
     *            Supplies the scheme-specific part
     * @return Normalized path string.
     */
    private static String normalizePath(final String scheme, String path) {
        LOG.trace("Entering the normalizePath method "); // Remove double
        // forward slashes.
        path = StringUtils.replace(path, "//", "/");

        // Remove backslashes if this looks like a Windows path. Avoid
        // the substitution if it looks like a non-local URI.
        if (WINDOWS && (hasWindowsDrive(path) || (scheme == null) || (scheme.isEmpty()) || ("file".equals(scheme)))) {
            path = StringUtils.replace(path, "\\", "/");
        }

        // trim trailing slash from non-root path (ignoring windows drive)
        final int minLength = hasWindowsDrive(path) ? 4 : 1;
        if (path.length() > minLength && path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        LOG.trace("Existing the normalizePath method");
        return path;
    }

    /**
     * Check not relative.
     */
    void checkNotRelative() {
        LOG.trace("Entering the checkNotRelative method ");
        if (!isAbsolute() && toUri().getScheme() == null) {
            throw new HadoopIllegalArgumentException("Path is relative");
        }
        LOG.trace("Existing the checkNotRelative method");
    }

    /**
     * Pathnames with scheme and relative path are illegal.
     */
    void checkNotSchemeWithRelative() {
        LOG.trace("Entering the checkNotSchemeWithRelative method ");
        if (toUri().isAbsolute() && !isUriPathAbsolute()) {
            throw new HadoopIllegalArgumentException("Unsupported name: has scheme but relative path-part");
        }
        LOG.trace("Existing the checkNotSchemeWithRelative method");
    }

    /**
     * Check path arg.
     *
     * @param path
     *            the path
     * @throws IllegalArgumentException
     *             the illegal argument exception
     */
    private void checkPathArg(final String path) throws IllegalArgumentException {
        LOG.trace("Entering the checkPathArg method "); // disallow construction
        // of a Path
        // from an empty string
        if (path == null) {
            throw new IllegalArgumentException("Can not create a Path from a null string");
        }
        if (path.length() == 0) {
            throw new IllegalArgumentException("Can not create a Path from an empty string");
        }
        LOG.trace("Existing the checkPathArg method");
    }

    @Override
    public int compareTo(final Object object) {
        final BpsPath that = (BpsPath) object;
        return this.uri.compareTo(that.uri);
    }

    /**
     * Return the number of elements in this path.
     *
     * @return the int
     */
    public int depth() {
        final String path = uri.getPath();
        int depth = 0;
        int slash = path.length() == 1 && path.charAt(0) == '/' ? -1 : 0;
        while (slash != -1) {
            depth++;
            slash = path.indexOf(SEPARATOR, slash + 1);
        }
        return depth;
    }

    @Override
    public boolean equals(final Object object) {
        if (!(object instanceof BpsPath)) {
            return false;
        }
        final BpsPath that = (BpsPath) object;
        return this.uri.equals(that.uri);
    }

    /**
     * Return the FileSystem that owns this Path.
     *
     * @param conf
     *            the conf
     * @return the file system
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public FileSystem getFileSystem(final Configuration conf) throws IOException {
        return FileSystem.get(this.toUri(), conf);
    }

    /**
     * Returns the final component of this path.
     *
     * @return the name
     */
    public String getName() {
        final String path = uri.getPath();
        final int slash = path.lastIndexOf(SEPARATOR);
        return path.substring(slash + 1);
    }

    /**
     * Returns the parent of a path or null if at root.
     *
     * @return the parent
     */
    public BpsPath getParent() {
        LOG.trace("Entering the getParent method ");
        final String path = uri.getPath();
        final int lastSlash = path.lastIndexOf('/');
        final int start = hasWindowsDrive(path) ? 3 : 0;
        if ((path.length() == start) || // empty path
                (lastSlash == start && path.length() == start + 1)) { // at root
            return null;
        }
        String parent;
        parent = getParent(path, lastSlash);
        LOG.trace("Existing the getParent method");
        return new BpsPath(uri.getScheme(), uri.getAuthority(), parent);
    }

    /**
     * @param path
     * @param lastSlash
     * @return
     */
    private String getParent(final String path, final int lastSlash) {
        String parent;
        if (lastSlash == -1) {
            parent = CUR_DIR;
        } else {
            final int end = hasWindowsDrive(path) ? 3 : 0;
            parent = path.substring(0, lastSlash == end ? end + 1 : lastSlash);
        }
        return parent;
    }

    @Override
    public int hashCode() {
        return uri.hashCode();
    }

    /**
     * Initialize.
     *
     * @param scheme
     *            the scheme
     * @param authority
     *            the authority
     * @param path
     *            the path
     * @param fragment
     *            the fragment
     */
    private void initialize(final String scheme, final String authority, final String path, final String fragment) {
        LOG.trace("Entering the initialize method ");
        try {
            this.uri = new URI(scheme, authority, normalizePath(scheme, path), null, fragment).normalize();
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        LOG.trace("Existing the initialize method");
    }

    /**
     * True if the path component of this URI is absolute.
     *
     * @return true, if is absolute
     */
    /**
     * There is some ambiguity here. An absolute path is a slash relative name without a scheme or an authority. So either this method was incorrectly
     * named or its implementation is incorrect. This method returns true even if there is a scheme and authority.
     *
     * @return true if absolute else false
     */
    public boolean isAbsolute() {
        return isUriPathAbsolute();
    }

    /**
     * Is an absolute path (ie a slash relative path part) AND a scheme is null AND authority is null.
     *
     * @return true, if is absolute and scheme authority null
     */
    public boolean isAbsoluteAndSchemeAuthorityNull() {
        return (isUriPathAbsolute() && uri.getScheme() == null && uri.getAuthority() == null);
    }

    /**
     * Checks if is root.
     *
     * @return true if and only if this path represents the root of a file system
     */
    public boolean isRoot() {
        return getParent() == null;
    }

    /**
     * True if the path component (i.e. directory) of this URI is absolute.
     *
     * @return true, if is uri path absolute
     */
    public boolean isUriPathAbsolute() {
        final int start = hasWindowsDrive(uri.getPath()) ? 3 : 0;
        return uri.getPath().startsWith(SEPARATOR, start);
    }

    /**
     * Returns a qualified path object.
     *
     * @param defaultUri
     *            the default uri
     * @param workingDir
     *            the working dir
     * @return the bps path
     */
    public BpsPath makeQualified(final URI defaultUri, final BpsPath workingDir) {
        LOG.trace("Entering the makeQualified method ");
        BpsPath path = this;
        if (!isAbsolute()) {
            path = new BpsPath(workingDir, this);
        }

        final URI pathUri = path.toUri();

        String scheme = pathUri.getScheme();
        String authority = pathUri.getAuthority();
        final String fragment = pathUri.getFragment();

        if (scheme != null && (authority != null || defaultUri.getAuthority() == null)) {
            return path;
        }

        if (scheme == null) {
            scheme = defaultUri.getScheme();
        }

        if (authority == null) {
            authority = defaultUri.getAuthority();
            if (authority == null) {
                authority = "";
            }
        }

        URI newUri = null;
        try {
            newUri = new URI(scheme, authority, normalizePath(scheme, pathUri.getPath()), null, fragment);
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
        LOG.trace("Existing the makeQualified method");
        return new BpsPath(newUri);
    }

    /**
     * Adds a suffix to the final name in the path.
     *
     * @param suffix
     *            the suffix
     * @return the bps path
     */
    public BpsPath suffix(final String suffix) {
        return new BpsPath(getParent(), getName() + suffix);
    }

    @Override
    public String toString() {
        LOG.trace("Entering the toString method "); // we can't use
        // uri.toString(), which
        // escapes everything, because we
        // want
        // illegal characters unescaped in the string, for glob processing, etc.
        final StringBuilder buffer = new StringBuilder();
        if (uri.getScheme() != null) {
            buffer.append(uri.getScheme());
            buffer.append(":");
        }
        if (uri.getAuthority() != null) {
            buffer.append("//");
            buffer.append(uri.getAuthority());
        }
        if (uri.getPath() != null) {
            String path = uri.getPath();
            if (path.indexOf('/') == 0 && hasWindowsDrive(path) && // has
                    // windows
                    // drive
                    uri.getScheme() == null && // but no scheme
                    uri.getAuthority() == null) {
                path = path.substring(1); // remove slash before drive
            }
            buffer.append(path);
        }
        if (uri.getFragment() != null) {
            buffer.append("#");
            buffer.append(uri.getFragment());
        }
        LOG.trace("Existing the toString method");
        return buffer.toString();
    }

    /**
     * Convert this to a URI.
     *
     * @return the uri
     */
    public URI toUri() {
        return uri;
    }

}
