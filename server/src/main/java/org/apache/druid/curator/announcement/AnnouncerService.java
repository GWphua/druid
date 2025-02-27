package org.apache.druid.curator.announcement;

public interface AnnouncerService
{
    /**
     * Starts the announcer service, initializing any resources required for operation.
     * <p>
     * Implementations of this method are expected to perform any necessary setup
     * so that the component is ready for use. For components managed by a lifecycle
     * manager, this method is typically invoked automatically during the startup phase.
     * </p>
     */
    void start();

    /**
     * Stops the announcer service, releasing any resources held and ceasing further operations.
     * <p>
     * Implementations of this method should clean up any resources allocated during
     * operation and ensure that the component can be safely shut down. When managed by a lifecycle
     * manager, this method is generally called during the shutdown phase of the application.
     * </p>
     */
    void stop();

    /**
     * Overload of {@link #announce(String, byte[], boolean)}, but removes parent node of path after announcement.
     */
    void announce(String path, byte[] bytes);

    /**
     * Announces the provided bytes at the given path.
     *
     * <p>
     * Announcement using {@link NodeAnnouncer} will create an ephemeral znode at the specified path, and listens for
     * changes on your znode. Your znode will exist until it is unannounced, or until {@link #stop()} is called.
     * </p>
     *
     * @param path                  The path to announce at
     * @param bytes                 The payload to announce
     * @param removeParentIfCreated remove parent of "path" if we had created that parent during announcement
     */
    void announce(String path, byte[] bytes, boolean removeParentIfCreated);

    /**
     * Unannounces an announcement created at path.  Note that if all announcements get removed, the Announcer
     * will continue to have ZK watches on paths because clearing them out is a source of ugly race conditions.
     * <p/>
     * If you need to completely clear all the state of what is being watched and announced, stop() the Announcer.
     *
     * @param path the path to unannounce
     */
    void unannounce(String path);

    /**
     * Updates the announcement at the specified path.
     *
     * @param path      The path of the announcement to update
     * @param bytes     The payload to update
     */
    void update(String path, byte[] bytes);
}
