package com.splicemachine.concurrent.traffic;

/**
 * @author Scott Fines
 *         Date: 11/14/14
 */
public interface TrafficStats {

    /**
     * @return the total number of permits requested since the controller was created
     */
    long totalPermitsRequested();

    /**
     * @return the total number of permits granted since the controller was created
     */
    long totalPermitsGranted();

    /**
     * @return the overall permit throughput (permits granted/second) since the controller
     * was created.
     */
    double permitThroughput();

    /**
     * @return the exponentially-weighted 1-minute average throughput (permits granted/second)
     */
    double permitThroughput1M();

    /**
     * @return the exponentially-weighted 5-minute average throughput (permits granted/second)
     */
    double permitThroughput5M();

    /**
     * @return the exponentially-weighted 15-minute average throughput (permits granted/second)
     */
    double permitThroughput15M();

    /**
     * @return the total number of <em>permit requests</em> since the controller was created.
     * Note that this differs from {@link #totalPermitsRequested()} in that this essentially
     * measures the number of method calls, while {@link #totalPermitsRequested()} measures
     * the number of permits themselves.
     */
    long totalRequests();

    /**
     * @return the average time taken to permit and/or deny a permit request. Note that
     * this is the latency for a single <em>request</em>, not for a single <em>permit</em>. Thus,
     * one request of 1000 permits could show the same as one request for 1 permit. This helps
     * to measure time spent inside of a traffic control code block.
     */
    long avgRequestLatency();
}