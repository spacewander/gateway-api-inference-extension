<!-- Dev guide -->


## Logging

### Change log verbosity
We use the `k8s.io/klog/v2` package to manage logging. 

We generally follow the [k8s instrumentation logging guidelines](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-instrumentation/logging.md), which states "the practical default level is V(2). Developers and QE environments may wish to run at V(3) or V(4)".

To configure logging verbosity, specify the `v` flag such as  `--v=2`.

### Add logs

The [k8s instrumentation logging guidelines](https://github.com/kubernetes/community/blob/master/contributors/devel/sig-instrumentation/logging.md) has the following definitions:

* `klog.V(0).InfoS` = `klog.InfoS` - Generally useful for this to **always** be visible to a cluster operator
* `klog.V(1).InfoS` - A reasonable default log level if you don't want verbosity.
* `klog.V(2).InfoS` - Useful steady state information about the service and important log messages that may correlate to significant changes in the system.  This is the recommended default log level for most systems.
* `klog.V(3).InfoS` - Extended information about changes
* `klog.V(4).InfoS` - Debug level verbosity
* `klog.V(5).InfoS` - Trace level verbosity

We choose to simplify to the following 3 common levels.
```
const(
    DEFAULT=2
    VERBOSE=3
    DEBUG=4
    TRACE=5
)
```

The guidelines are written in the context of a k8s controller. Our [ext-proc](../pkg/ext-proc/) does more things such as handling requests and scraping metrics, therefore we adapt the guidelines as follows:

1. The server startup process and configuration. 
   * `klog.InfoS`  Logging at the `V(0)` verbosity level is generally welcome here as this is only logged once at startup, and provides useful info for debugging.

2. Reconciler loops. The reconciler loops watch for CR changes such as the `InferenceModel` CR. And given changes in these CRs significantly affect the behavior of the extension, we recommend using v=1 verbosity level as default, and sparsely use higher verbosity levels.
   
   * `klog.V(DEFAULT).InfoS`
      * Default log level in the reconcilers.
      * Information about config (listening on X, watching Y)
      * Errors that repeat frequently that relate to conditions that can be corrected (e.g., inference model not initialized yet)
      * System state changing (adding/removing objects in the data store)
   * `V(VERBOSE)` and above: Use your best judgement. 

3. Inference request handling. These requests are expected to be much higher volume than the control flow in the reconcilers and therefore we should be mindful of log spamming. We recommend using v=2 to log important info about a request, such as the HTTP response code, and higher verbosity levels for less important info.

   * `klog.V(DEFAULT).InfoS`
      * Logging the status code of an HTTP request
      * Important decision making such as picking the target model, target pod
   * `klog.V(VERBOSE).InfoS`
      * Detailed request scheduling algorithm operations, such as running the filtering logic
   * `V(DEBUG)` and above: Use your best judgement. 

4. Metric scraping loops. These loops run at a very high frequency, and logs can be very spammy if not handled properly.
    * `klog.V(TRACE).InfoS`
      * Transient errors/warnings, such as failure to get response from a pod.
      * Important state changes, such as updating a metric.

5. Misc 
   1. Periodic (every 5s) debug loop which prints the current pods and metrics.
      * `klog.WarningS` If the metrics are not fresh enough, which indicates an error occurred during the metric scraping loop.
      * `klog.V(DEBUG).InfoS`
         *  This is very important to debug the request scheduling algorithm, and yet not spammy compared to the metric scraping loop logs.