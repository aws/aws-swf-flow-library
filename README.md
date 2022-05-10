# Releases of aws-swf-flow-library
## 1.12.x
The `1.12.x` release added following changes based on the `1.11.x` release:

* Supported scaling up the number of threads for task polling and task processing independently. This helps to avoid hitting the hard limit of concurrent pollers per task list, as you can increase the task processing threads without increasing the polling threads.
* Allowed the number of task processing threads to dynamically change between 0 and a configured value. This feature is disabled by default, and you can turn it on by `setAllowCoreThreadTimeOut(true)` for the workflow/activity workers.
* Removed the `setTaskExecutorThreadPoolSize()` and `getTaskExecutorThreadPoolSize()` methods from `GenericActivityWorker` and `ActivityWorker`. To configure the polling thread count and task execution thread count for your activity worker, please use `setExecuteThreadCount()` and `getExecuteThreadCount()` instead.
* Removed the `SpringGracefulShutdownActivityWorker` and `SpringGracefulShutdownWorkflowWorker` classes. The `SpringActivityWorker` and `SpringWorkflowWorker` have the graceful shutdown logic in themselves.
* Supported `SimpleWorkflowClientConfig` for tuning HTTP request timeouts of SWF APIs.
* Improved the retry policy during worker startup to avoid startup failure due to `RegisterActivityType` and `RegisterWorkflowType` throttling.
* Truncated stack trace to comply with the length limit of the `details` field in the `RespondActivityTaskFailed` API. Since the `details` field has a maximum length of 32768, SWF will return 400s if the original exception has a large stack trace. This change truncates the stack trace in case of detail length exceeded, by preserving the first stack trace element and logging the original stack trace.
* Upgraded the Jackson dependencies to 2.13.x. We've received customer reports that Jackson upgrade can trigger deserialization error (for certain data types) in their workflows. To make it smooth, you'll need to bump up your workflow type accordingly and let old workflow executions drain out. Essentially you should use the same deployment strategy (e.g., have two fleets running two versions of workflows) as what you do for a workflow logic change.
