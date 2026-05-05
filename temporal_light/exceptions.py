class WorkflowSuspended(Exception):
    """Raised internally to stop workflow execution cleanly.

    Not an error — raised when a workflow must pause (retry backoff, sleep,
    signal wait). The runner catches this and releases the workflow lock without
    marking the workflow as failed.
    """


class DivergenceError(Exception):
    """Raised when replay detects an incompatible code change.

    Occurs when the step name at a given step_index in the running code does
    not match the step name recorded in the scheduled event for that index.
    """
