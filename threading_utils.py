def propagate_exceptions(futures):
    # Calling result() on a future whose execution raised an exception will propagate the exception to the caller
    [future.result() for future in futures]
