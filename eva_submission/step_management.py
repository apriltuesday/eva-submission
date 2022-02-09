import functools
from enum import Enum


@functools.total_ordering
class SubmissionStep(Enum):
    # Order of definition indicates order in which steps are run.
    # String value should match filename of nextflow pipeline, if present.
    METADATA = 'metadata_load'
    ACCESSION = 'accession'
    LOAD = 'variant_load'
    # TODO can annotation and statistics be resumable?
    ANNOTATION = 'annotation'
    STATISTICS = 'statistics'

    def __str__(self):
        return str(self.value)

    def __gt__(self, other):
        if isinstance(other, SubmissionStep):
            return self._member_names_.index(self.name) > self._member_names_.index(other.name)
        return NotImplemented


def step(step_name):
    """Decorator for Eload functions that will store step name before running the function."""
    def step_decorator(func):

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            args[0].set_step(step_name)
            func(*args, **kwargs)
            # If no exception is raised, clear the step
            args[0].clear_step()

        return wrapper

    return step_decorator
