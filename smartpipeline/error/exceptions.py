from typing import Optional


class Error(Exception):

    def set_stage(self, stage: str):
        self._pipeline_stage = stage

    def get_stage(self) -> Optional[str]:
        return getattr(self, '_pipeline_stage', None)

    def with_exception(self, exception: Exception):
        self._pipeline_exception = exception
        return self

    def get_exception(self) -> Exception:
        return getattr(self, '_pipeline_exception', Exception())

    def __str__(self) -> str:
        error = super(Error, self).__str__().strip()
        exception = str(self.get_exception()).strip()
        return '\n'.join((error, exception)).strip()


class CriticalError(Error):
    pass