"""Exceptions raised by application messages and by port implementations."""

from finspeq.domain.errors import FinspeqError


class InvalidMessageError(FinspeqError):
    """An application message violates its structural rules."""


class PortError(FinspeqError):
    """An outbound port could not complete an operation."""


class StoreUnavailableError(PortError):
    """A state store did not respond or refused the connection."""


class PublishFailedError(PortError):
    """The broker did not acknowledge a published event."""


class CodecError(PortError):
    """A log record cannot be encoded or decoded."""


class PositionGapError(PortError):
    """A batch's unapplied offsets do not continue directly from the stored position."""


class EvaluationError(FinspeqError):
    """Scores and labels cannot be evaluated: empty, unequal lengths, or a single class."""


class InvalidSourceDataError(PortError):
    """A historical record is out of canonical order or violates a domain rule."""
