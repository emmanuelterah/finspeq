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


class PositionGapError(PortError):
    """A batch's unapplied offsets do not continue directly from the stored position."""
