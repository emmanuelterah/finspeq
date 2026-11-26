"""Exception hierarchy for Finspeq and for domain rule violations."""


class FinspeqError(Exception):
    """Base class for every exception raised by Finspeq code."""


class DomainError(FinspeqError):
    """A value violates a domain rule."""


class InvalidCardIdError(DomainError):
    """A card id is not 12 to 19 ASCII decimal digits."""


class InvalidTxIdError(DomainError):
    """A transaction id is not 32 lowercase hexadecimal characters."""


class InvalidMoneyError(DomainError):
    """An amount is not a positive number of cents."""


class InvalidEventTimeError(DomainError):
    """An event time is negative."""


class InvalidGeoPointError(DomainError):
    """A latitude or longitude is outside its valid range or is NaN."""


class InvalidCategoryError(DomainError):
    """A category name is not one of the known merchant categories."""


class InvalidMerchantError(DomainError):
    """A merchant name is empty or only whitespace."""


class InvalidWindowError(DomainError):
    """A card window is inconsistent with its card, bounds, or entry order."""


class InvalidFeatureVectorError(DomainError):
    """A feature vector contains a NaN or infinite value."""


class InvalidFeatureSetError(DomainError):
    """A feature set has no version, no features, or duplicate feature names."""


class SchemaMismatchError(DomainError):
    """A feature vector's schema differs from the schema its consumer expects."""


class InvalidScoreError(DomainError):
    """A fraud probability is outside [0, 1] or is NaN."""


class InvalidThresholdsError(DomainError):
    """Decision thresholds do not satisfy 0 <= review < block <= 1."""
