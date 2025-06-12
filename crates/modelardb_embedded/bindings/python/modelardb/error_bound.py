import math
from dataclasses import dataclass


@dataclass
class AbsoluteErrorBound:
    """A per value absolute error bound.

    param value: A positive finite value that specifies the error bound.
    :type value: float
    :raises ValueError: If `value` is not a positive normal value.
    """

    def __init__(self, value: float):
        # Error checking is required as the C-API depends on the error bounds being valid.
        if not math.isfinite(value) or value < 0:
            raise ValueError("An absolute error bound must be a positive finite value.")
        self.value = value


@dataclass
class RelativeErrorBound:
    """A per value relative error bound.

    param value: A value between 0 and 100 that specifies the error bound.
    :type value: float
    :raises ValueError: If `value` is not between 0 an 100.
    """

    def __init__(self, value: float):
        # Error checking is required as the C-API depends on the error bounds being valid.
        if not 0 <= value <= 100:
            raise ValueError(
                "A relative error bound must be a value from 0.0% to 100.0%."
            )
        self.value = value
