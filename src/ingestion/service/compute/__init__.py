"""Compute engine — technical indicator calculations.

All functions take DataFrame/Series and return computed results.
They are pure computation, no I/O.
"""

from .indicators import compute_macd, compute_kdj, compute_rsi, signal_scan

__all__ = ["compute_macd", "compute_kdj", "compute_rsi", "signal_scan"]
