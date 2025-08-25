"""Core scheduling components for the taskaio library.

This module provides the basic classes for task scheduling and management.
It exposes the main scheduler interface and task planning components that
form the basis of the taskaio asynchronous task scheduling system.
"""

from taskaio._internal.scheduler import TaskPlan, TaskScheduler

__all__ = (
    "TaskPlan",
    "TaskScheduler",
)
__version__ = "0.0.1"
