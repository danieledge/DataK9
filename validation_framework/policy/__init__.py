"""
Validation Policy Module

Provides policy enforcement to ensure developers include required validations.
Supports built-in policies (minimal, standard, strict) and custom policy definitions.
"""

from validation_framework.policy.schema import (
    ValidationPolicy,
    PolicyViolation,
    EnforcementMode,
)
from validation_framework.policy.analyzer import PolicyAnalyzer
from validation_framework.policy.builtin_policies import POLICIES, get_policy
from validation_framework.policy.defaults import get_default_check_config

__all__ = [
    'ValidationPolicy',
    'PolicyViolation',
    'EnforcementMode',
    'PolicyAnalyzer',
    'POLICIES',
    'get_policy',
    'get_default_check_config',
]
