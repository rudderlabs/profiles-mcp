"""
Unit tests for ProfilesTools version comparison functionality.

Tests the semantic version comparison used for pb CLI version checking.
"""

import pytest
from src.tools.profiles import ProfilesTools


class TestSemverComparison:
    """Test suite for semantic version comparison."""

    def test_compare_equal_versions(self):
        """Test that equal versions are considered sufficient."""
        assert ProfilesTools._compare_semver("0.24.0", "0.24.0") is True
        assert ProfilesTools._compare_semver("1.0.0", "1.0.0") is True
        assert ProfilesTools._compare_semver("0.23.3", "0.23.3") is True

    def test_compare_greater_major_version(self):
        """Test that greater major versions pass."""
        assert ProfilesTools._compare_semver("1.0.0", "0.24.0") is True
        assert ProfilesTools._compare_semver("2.0.0", "1.5.3") is True
        assert ProfilesTools._compare_semver("1.0.0", "0.0.1") is True

    def test_compare_greater_minor_version(self):
        """Test that greater minor versions pass."""
        assert ProfilesTools._compare_semver("0.25.0", "0.24.0") is True
        assert ProfilesTools._compare_semver("0.24.1", "0.24.0") is True
        assert ProfilesTools._compare_semver("1.5.0", "1.4.99") is True

    def test_compare_greater_patch_version(self):
        """Test that greater patch versions pass."""
        assert ProfilesTools._compare_semver("0.24.1", "0.24.0") is True
        assert ProfilesTools._compare_semver("0.23.4", "0.23.3") is True
        assert ProfilesTools._compare_semver("1.0.1", "1.0.0") is True

    def test_compare_lesser_major_version(self):
        """Test that lesser major versions fail."""
        assert ProfilesTools._compare_semver("0.24.0", "1.0.0") is False
        assert ProfilesTools._compare_semver("1.5.3", "2.0.0") is False
        assert ProfilesTools._compare_semver("0.0.1", "1.0.0") is False

    def test_compare_lesser_minor_version(self):
        """Test that lesser minor versions fail."""
        assert ProfilesTools._compare_semver("0.23.0", "0.24.0") is False
        assert ProfilesTools._compare_semver("0.24.0", "0.24.1") is False
        assert ProfilesTools._compare_semver("1.4.99", "1.5.0") is False

    def test_compare_lesser_patch_version(self):
        """Test that lesser patch versions fail."""
        assert ProfilesTools._compare_semver("0.24.0", "0.24.1") is False
        assert ProfilesTools._compare_semver("0.23.3", "0.23.4") is False
        assert ProfilesTools._compare_semver("1.0.0", "1.0.1") is False

    def test_compare_specific_pb_versions(self):
        """Test specific pb version scenarios mentioned in requirements."""
        # Current version 0.23.3 should fail against required 0.24.0
        assert ProfilesTools._compare_semver("0.23.3", "0.24.0") is False
        
        # Version 0.24.0 should pass against required 0.24.0
        assert ProfilesTools._compare_semver("0.24.0", "0.24.0") is True
        
        # Version 0.24.1 should pass against required 0.24.0
        assert ProfilesTools._compare_semver("0.24.1", "0.24.0") is True
        
        # Version 0.25.0 should pass against required 0.24.0
        assert ProfilesTools._compare_semver("0.25.0", "0.24.0") is True

    def test_compare_invalid_version_strings(self):
        """Test that invalid version strings return False."""
        assert ProfilesTools._compare_semver("invalid", "0.24.0") is False
        assert ProfilesTools._compare_semver("0.24.0", "invalid") is False
        assert ProfilesTools._compare_semver("0.24", "0.24.0") is False
        assert ProfilesTools._compare_semver("", "0.24.0") is False
        assert ProfilesTools._compare_semver("v0.24.0", "0.24.0") is False

    def test_compare_multi_digit_versions(self):
        """Test versions with multi-digit components."""
        assert ProfilesTools._compare_semver("0.100.0", "0.24.0") is True
        assert ProfilesTools._compare_semver("10.0.0", "9.99.99") is True
        assert ProfilesTools._compare_semver("1.10.5", "1.9.10") is True
        assert ProfilesTools._compare_semver("0.20.100", "0.20.99") is True

    def test_compare_edge_cases(self):
        """Test edge cases and boundary conditions."""
        # Zero versions
        assert ProfilesTools._compare_semver("0.0.0", "0.0.0") is True
        assert ProfilesTools._compare_semver("0.0.1", "0.0.0") is True
        assert ProfilesTools._compare_semver("0.0.0", "0.0.1") is False
        
        # Large version numbers
        assert ProfilesTools._compare_semver("999.999.999", "0.24.0") is True
        assert ProfilesTools._compare_semver("0.24.0", "999.999.999") is False

