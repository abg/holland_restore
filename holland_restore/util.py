"""General utilities for holland_restore"""

import re
import fnmatch

__all__ = [
    'Filter',
]

class FilteredItem(Exception):
    """Raised when text is filtered"""

class Filter(object):
    """General inclusion/exclusion filter"""

    def __init__(self):
        self._include = []
        self._exclude = []
    
    def include(self, patterns):
        """Filter names not included in the list of glob patterns

        :param patterns: list of globs
        """
        self._include = [fnmatch.translate(p) for p in patterns]

    def exclude(self, patterns):
        """Filter names matched by any glob in the patterns list

        :param pattern: list of globs
        """
        self._exclude = [fnmatch.translate(p) for p in patterns]
    
    def __call__(self, text):
        for pat in self._include:
            if re.match(pat, text):
                break
        else:
            if self._include:
                raise FilteredItem(text, 'No matching include pattern')
        for pat in self._exclude:
            if re.match(pat, text):
                raise FilteredItem(text, pat)
