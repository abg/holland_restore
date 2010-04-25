"""Line-based stream tokenization support"""
from holland_restore.tokenizer.base import Token, TokenizationError, Tokenizer
from holland_restore.tokenizer.rules import RULES
from holland_restore.tokenizer.util import read_until, yield_until, \
                                           scan_until_preserving, \
                                           yield_until_preserving
