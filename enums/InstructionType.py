"""
Authors:
1) Joel Marvin Tellis (jt4680)
2) Sahil Bakshi (sb8916)
"""
from enum import Enum


class InstructionType(Enum):
    """
    Type of instruction present
    """
    READ = 0
    READ_ONLY = 1
    WRITE = 2
