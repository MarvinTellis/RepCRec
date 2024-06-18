"""
Authors:
1) Joel Marvin Tellis (jt4680)
2) Sahil Bakshi (sb8916)
"""
from enum import Enum


class TransactionStatus(Enum):
    """
    Tells status of a particular transaction
    """
    RUNNING = 0
    WAITING = 1
    ABORTED = 2
    COMMITTED = 3
