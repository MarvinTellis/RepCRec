"""
Authors:
1) Joel Marvin Tellis (jt4680)
2) Sahil Bakshi (sb8916)
"""
from enum import Enum

class SiteStatus(Enum):
    """
    Tells status of a particular site
    """
    UP = 0
    DOWN = 1
    RECOVERED = 2
