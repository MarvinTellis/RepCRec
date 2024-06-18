"""
Authors:
1) Joel Marvin Tellis (jt4680)
2) Sahil Bakshi (sb8916)
"""
import logging

from RepCRec.DataManager import DataManager
from RepCRec.enums.SiteStatus import SiteStatus

log = logging.getLogger(__name__)


class Site:
    """
    This class represents the Site

    Paramters:
        index: Index of the current site

    Attributes:
        id : Site id
        status : Site status
        last_failure_time : Most recent time when the site failed
        data_manager : Data Manager for this site

    """

    def __init__(self, index):
        self.id = index
        self.status = SiteStatus.UP
        self.last_failure_time = None
        # Initialise DataManger
        self.data_manager = DataManager(self.id)

    def set_status(self, status):
        """
        Changes the status of the site

        Parameters:
            staus: TransactionStatus to be set for this site
        """

        if status in SiteStatus:
            self.status = status
        else:
            log.error("Invalid Site status")
        return

    def get_status(self):
        """
        Returns status of the site

        Returns:
            Status of the site
        """
        return self.status

    def get_id(self):
        """
        Returns index of the site

        Returns:
            index of the site
        """
        return self.id

    def get_last_failure_time(self):
        """
        Returns last failure time of the site

        Returns:
            last failure time of the site
        """
        return self.last_failure_time

    def set_last_failure_time(self, time):
        """
        Sets last failure time of the site

        Parameters:
            time: Set the last failure time of site
        """
        self.last_failure_time = time

    def get_data_manager(self):
        """
        Returns data manager of this site

        Returns:
            Data Manager of the site
        """
        return self.data_manager

    def fail(self):
        """
        Fails a site
        """
        self.set_status(SiteStatus.DOWN)

    def recover(self):
        """
        Recover the site
        """
        self.set_status(SiteStatus.RECOVERED)
