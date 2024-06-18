"""
Authors:
1) Joel Marvin Tellis (jt4680)
2) Sahil Bakshi (sb8916)
"""
from RepCRec.enums.TransactionStatus import TransactionStatus

class Transaction:
    """
    Transaction is an entity that represents a running process
    as it was started via input file. This transaction can
    read and write over variables.

    Parameters:
        txn_id: Id of the transaction (1, 2, 3 etc)
        name: Name of the transaction (T1, T2, etc)
        timestamp : Time when this Transaction began

    Attributes:
        id: Id of the transaction (1, 2, 3 etc)
        name: Name of the transaction (T1, T2, etc)
        status : Running or Waiting(for reading data)
        start_time: Time at which the transaction started
        commit_time: Time at which the transaction committed
        sites_accessed : list of site_id's

    """

    def __init__(self, txn_id, name, timestamp):
        self.status = TransactionStatus.RUNNING
        self.id = txn_id
        self.name = name
        self.start_time = timestamp
        self.sites_accessed = []
        self.commit_time = None

    def get_name(self):
        """
        Get name of the transaction

        Returns:
            Name of the transaction
        """
        return self.name

    def get_id(self):
        """
        Get ID of the transaction

        Returns:
            id of the transaction
        """
        return self.id

    def get_status(self):
        """
        Get staus of the transaction

        Returns:
            Status of the transaction
        """
        return self.status

    def get_start_time(self):
        """
        Get start time of the transaction

        Returns:
            startt time of the transaction
        """
        return self.start_time

    def get_commit_time(self):
        """
        Get commit time of the transaction

        Returns:
            commit time of the transaction
        """
        return self.commit_time

    def set_commit_time(self, timestamp):
        """
        Set commit time of the transaction
        """
        self.commit_time = timestamp

    def get_sites_accessed(self):
        """
        Gets sites accessed by the transaction

        Returns:
            List of sites accessed
        """
        return self.sites_accessed

    def add_sites_accessed(self, site_id, operation, timestamp):
        """
        Add the input (site_id, operation, timestamp) tuple to the list of sites_accessed by the transaction
        """
        self.sites_accessed.append((site_id, operation, timestamp))
        return

    def set_status(self, status):
        """
        Set status of the transaction

        Args:
            status: TransactionStatus type to be set to this transaction's status
        Raises:
            ValueError if unknown transactionstatus type is passed
        """
        if status in TransactionStatus:
            self.status = status
        else:
            raise ValueError("TransactionStatus is not valid")
        return

