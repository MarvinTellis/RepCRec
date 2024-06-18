"""
Authors:
1) Joel Marvin Tellis (jt4680)
2) Sahil Bakshi (sb8916)
"""
import logging
from collections import defaultdict
from RepCRec.Variable import Variable

log = logging.getLogger(__name__)

class DataManager:
    """
    DataManager is local to every site and manages the data on sites

    Parameters:
        site_id: site_id of the site on which current data manager is

    Attributes:
        committed_variables ( Dict ) : KEY is variable index and VALUE is the Variable Object
        local_copies_per_txn ( Dict ) : Stores for each txn, another Dict with KEY as variable index and VALUE as written value
    """
    def __init__(self, site_id):
        def get_default_dict():
            return defaultdict(lambda: 0)

        self.site_id = site_id
        self.committed_variables = {}
        self.local_copies_per_txn = defaultdict(get_default_dict)

        for i in range(1, 21):
            if i % 2 == 0 or (1 + i % 10) == site_id:
                variable = Variable(i, 'x' + str(i), 10 * i, self.site_id)
                self.committed_variables[i] = variable

    def update_local_copy(self, transaction_id, variable_id, value):
        """
        Update the value written by the transaction to its local copy.

        Paramters:
            transaction_id : ID of transaction
            variable_id : ID of Variable
            value : New Value
        """
        self.local_copies_per_txn[transaction_id][variable_id] = value

    def get_committed_variables(self):
        """
        Returns the committed variables dictionary
        """
        return self.committed_variables

    def get_committed_variable_value(self, variable_id):
        """
        Returns the committed value of the specified variable id

        Paramters:
            variable_id : ID of Variable
        """
        return self.committed_variables[variable_id].get_value()

    def get_committed_variable_time(self, variable_id):
        """
        Returns the timestamp when the specified variable id was last committed

        Paramters:
            variable_id : ID of Variable
        """
        return self.committed_variables[variable_id].most_recent_snapshot_time()

    def commit_txn(self, transaction_id, timestamp):
        """
        Commit a transaction if it is valid (did not abort earlier due to error conditions)
        And it has no conflicts in the serialization graph.

        Paramters:
            transaction_id : ID of transaction
            timestamp : timestamp when the transaction has to be committed
        """
        # get the dict of variables for that txn
        local_copies = self.local_copies_per_txn[transaction_id]

        for variable_index in local_copies.keys():
            curr_varr = self.committed_variables[variable_index]
            curr_varr.set_value(local_copies[variable_index])
            curr_varr.update_snapshot(timestamp, local_copies[variable_index])

        self.local_copies_per_txn[transaction_id] = {}

    def find_most_recent_snapshot(self, timestamp, variable_id, txn_id):
        """
        Get the most recent snapshot of a variable that was committed before a transaction T begins

        Paramters:
            txn_id : ID of transaction
            variable_id : ID of Variable
            timestamp : start time of Transaction T

        Returns:
            The value that was committed before the Transaction T began
        """
        # Check if T first updated the value on this site. If yes, then that should be returned
        if variable_id in self.local_copies_per_txn[txn_id].keys() :
            # T updated the varaible on this site. It should read the updated value
            log.info("Returning local copy value as T%s can see its own changes", str(txn_id))
            return self.local_copies_per_txn[txn_id][variable_id]

        # T did not update this variable, so it should read the commited value before T began
        return self.committed_variables[variable_id].find_snapshot_before_time(timestamp)

    def get_committed_variable_before_time(self, timestamp, variable_id):
        """
        Get the time of the most recent snapshot of a variable that was committed before a transaction T begins

        Paramters:
            variable_id : ID of Variable
            timestamp : start time of Transaction T

        Returns:
            The time when the variable was last committed before the Transaction T began
        """
        return self.committed_variables[variable_id].find_time_of_snapshot_before(timestamp)

    def get_local_variables(self, transaction_id):
        """
        Returns the local copies of variables that were written by some Transactions.
        """
        return self.local_copies_per_txn[transaction_id]

    def check_commit_btw_time_range(self, timestamp1, timestamp2, variable_id):
        """
        Check if a commit happened between the timestamp1 and timestamp2 on variable_id

        Paramters:
            timestamp1 : Time instant 1
            timestamp2 : Time instant 2
            variable_id : ID of Variable

        Returns:
            True : if Commit happened
            False : Otherwise
        """

        variable_snapshots = self.committed_variables[variable_id].get_snapshots_list()

        for time, committed_val in variable_snapshots :
            if time > timestamp1 and time < timestamp2 :
                return True

        return False
