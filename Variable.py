"""
Authors:
1) Joel Marvin Tellis (jt4680)
2) Sahil Bakshi (sb8916)
"""
class Variable:
    """
    Variable class represents the data of our sites which can be read or written by transactions

    Parameters:
        index: index of variable
        name: Name of the variable
        value: Initial value of the variable
        current_site_id: Index of the site on which the variable is present

    Attributes:
        snapshots: List representing snapshot history containing (time, value) tuples where
            time is timestamp when a variable was committed by some transaction
            value is the new value that was committed.
    """

    def __init__(self, index, name, value, current_site_id):
        self.index = index
        self.name = name
        self.current_site_id = current_site_id
        self.value = value
        self.snapshots = []
        self.snapshots.append((0, value))

    def get_sites(self, site_id):
        """
        Class method which returns the sites on which the variable is present given an id

        Args:
            site_id: id of the variable for which list of sites is requested
        Returns:
            List of sites on which variable with index=id is present
        """
        if type(site_id) == str:
            site_id = int(site_id[1:])

        if site_id % 2 == 0:
            return 'all'
        else:
            return (site_id % 10) + 1

    def get_current_site(self):
        """
        Getter for current site

        Returns:
            Current site index of the variable
        """
        return self.current_site_id

    def get_value(self):
        """
        Getter for value

        Returns:
            Current value of the variable
        """
        return self.value

    def set_value(self, value):
        """
        Setter of variable value

        Args:
            value: Value to which the value of variable is to be set
        """
        self.value = value

    def update_snapshot(self, timestamp, new_value):
        """
        Update the snapshot
        """
        self.snapshots.append((timestamp, new_value))

    def most_recent_snapshot_time(self):
        """
        Return the timestamp of the most recent snapshot of the variable
        """
        if len(self.snapshots) > 0 :
            return self.snapshots[-1][0]

        return float('-inf')

    def find_snapshot_before_time(self, timestamp):
        """
        Return the most recent snapshot of the variable before the specified timestamp
        """
        for i in range(len(self.snapshots)-1, -1, -1):
            entry = self.snapshots[i]
            if entry[0] < timestamp:
                return entry[1]

        return None

    def find_time_of_snapshot_before(self, timestamp):
        """
        Return the time of most recent snapshot of the variable before the specified timestamp
        """
        for i in range(len(self.snapshots)-1, -1, -1):
            entry = self.snapshots[i]
            if entry[0] < timestamp:
                return entry[0]

        return None

    def get_snapshots_list(self):
        """
        Returns the list of snapshots of this variable
        """
        return self.snapshots
