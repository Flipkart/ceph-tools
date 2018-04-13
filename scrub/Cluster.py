from CredentialManager import CredentialManager

# To-do: Use Flyweight design pattern as we only need limited set of Cluster objects
class Cluster:

    def __init__(self, cluster_name):
        credentials = self.__get_credentials(cluster_name)

        self.name = cluster_name
        self.admin_access_key = credentials["access_key"]
        self.admin_secret_key = credentials["secret_key"]
        self.host = credentials["host"]
        self.admin_host = credentials["admin_host"]

    def __get_credentials(self, cluster_name):
        return CredentialManager().get_credentials(cluster_name)
