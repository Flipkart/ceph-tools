from FileUtil import FileUtil
from Singleton import Singleton

CREDENTIALS_LOCATION = './credentials.json'



class CredentialManager(object):
    __metaclass__ = Singleton

    def __init__(self):
        self.__credentials = self._load_credentials()

    def _load_credentials(self):
        return FileUtil.load_json(CREDENTIALS_LOCATION)

    def get_credentials(self, cluster_name):
        if cluster_name in self.__credentials:
            return self.__credentials[cluster_name]
        return None

