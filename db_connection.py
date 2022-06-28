from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

SECURE_CONNECT_BUNDLE = 'C:\\Users\\PapKate\\Desktop\\NoSQLProject\\muse.zip'
# This is the username
USERNAME = "lHCZZJMknZNBZglmRAsuKFdA";
# This is the password
PASSWORD = "4t_Yb_4ENDDrZg9H7KRaqe7B.oaOJOaYijUz4OzlGPDWzACtXmmvPZ.xeeocKCvA9Y.k6TtERKFNqyH0ziM..9Zb0b3dF7MhtSizsqWXp6,Fw,2j8K7uxUF7AJxtD812";
# This is the keyspace name
KEYSPACE = "musemovies"; 

class Connection:
    def __init__(self):
        self.secure_connect_bundle=SECURE_CONNECT_BUNDLE
        self.path_to_creds=''
        self.cluster = Cluster(
            cloud={
                'secure_connect_bundle': self.secure_connect_bundle
            },
            auth_provider=PlainTextAuthProvider(USERNAME, PASSWORD)
        )
        self.session = self.cluster.connect(KEYSPACE)
    def close(self):
        self.cluster.shutdown()
        self.session.shutdown()