from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

cloud_config= {
        'secure_connect_bundle': 'C:\\Users\\PapKate\\Desktop\\NoSQLProject\\muse.zip'
}
auth_provider = PlainTextAuthProvider('lHCZZJMknZNBZglmRAsuKFdA', '4t_Yb_4ENDDrZg9H7KRaqe7B.oaOJOaYijUz4OzlGPDWzACtXmmvPZ.xeeocKCvA9Y.k6TtERKFNqyH0ziM..9Zb0b3dF7MhtSizsqWXp6,Fw,2j8K7uxUF7AJxtD812')
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

row = session.execute("select release_version from system.local").one()
if row:
    print(row[0])
else:
    print("An error occurred.")