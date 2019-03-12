from params import Parameters
from pub.publication import Publication
# import sub.subscription
# from test.foo import Foo
#from import_file import import_file

pm = Parameters('pg-foo.yaml')

print(pm.publication.database.name)

# syncPub = SyncPublication(pm.publication)
# syncPub.exec()
publication = Publication()
publication.exec(pm.publication)

