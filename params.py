import yaml

class Publication:

    def __init__(self, rdict):
        self.name = rdict['name']
        self.host = rdict['host']
        self.port = rdict['port']
        self.user = rdict['user']
        self.password = rdict['password']
        self.database = DatabasePublication(rdict['database'])


class DatabasePublication:
    def __init__(self, rdict):
        self.name = rdict['name']
        self.tables = rdict['tables']
        self.owner = OwnerPublication(rdict['owner'])


class OwnerPublication:
    def __init__(self, rdict):
        self.owner = rdict['user']


class Subscription:

    def __init__(self, rdict):
        self.name = rdict['name']
        self.host = rdict['host']
        self.port = rdict['port']
        self.user = rdict['user']
        self.user = rdict['password']
        self.user = rdict['database']
        self.user = rdict['schema']

class Parameters:

    def __init__(self, file):

        with open(file, 'r') as f:
            params = yaml.safe_load(f)

        rdict = params['publication']
        self.publication = Publication(rdict)

        bdict = params['subscription']
        self.subscription = Subscription(bdict)
