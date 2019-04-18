import yaml

class Connection:
    def __init__(self, rdict):
        self.database = rdict['database']
        self.host = rdict['host']
        self.port = rdict['port']
        self.user = rdict['user']
        self.password = rdict['password']

class Publication:

    def __init__(self, rdict):
        self.connection = Connection(rdict['connection'])
        self.name = rdict['name']
        self.slot_name = rdict['slot_name']
        self.owner = OwnerPublication(rdict['owner'])
        self.tables = rdict['tables']

class OwnerPublication:
    def __init__(self, rdict):
        self.user = rdict['user']

class Subscription:

    def __init__(self, rdict):
        self.connection = Connection(rdict['connection'])
        self.name = rdict['name']
        self.owner = OwnerSubscription(rdict['owner'])

class OwnerSubscription:
    def __init__(self, rdict):
        self.owner = rdict['user']

class Parameters:

    def __init__(self, file):

        with open(file, 'r') as f:
            params = yaml.safe_load(f)

        rdict = params['publication']
        self.publication = Publication(rdict)

        bdict = params['subscription']
        self.subscription = Subscription(bdict)
