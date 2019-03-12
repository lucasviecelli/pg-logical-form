from params import Parameters
from pub.publication import Publication
from sub.subscription import Subscription

# >>> from colorama import Fore, Back, Style
# >>> print(Fore.RED + 'some red text')
# some red text
# >>> print(Back.GREEN + 'and with a green background')


pm = Parameters('pg-foo.yaml')

# syncPub = SyncPublication(pm.publication)
# syncPub.exec()
publication = Publication(pm.publication)
publication.run()

print('ADD')
print(publication.commands_add)

print('CHANGE')
print(publication.commands_change)


subscription = Subscription(pm)
subscription.run()