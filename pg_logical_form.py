from params import Parameters
from pub.publication import Publication
from sub.subscription import Subscription
from lib.open_connection import OpenConnection
from colorama import Fore, Back, Style
import sys
import lib.execute_sql as ExecuteSql

def print_messages(color, commands):
    if len(commands) > 0:
        for cmd in commands:
           print(color + "~> " + cmd + "\n")

def print_header(color, commands, message):
    if len(commands) > 0:
        print(color + message + "\n")

pm = Parameters('pg-foo.yaml')

conn = OpenConnection(pm.publication, pm.subscription)

publication = Publication(pm, conn.get_connection_pub())
publication.run()

subscription = Subscription(pm, conn.get_connection_sub(), publication.has_any_changes())
subscription.run()

if publication.has_any_changes():
   print( Fore.CYAN + '\npg-logical-form will perform the following actions in PUBLICATION:\n')

print_header(Fore.LIGHTGREEN_EX, publication.commands_add, 'Add things of Publication:')
print_messages(Fore.GREEN, publication.commands_add)

print_header(Fore.LIGHTYELLOW_EX, publication.commands_change, 'Change things of Publication:')
print_messages(Fore.YELLOW, publication.commands_change)

print_header(Fore.RED, publication.commands_remove, 'REMOVE things of Publication:')
print_messages(Fore.RED, publication.commands_remove)

if subscription.has_any_changes():
   print(Fore.CYAN + 'pg-logical-form will perform the following actions in SUBSCRIPTION:\n')

print_header(Fore.GREEN, subscription.commands_add, 'Add things of Subscription:')
print_messages(Fore.GREEN, subscription.commands_add)

print_header(Fore.LIGHTYELLOW_EX, subscription.commands_change, 'Change things of Subscription:')
print_messages(Fore.YELLOW, subscription.commands_change)

print_header(Fore.RED, subscription.commands_remove, 'REMOVE things of Subscription:')
print_messages(Fore.RED, subscription.commands_remove)

print(Fore.RESET)

changes = publication.has_any_changes() or subscription.has_any_changes()

if changes:
   if sys.argv[1] == 'apply':
      text = input("Do you want to apply all (no, yes)? ")

      if text == 'yes':
         if publication.commands_add:
            print("Execute add commands of publication...")
            ExecuteSql.exec_scripts(conn.get_connection_pub(), publication.commands_add)

         if publication.commands_change:
            print("Execute change commands of publication...")
            ExecuteSql.exec_scripts(conn.get_connection_pub(), publication.commands_change)

         if subscription.commands_add:
            print("Execute add commands of subscription...")
            ExecuteSql.exec_scripts(conn.get_connection_sub(), subscription.commands_add)

         if subscription.commands_change:
            print("Execute change commands of subscription...")
            ExecuteSql.exec_scripts(conn.get_connection_sub(), subscription.commands_change)
else:
   print(Fore.LIGHTBLUE_EX + "Nothing to do")
   print(Fore.RESET)

