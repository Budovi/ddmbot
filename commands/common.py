import logging

# set up the logger
log = logging.getLogger('ddmbot.commands')


# Decorator that adds privileged attribute to the command object
def privileged(command):
    command.privileged = True
    return command
