import discord.ext.commands as dec
import inspect


class DdmBotHelpFormatter(dec.HelpFormatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_ending_note(self):
        return "Type \'{}{} command\' for more info on a command.\n" \
               "Commands marked with '*' are operator-only.".format(self.clean_prefix, self.context.command)

    @staticmethod
    def _partition(predicate, iterable):
        trues = []
        falses = []
        for item in iterable:
            if predicate(item):
                trues.append(item)
            else:
                falses.append(item)
        return trues, falses

    def format(self):
        """Handles the actual behaviour involved with formatting.

        To change the behaviour, this method should be overridden.

        Returns
        --------
        list
            A paginated output of the help command.
        """
        self._paginator = dec.Paginator()

        # we need a padding of ~80 or so

        description = self.command.description if not self.is_cog() else inspect.getdoc(self.command)

        if description:
            # <description> portion
            self._paginator.add_line(description, empty=True)

        if isinstance(self.command, dec.Command):
            # <signature portion>
            signature = self.get_command_signature()
            self._paginator.add_line(signature, empty=True)

            # <long doc> section
            if self.command.help:
                self._paginator.add_line(self.command.help, empty=True)

            # end it here if it's just a regular command
            if not self.has_subcommands():
                self._paginator.close_page()
                return self._paginator.pages

        max_width = self.max_name_size

        def qualified_name(tup):
            return tup[1].qualified_name

        def is_group(tup):
            return isinstance(tup[1], dec.GroupMixin)

        groups, commands = self._partition(is_group, sorted(self.filter_command_list(), key=qualified_name))

        add_sub_prefix = not self.is_bot() and self.has_subcommands()

        self._paginator.add_line('Subcommands:' if add_sub_prefix else 'Commands:')
        if commands:
            self._add_subcommands_to_page(max_width, commands)

        if groups:
            if commands:
                self._paginator.add_line()
            self._paginator.add_line('The following {0}commands are groups and are usually invoked with a subcommand. '
                                     'Type \'{1}{2}{3} {0}command\' to get a list of all available subcommands.'
                                     .format('sub' if add_sub_prefix else '', self.clean_prefix, self.context.command,
                                             '' if self.is_bot() or self.is_cog() else ' {}'.format(self.command)))
            self._add_subcommands_to_page(max_width, groups)

        # add the ending note
        self._paginator.add_line()
        ending_note = self.get_ending_note()
        self._paginator.add_line(ending_note)
        return self._paginator.pages
