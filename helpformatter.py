import discord.ext.commands as dec
import inspect


class DdmBotHelpFormatter(dec.HelpFormatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_ending_note(self):
        command_name = self.context.invoked_with
        return "Type {}{} command for more info on a command.\n" \
               "Commands marked with '*' are operator-only.".format(self.clean_prefix, command_name)

    def format(self):
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

        data = sorted(self.filter_command_list(), key=qualified_name)

        self._paginator.add_line('Commands:')
        self._add_subcommands_to_page(max_width, data)

        # add the ending note
        self._paginator.add_line()
        ending_note = self.get_ending_note()
        self._paginator.add_line(ending_note)
        return self._paginator.pages
