# Discord Direct Music Bot #

The aim of this project is to create (yet another) music bot for Discrod with a focus on the following features:

* User-based playlists, "DJ"s take turns in a round-robin manner
* Song management and overplay protection
* Automatic playlist when there is nobody to play
* Option to connect to the direct music stream (better quality but still bounded to the "Discord UI")

Please note this project is in early stages and not everything works properly and / or as intended. It will be eventually released under the MIT License.

## How do I get set up? ##

### Installation ###

DdmBot has the following dependencies:

* **GNU/Linux OS**, used distribution is up to you, but code is not portable and Windows is not supported
* **python 3.5.1** or newer
* [**discord.py**](https://github.com/Rapptz/discord.py), python Discord API
* **aiohttp** (version used by *discord.py* will be just fine so you won't need to install this separately)
* **ffmpeg** *with libfdk_aac support* (you need to either compile it yourself ([homepage](https://ffmpeg.org/)) or try to find it in your distribution's repositories)
* [**peewee**](https://github.com/coleifer/peewee), ORM library used for the database support

For the python dependencies installation I recommend using PIP, as described on the linked project pages.

### Launching the bot ###

Before launching the bot, make sure to fill up the missing entries in the `config.ini` file. *Please avoid committing `config.ini` with filled up IDs and bot token into this repository.*

Bot can be launched as follows:
```
python3 ddmbot.py
```

### Increasing FIFO sizes ###

Bot uses named pipes, aka FIFOs (`man 7 pipe`) for IPC between *ffmpeg* and bot itself. The default size of the FIFO on a modern Linux system is 64kiB. This may not be sufficient and I suggest you to change the size to at least 1MiB. For this purpose, C utility **pipe_resize** was written and can be compiled from source.

To compile **pipe_resize** utility you need a set of "non-archaic" build utilities that will usually install along with a compiler, *gcc*. Most likely those tools are already installed in your system. If not, please follow the development guideline for your distribution. After you have installed *gcc*, run the following command:
```
gcc pipe_resize.c -o pipe_resize
```
This should create an executable file `pipe_resize`. *Please avoid committing built tool into this repository.* Use it as follows:
```
./pipe_resize <path_to_pipe> [new_size_in_bytes]
```
If `new_size_in_bytes` is not provided, current size is printed to standard output. Otherwise new size of the FIFO is printed. You can find the maximum size that can be set without root privileges in the `/proc/sys/fs/pipe-max-size` file.

Running bot with increased pipe sizes then involves creating the FIFOs in advance (see `config.ini` for their location) and resizing them with a **pipe_resize** utility in prior to launching the bot.

## Contribution guidelines ##

Contributions are welcome. For python development, I suggest using [**PyCharm**](http://www.jetbrains.com/pycharm/) IDE (Community Edition is good enough). Apart of being a good IDE, it will keep your coding style compatible with the rest of the project.

Potential contributors must agree with releasing their code under the MIT License that will be added with the public release.
