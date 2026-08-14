# Discord connector plugin for SA:MP and open.mp

| GitHub Actions | Total downloads | Latest release |
| :---: | :---: | :---: |
|  ![Build status](https://github.com/vosticdev/samp-discord-connector/actions/workflows/build.yml/badge.svg)|  [![All Releases](https://img.shields.io/github/downloads/vosticdev/samp-discord-connector/total.svg?maxAge=86400)](https://github.com/vosticdev/samp-discord-connector/releases)  |  [![latest release](https://img.shields.io/github/release/vosticdev/samp-discord-connector.svg?maxAge=86400)](https://github.com/vosticdev/samp-discord-connector/releases)  |
-------------------------------------------------
**This plugin allows you to control a Discord bot from within your PAWN script.**

Version 0.4 adds a serialized WebSocket write queue, Discord Gateway v10
session resume handling, heartbeat ACK monitoring, reconnect backoff with
jitter, presence-update throttling, and reliable REST rate-limit retries.

Credits
-------
This project was originally created by **Alex "Maddin4t0r" Martin**. This
repository is a maintained fork that builds on his original SA:MP Discord
connector, with later community contributions and compatibility/reliability
updates. The original MIT copyright notice remains in [LICENSE](LICENSE).

Download and deployment
-----------------------
Download the archive for your server operating system from the
[latest release](https://github.com/vosticdev/samp-discord-connector/releases/latest):

- Windows: `discord-connector-*-Windows.zip`
- Linux: `discord-connector-*-Linux.zip`

Stop the server before replacing an existing installation. Always deploy the
connector and its bundled libraries from the same release; do not mix files
from different versions.

### open.mp on Windows

1. Extract the Windows archive into a temporary directory.
2. Copy all three files from the archive's `plugins` directory into the
   server's `components` directory:

   ```text
   components/
   |-- discord-connector.dll
   |-- libcrypto-3.dll
   `-- libssl-3.dll
   ```

3. Copy `log-core2.dll` to the server root, next to `omp-server.exe`.
4. Copy `pawno/include/discord-connector.inc` from the archive into the include
   directory used to compile your gamemode. This is usually
   `qawno/include/discord-connector.inc` for open.mp, or
   `pawno/include/discord-connector.inc` when using Pawno.

The resulting layout should contain:

```text
server/
|-- omp-server.exe
|-- log-core2.dll
|-- components/
|   |-- discord-connector.dll
|   |-- libcrypto-3.dll
|   `-- libssl-3.dll
`-- qawno/include/discord-connector.inc
```

Do not add the connector to `pawn.legacy_plugins` or to a `plugins` line.
open.mp loads it from `components` automatically.

### open.mp on Linux

1. Extract the Linux archive into a temporary directory.
2. Copy `plugins/discord-connector.so` into the server's `components`
   directory.
3. Copy `log-core2.so` to the server root, next to the `omp-server` executable.
4. Copy `pawno/include/discord-connector.inc` from the archive into the include
   directory used to compile your gamemode.

The resulting layout should contain:

```text
server/
|-- omp-server
|-- log-core2.so
|-- components/
|   `-- discord-connector.so
`-- qawno/include/discord-connector.inc
```

If required by your host, make the server executable with
`chmod +x omp-server`. The connector itself only needs normal read permission.
Do not add it to `pawn.legacy_plugins`; open.mp loads it from `components`.

### Configure the Discord token on open.mp

Add the `discord` object inside the existing top-level object in `config.json`:

```json
{
  "discord": {
    "bot_token": "YOUR_DISCORD_BOT_TOKEN"
  }
}
```

If `config.json` already contains other settings, merge only the `discord`
object into it instead of replacing the whole file. As an alternative, set the
`DCC_BOT_TOKEN` environment variable in your hosting panel. Never commit or
share the bot token.

### SA:MP on Windows

1. Stop the server and extract the Windows archive directly into the SA:MP
   server root. Keep the archive layout unchanged: `discord-connector.dll`,
   `libcrypto-3.dll`, and `libssl-3.dll` remain together in `plugins`, while
   `log-core2.dll` remains in the server root.
2. Add `discord-connector` to the `plugins` line in `server.cfg`.
3. Add `discord_bot_token YOUR_DISCORD_BOT_TOKEN` to `server.cfg`, or set the
   `DCC_BOT_TOKEN` environment variable.

Example:

```text
plugins discord-connector
discord_bot_token YOUR_DISCORD_BOT_TOKEN
```

### SA:MP on Linux

1. Stop the server and extract the Linux archive directly into the SA:MP
   server root. Keep `discord-connector.so` in `plugins` and `log-core2.so` in
   the server root.
2. Add `discord-connector.so` to the `plugins` line in `server.cfg`.
3. Add `discord_bot_token YOUR_DISCORD_BOT_TOKEN` to `server.cfg`, or set the
   `DCC_BOT_TOKEN` environment variable.

Example:

```text
plugins discord-connector.so
discord_bot_token YOUR_DISCORD_BOT_TOKEN
```

After deployment, start the server and check the console/log for connector
startup errors. If the bot reports an intent error, enable the required
privileged gateway intents in the Discord Developer Portal.

I am getting a intent error, how do I fix it?
---------------
If you're getting an intent error, you need to go to the [discord developer dashboard](https://discord.com/developers/applications) and select your bot.
Then, you need to go to your bot settings and activate your intents.

Build instruction
---------------
*Note*: The plugin has to be a 32-bit library; that means all required libraries have to be compiled in 32-bit and the compiler has to support 32-bit.
#### Windows
1. install a C++ compiler of your choice
2. install [CMake](http://www.cmake.org/)
3. install [Conan](https://conan.io)
4. clone this repository recursively (`git clone --recursive https://...`)
5. create a folder named `build` and execute CMake in there
6. build the generated project files with your C++ compiler

#### Linux
1. install a C++ compiler of your choice
2. install [CMake](http://www.cmake.org/)
3. install [Conan](https://conan.io)
4. clone this repository recursively (`git clone --recursive https://...`)
5. create a folder named `build` and execute CMake in there (`mkdir build && cd build && cmake ..`)
6. build the generated project files with your C++ compiler
