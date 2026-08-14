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

**How to install on an open.mp server**
-----------------------------------
1. Extract the contents of the archive to a directory, copy the file(s) in plugins into **COMPONENTS** if you do not do this, it will try to load as a SA:MP plugin instead.
2. Edit your configuration file (**config.json**) as follows:
   ```json
      "discord": {
         "bot_token": "MYBOTTOKEN"
      }
    ```
   Alternatively you can use the environment variable **DCC_BOT_TOKEN** to set the token instead. **DO NOT SHARE YOUR TOKEN WITH ANYONE**

How to install on a SA:MP server
--------------------------------
1. Extract the content of the downloaded archive into the root directory of your SA-MP server.
2. Edit the server configuration (*server.cfg*) as follows:
   - Windows: `plugins discord-connector`
   - Linux: `plugins discord-connector.so`
3. Add `discord_bot_token YOURDISCORDBOTTOKEN` to your *server.cfg* file, or set it in the environment variable `DCC_BOT_TOKEN` (__never share your bot token with anyone!__)

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
