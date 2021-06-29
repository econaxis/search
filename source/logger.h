//
// Created by henry on 2021-06-24.
//

#ifndef GAME_LOGGER_H
#define GAME_LOGGER_H

#include <fmt/ostream.h>
#include <experimental/source_location>
#include <syslog.h>
#include <iostream>
namespace log_priv {
    inline bool open_log() {
        openlog("search", LOG_CONS, LOG_USER);
        return true;
    }
}

inline void log(const std::string &log_string,
                const std::experimental::source_location location = std::experimental::source_location::current()) {
    const static bool opened = log_priv::open_log();

    auto deb_FILE = fmt::format("SOURCE_FILE={}", std::string(location.file_name()));
    auto deb_LINE = fmt::format("SOURCE_LINE={}", std::to_string(location.line()));
    auto deb_FUNCTION = fmt::format("SOURCE_FUNCTION={}", std::string(location.function_name()));
    auto deb_STRING = fmt::format("MESSAGE={}", log_string);
    auto final = fmt::format("{}:{},{},{}",
                             deb_FILE,
                             deb_FUNCTION,
                             deb_LINE,
                             deb_STRING
    );
    syslog(LOG_DEBUG, "%s", final.c_str());
}

inline void log(const auto &var1, const auto &var2, const auto &var3, const auto& var4,
                const std::experimental::source_location location = std::experimental::source_location::current()) {
    log(fmt::format("{} {} {} {}", var1, var2, var3, var4), location);
}

inline void log(const auto &var1, const auto &var2, const auto &var3,
                const std::experimental::source_location location = std::experimental::source_location::current()) {
    log(fmt::format("{} {} {}", var1, var2, var3), location);
}


inline void log(const auto &var1, const auto &var2,
                const std::experimental::source_location location = std::experimental::source_location::current()) {
    log(fmt::format("{} {}", var1, var2), location);
}


inline void log(auto var1,
                const std::experimental::source_location location = std::experimental::source_location::current()) {
    log(fmt::format("{}", var1), location);
}

template<typename ...Params>
void print(Params &&... params) {
    log(std::forward<Params>(params)...);
    (std::cout<<...<<params);
}



#endif //GAME_LOGGER_H
