//
// Created by henry on 2021-06-24.
//

#ifndef GAME_LOGGER_H
#define GAME_LOGGER_H

#include <fmt/ostream.h>
#include <experimental/source_location>
#include <syslog.h>
#include <string>
#include <thread>
#include <iostream>

namespace log_priv {
    inline bool open_log() {
        openlog("search", LOG_CONS, LOG_USER);
        return true;
    }
}

inline void log(const std::string &log_string, const std::experimental::source_location location) {
    const static bool opened = log_priv::open_log();

//    auto deb_FILE = fmt::format("SOURCE_FILE={}", std::string(location.file_name()));
//    auto deb_LINE = fmt::format("SOURCE_LINE={}", std::to_string(location.line()));
//    auto deb_FUNCTION = fmt::format("SOURCE_FUNCTION={}", std::string(location.function_name()));
//    auto deb_THREADID = fmt::format("THREADID={}", std::this_thread::get_id());
    auto deb_STRING = fmt::format("MESSAGE=\"{}\"", log_string);
//    auto final = fmt::format("{}         [{},{},{},{}]",
//                             deb_STRING,
//                             deb_FILE,
//                             deb_FUNCTION,
//                             deb_LINE,
//                             deb_THREADID
//    );
    syslog(LOG_DEBUG, "%s", deb_STRING.c_str());
}

inline void log(const auto &var1, const auto &var2, const auto &var3, const auto &var4, const auto& var5,
                const std::experimental::source_location location = std::experimental::source_location::current()) {
    log(fmt::format("{} {} {} {} {}", var1, var2, var3, var4, var5), location);
}

inline void log(const auto &var1, const auto &var2, const auto &var3, const auto &var4,
                const std::experimental::source_location location = std::experimental::source_location::current()) {
    log(fmt::format("{} {} {} {}", var1, var2, var3, var4), location);
}

inline void log(const auto &var1, const auto &var2, const auto &var3,
                std::experimental::source_location location = std::experimental::source_location::current()) {
    log(fmt::format("{} {} {}", var1, var2, var3), location);
}


inline void log(const auto &var1, const auto &var2,
                std::experimental::source_location location = std::experimental::source_location::current()) {
    log(fmt::format("{} {}", var1, var2), location);
}


inline void log(auto var1,
                std::experimental::source_location location = std::experimental::source_location::current()) {
    log(fmt::format("{}", var1), location);
}

template<typename ...Params>
inline void print(Params &&... params) {
    log(std::forward<Params>(params)...);
    ((std::cout << " " << params), ...);
    std::cout<<"\n";
}

inline void print_range(const std::string& str, auto beg, auto end) {
    std::string buffer;
    while(beg != end) {
        buffer.append(*(beg++));

        // Only add the separator (space) if not at the last element.
        if(beg + 1 != end) buffer.append(" ");
    }
    log(fmt::format("{} {}", str, buffer));
}


#endif //GAME_LOGGER_H
