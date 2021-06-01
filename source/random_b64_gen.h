#ifndef GAME_RANDOM_B64_GEN_H
#define GAME_RANDOM_B64_GEN_H

#include <random>

inline std::mt19937 &randgen() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    return gen;
}
inline constexpr std::string_view b64chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890-_";
inline std::string random_b64_str(int length = 200) {
    std::uniform_int_distribution<uint> dist(0, b64chars.size() - 1); // ASCII table codes for normal characters.

    std::string output;
    output.reserve(length);

    for (int i = 0; i < length; i++) {
        auto temp = dist(randgen());

        output += static_cast<char>(b64chars[temp]);
    }
    return output;
}


#endif //GAME_RANDOM_B64_GEN_H
