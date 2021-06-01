#ifndef GAME_BASE26NUM_H
#define GAME_BASE26NUM_H
#include <cstdint>
#include <string>

struct Base26Num {
    explicit Base26Num(std::string from);

    uint64_t num; // Represent 3 alphabet letters in uint16_t.
    explicit Base26Num(uint64_t num): num(num){};

    bool operator<(Base26Num other) const {
        return num < other.num;
    }

    Base26Num operator+(Base26Num other) {
        return Base26Num{num + other.num};
    }
    Base26Num operator-(Base26Num other) {
        if (other.num >= num)  return *this;
        else return Base26Num{num - other.num};
    }
};

#endif //GAME_BASE26NUM_H
