

#include "Base26Num.h"
#include <cmath>
#include <numeric>
#include "Tokenizer.h"

/**
 * We're actually using Base27 because we want "AA" to be higher than "AAA."
 * Using Base26 would mean both are 0 and have no ordering. Therefore, A corresponds to 1.
 * This makes AA be 11... and AAA be 111...; 11...<111...
 */
constexpr uint64_t LETTER_POW1 = 27;
constexpr uint64_t LETTER_POW2 = 27 * LETTER_POW1;
constexpr uint64_t LETTER_POW3 = 27 * LETTER_POW2;

constexpr uint64_t LETTER_POW4 = 27 * LETTER_POW3;
constexpr uint64_t LETTER_POW5 = 27 * LETTER_POW4;
constexpr uint64_t LETTER_POW6 = 27 * LETTER_POW5;
constexpr uint64_t LETTER_POW7 = 27 * LETTER_POW6;
constexpr uint64_t LETTER_POW8 = 27 * LETTER_POW7;
constexpr uint64_t LETTER_POW9 = 27 * LETTER_POW8;
constexpr uint64_t LETTER_POW10 = 27 * LETTER_POW9;
constexpr uint64_t LETTER_POW11 = 27 * LETTER_POW10;
constexpr uint64_t LETTER_POW12 = 27 * LETTER_POW11;
constexpr uint64_t alphabet_pow[] = {LETTER_POW1, LETTER_POW2, LETTER_POW3, LETTER_POW4, LETTER_POW5, LETTER_POW6,
                                     LETTER_POW7, LETTER_POW8, LETTER_POW9, LETTER_POW10, LETTER_POW11, LETTER_POW12};


constexpr std::size_t MAX_CHARS = 10;

/**
 * Used to convert a string to a 64 bit unsigned integer for quicker comparison and easier memory usage.
 * Only the first MAX_CHARS characters are included in the number. All further characters are ignored.
 * This shouldn't be a problem as these comparisons hint where in the disk to search, from which we
 * compare strings normally.
 */
Base26Num::Base26Num(std::string from) {
    num = 0;
    Tokenizer::remove_punctuation(from);
    const int max_iter = std::min(from.size(), MAX_CHARS);
    for (int i = 0; i < max_iter; i++) {
        num += (from[i] - 'A' + 1) * alphabet_pow[MAX_CHARS - i - 1];
    }
}

Base26Num Base26Num::fiddle(int idx) {
    assert(std::abs(idx) < MAX_CHARS);
    assert(idx != 0);

    auto absidx = std::abs(idx);
    int sign = !std::signbit(idx);

    return Base26Num{num + sign * alphabet_pow[MAX_CHARS - (absidx - 1) - 1]};
}
