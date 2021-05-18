
#ifndef GAME_DOCUMENTSMATCHER_H
#define GAME_DOCUMENTSMATCHER_H

#include "TopDocs.h"
#include <vector>
namespace DocumentsMatcher {

    TopDocs AND(std::vector<TopDocs> &results);
};

#endif //GAME_DOCUMENTSMATCHER_H
