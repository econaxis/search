#include "stdio.h"

extern void myfunc();

int main() {
    int iters;
    scanf("%d", &iters);
    for(int i =0; i < iters; i++) {
        i = i + 1;
        myfunc();
    }
}