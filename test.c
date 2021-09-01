#include <stdio.h>

int do_query(char** tuple);

char* data[] = {"3", "30"};

int main() {
    int res = do_query(data);
    printf("%d", res);
}