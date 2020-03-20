/* Copyright (C) 2020 D8DATAWORKS - All Rights Reserved */

#ifdef TEST_RUNNER


#define CATCH_CONFIG_RUNNER
#include "catch.hpp"
#include "blocksync/test.hpp"

int main(int argc, char* argv[])
{
    return Catch::Session().run(argc, argv);
}


#endif //TEST_RUNNER



#if ! defined(TEST_RUNNER)


#include "clipp.h"

using namespace clipp;

int main(int argc, char* argv[])
{
    string path = "store";
    size_t threads = 1;

    auto cli = (
        option("-p", "--path").doc("Path where blocks and database are stored") & value("directory", path),
        option("-t", "--threads").doc("How many event threads per open port") & value("threads", threads)
        );

    try
    {
    }
    catch (const exception & ex)
    {
        cerr << ex.what() << endl;
        return -1;
    }

    return 0;
}


#endif