ADD_LIBRARY(bsdiff STATIC bsdiff.c bspatch.c)
ADD_EXECUTABLE(bs_tests bs_tests.c)

TARGET_LINK_LIBRARIES(bs_tests bsdiff)

ADD_TEST(bs_tests bs_tests.c)
