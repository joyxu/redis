#include "obj_gen.h"

#include <assert.h>
#include <stdio.h>
#include <vector>

int main() {
    static const unsigned long long kMin = 1;
    static const unsigned long long kMax = 10000;
    static const unsigned long long kSamples = 1000000;
    std::vector<unsigned long long> counts(kMax - kMin + 1, 0);
    object_generator generator;

    generator.set_random_seed(12345);
    for (unsigned long long i = 0; i < kSamples; i++) {
        unsigned long long key = generator.zipfian_distribution(kMin, kMax, 1.0);
        assert(key >= kMin && key <= kMax);
        counts[key - kMin]++;
    }

    /* s=1 must be skewed, unlike the former uniform implementation. */
    assert(counts[0] > 50000);
    assert(counts[0] > counts[9]);
    assert(counts[9] > counts[99]);
    assert(counts[99] > counts[999]);
    printf("obj_gen_zipf_ut: PASS top=%llu rank10=%llu rank100=%llu rank1000=%llu\n",
           counts[0], counts[9], counts[99], counts[999]);
    return 0;
}
