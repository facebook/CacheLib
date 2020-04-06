#include <cachelot/common.h>
#include <cachelot/cache.h>
#include <cachelot/random.h>
#include <cachelot/hash_fnv1a.h>
#include <cachelot/stats.h>

#include <iostream>
#include <iomanip>

using namespace cachelot;

using std::chrono::microseconds;
using std::chrono::milliseconds;
using std::chrono::seconds;
using std::chrono::minutes;
using std::chrono::hours;

constexpr size_t num_items = 1000000;
// Daniel at Ben: the cache memory size must be a power of two for some reason
constexpr size_t cache_memory = 512 * Megabyte;
constexpr size_t page_size = 4 * Megabyte;
constexpr size_t hash_initial = 131072;
constexpr uint8 min_key_len = 14;
constexpr uint8 max_key_len = 40;
constexpr uint32 min_value_len = 12;
constexpr uint32 max_value_len = 45;

namespace {

    // Hash function
    static auto calc_hash = fnv1a<cache::Cache::hash_type>::hasher();

    static struct stats_type {
        uint64 num_get = 0;
        uint64 num_set = 0;
        uint64 num_del = 0;
        uint64 num_cache_hit = 0;
        uint64 num_cache_miss = 0;
        uint64 num_error = 0;
    } bench_stats;

    inline void reset_stats() {
        new (&bench_stats)stats_type();
    }

}

typedef std::tuple<string, string> kv_type;
typedef std::vector<kv_type> array_type;
typedef array_type::const_iterator iterator;


// TODO: BEN: does this do what we want?
class CacheWrapper {
public:
    CacheWrapper() : m_cache(cache::Cache::Create(cache_memory, page_size, hash_initial, true)) {}

    void set(iterator it) {
        slice k (std::get<0>(*it).c_str(), std::get<0>(*it).size());
        slice v (std::get<1>(*it).c_str(), std::get<1>(*it).size());
        cache::ItemPtr item = nullptr;
        try {
            item = m_cache.create_item(k, calc_hash(k), v.length(), /*flags*/0, cache::Item::infinite_TTL);
            item->assign_value(v);
            m_cache.do_set(item);
            bench_stats.num_set += 1;
        } catch (const std::exception &) {
            bench_stats.num_error += 1;
        }
    }

    void get(iterator it) {
        slice k (std::get<0>(*it).c_str(), std::get<0>(*it).size());
        auto found_item = m_cache.do_get(k, calc_hash(k));
        bench_stats.num_get += 1;
        auto & counter = found_item ? bench_stats.num_cache_hit : bench_stats.num_cache_miss;
        counter += 1;
    }

    void del(iterator it) {
        slice k (std::get<0>(*it).c_str(), std::get<0>(*it).size());
        bool found = m_cache.do_delete(k, calc_hash(k));
        bench_stats.num_del += 1;
        auto & counter = found ? bench_stats.num_cache_hit : bench_stats.num_cache_miss;
        counter += 1;
    }
private:
    cache::Cache m_cache;
};


array_type data_array;
std::unique_ptr<CacheWrapper> csh;

inline iterator random_pick() {
    debug_assert(data_array.size() > 0);
    static random_int<array_type::size_type> rndelem(0, data_array.size() - 1);
    auto at = rndelem();
    return data_array.begin() + at;
}


static void generate_test_data() {
    data_array.reserve(num_items);
    for (auto n=num_items; n > 0; --n) {
        kv_type kv(random_string(min_key_len, max_key_len),
                   random_string(min_value_len, max_value_len));
        data_array.emplace_back(kv);
    }
}


static void warmup() {
    for (auto n=hash_initial; n > 0; --n) {
        csh->set(random_pick());
    }
}


auto chance = random_int<size_t>(1, 100);

int main(int /*argc*/, char * /*argv*/[]) {
    csh.reset(new CacheWrapper());
    generate_test_data();
    warmup();
    reset_stats();
    auto start_time = std::chrono::high_resolution_clock::now();
    for (int i=0; i<3; ++i) {
        for (iterator kv = data_array.begin(); kv < data_array.end(); ++kv) {
            csh->set(kv);
            if (chance() > 70) {
                csh->del(random_pick());
            }
            if (chance() > 30) {
                csh->get(random_pick());
            }
        }
    }




    // JUST ANALYSIS stuff

    auto time_passed = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now() - start_time);
    const double sec = time_passed.count() / 1000000000;
    std::cout << std::fixed << std::setprecision(3);
    std::cout << "Time spent: " << sec << "s" << std::endl;
    std::cout << "get:        " << bench_stats.num_get << std::endl;
    std::cout << "set:        " << bench_stats.num_set << std::endl;
    std::cout << "del:        " << bench_stats.num_del << std::endl;
    std::cout << "cache_hit:  " << bench_stats.num_cache_hit << std::endl;
    std::cout << "cache_miss: " << bench_stats.num_cache_miss << std::endl;
    std::cout << "error:      " << bench_stats.num_error << std::endl;
    const double RPS = (bench_stats.num_get + bench_stats.num_set + bench_stats.num_del) / sec;
    std::cout << "rps:        " << RPS << std::endl;
    std::cout << "avg. cost:  " << static_cast<unsigned>(1000000000 / RPS) << "ns" << std::endl;
    std::cout << std::endl;
    PrintStats();

    return 0;
}
