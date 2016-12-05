//
// Created by mk on 16-12-5.
//

#include <iostream>
#include <random>
#include <unordered_map>
#include <algorithm>
#include <thread>
#include <cassert>
#include <cmath>
#include <inttypes.h>

using namespace std;


string random_string(const uint32_t min_len,const uint32_t max_len){
    const auto len = min_len + rand()%(max_len - min_len);
    const char ch = 'a' + rand()%26;
    string str(len,ch);
    return str;
}
int main(int argc,char *argv[]){
    //命令总数C，数据总量D，分布类型【正态分布】
    char *end;
    const auto D = strtoull(argv[1],&end,10);
    const auto C = strtoull(argv[2],&end,10);
    const char *ckp_path = argv[3];
    const char *log_path = argv[4];
    std::random_device rd;
    std::mt19937 gen(rd());
    std::normal_distribution<> nd(D,1);
    std::unordered_map<int64_t ,uint64_t > map;
    cout << " generate log file" << endl;
    uint64_t log_items = 0;
    {
        std::unique_ptr<FILE,decltype(&fclose)> log_file(fopen(log_path,"w+"),fclose);

        while(map.size() < D || log_items < C){
            int64_t data = std::llround(nd(gen));
            map[data]++;
            fprintf(log_file.get(),"%" PRId64 "\t%s\n",data,random_string(512,1024).c_str());
            log_items++;
        }
    }
    cout << "generate checkpoint file" << endl;
    {
        std::unique_ptr<FILE,decltype(&fclose)> ckp_file(fopen(ckp_path,"w+"),fclose);
        fprintf(ckp_file.get(),"%" PRIu64 "\t%" PRId64 "\n",map.size(),log_items);
        for(auto each : map){
            fprintf(ckp_file.get(),"%" PRIu64 "\t%" PRId64 "\t%s\n",each.second,each.first,random_string(512,1024).c_str());

        }
    }

}