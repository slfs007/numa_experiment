#include <iostream>
#include <tbb/concurrent_queue.h>
#include <thread>
#include <unordered_map>
#include <future>
#include <assert.h>
#include <vector>
#include <cpp_redis/cpp_redis>
using namespace std;
struct Command{
    uint64_t times = 0;
    string key;
    string value;
    Command(){};
    Command(uint64_t t,char *k,char *v):times(t),key(k),value(v){};
};
void read_ckp(const string &path,tbb::concurrent_queue<Command> &queue,
              promise<uint64_t >& prom_d,promise<uint64_t >& prom_c){

    char buf[8*1024];
    unique_ptr<FILE,decltype(&fclose)> ckp_file(fopen(path.c_str(),"r"),fclose);

    if (ckp_file.get() == nullptr){
        cerr << "ckp file open error!" << endl;
        return ;
    }

    fgets(buf,sizeof(buf),ckp_file.get());
    char *end;
    auto d = strtoull(buf, &end, 10);
    auto c = strtoull(end, &end, 10);
    cout << "d and c is set . d = " << d << " c = " << c << endl;
    prom_c.set_value(c);
    prom_d.set_value(d);

    while(fgets(buf,sizeof(buf),ckp_file.get()) != NULL){
        auto key = strchr(buf,'\t');
        if (key == nullptr)
            continue;
        *key = 0;
        key++;
        auto val = strchr(key,'\t');
        if (val == nullptr)
            continue;
        *val = 0;
        val++;
        queue.emplace(atoll(buf),key,val);
    }
}
void read_log(const string &path,tbb::concurrent_queue<Command>& queue){
    char buf[8*1024];
    unique_ptr<FILE,decltype(&fclose)> log_file(fopen(path.c_str(),"r"),fclose);
    if (log_file.get() == nullptr){
        cerr << "log file open error!" << endl;
        return ;
    }
    while(fgets(buf,sizeof(buf),log_file.get()) != NULL){
        auto val = strchr(buf,'\t');
        if (val == nullptr)
            continue;
        *val = 0;
        val++;
        queue.emplace(0,buf,val);
    }
}
void thr_read_ckp_and_log(const string ckp_path,const string log_path,
                          tbb::concurrent_queue<Command> &queue,
                          promise<uint64_t >& prom_d,promise<uint64_t >& prom_c){
    Command empty_cmd;
    cout << "[load thread] start." << endl;
    cout << "[load thread] load ckp." << endl;
    read_ckp(ckp_path,queue,prom_d,prom_c);
    queue.push(empty_cmd);
    cout << "[load thread] load log." << endl;
    read_log(log_path,queue);
    queue.push(empty_cmd);
    cout << "[load thread] end." << endl;
}
uint64_t default_hash(const string &key){
    static hash<const char*> h;
    return h(key.c_str());
}

void distribute_command_in_checkpoint(tbb::concurrent_queue<Command> &queue,
                                      uint64_t v,
                                      unordered_map<string,uint32_t > &map,
                                      vector<tbb::concurrent_queue<Command>> &sub_queue){


}
void distribute_command_in_log(tbb::concurrent_queue<Command> &queue,
                               unordered_map<string,uint32_t > &map,
                               vector<tbb::concurrent_queue<Command>> &sub_queue){
    Command cmd;
    const auto sub_queue_size = sub_queue.size();

    auto iter = map.begin();
    while(1){
        while(!queue.try_pop(cmd)){
            usleep(10);
        }
        if (cmd.key.size() == 0)
            break;
        iter = map.find(cmd.key);
        uint32_t i;
        if (iter == map.end()){
            i = default_hash(cmd.key);
        }else{
            i = iter->second;
        }
        sub_queue[i%sub_queue_size].push(cmd);
    }
}

void distribute_command(tbb::concurrent_queue<Command> &queue,
                        shared_future<uint64_t > d,shared_future<uint64_t > c,
                        vector<tbb::concurrent_queue<Command>> &sub_queue){
    cout << "[dis_cmd thread] start." << endl;

    unordered_map<string,uint32_t > map;

    const uint64_t D = d.get();
    const uint64_t C = c.get();
    assert(D != 0);
    assert(D < C);
    const uint64_t V = C/D;
    assert(V != 0);

    cout << "[dis_cmd thread] get command from checkpoint." << endl;
    distribute_command_in_checkpoint(queue,V,map,sub_queue);
    cout << "[dis_cmd thread] get command from log." << endl;
    distribute_command_in_log(queue,map,sub_queue);

    Command empty_cmd;
    for( auto & each_sub_queue : sub_queue){
        each_sub_queue.push(empty_cmd);
    }
    cout << "[dis_cmd thread] end." << endl;

}
void thr_exec_command(tbb::concurrent_queue<Command> &queue,string addr,const uint32_t port){

    cpp_redis::redis_client client;
    client.connect(addr,port);
    if ( !client.is_connected()){
        cerr << "redis connect failed. addr = " << addr << " port = " << port << endl;
        return;
    }
    Command cmd;
    while(1){
        while( !queue.try_pop(cmd)){
            usleep(10);
        }
        if (cmd.key.size() == 0)
            break;
        client.set(cmd.key,cmd.value);
    }
    client.disconnect();
}
int main(int argc,char *argv[]) {
    //argv[1] number of sub_queue
    //argv[2] ckp path
    //argv[3] log path
    //argv[4...] port
    if (argc < 4){
        cerr << "usage ./numa_test [number of exec_thr] [ckp path] [log path] "
                        " [...port(number of ports must equal number of exec_thr)]" << endl;
    }
    const auto n = stoi(argv[1]);
    if (argc != n + 3){
        cerr << "usage ./numa_test [number of exec_thr] [ckp path] [log path] "
                " [...port(number of ports must equal number of exec_thr)]" << endl;
    }

    promise<uint64_t > prom_d,prom_c;
    shared_future<uint64_t> d,c;
    d = prom_d.get_future();
    c = prom_c.get_future();

    tbb::concurrent_queue<Command> queue;
    thread thr_load(thr_read_ckp_and_log, argv[1], argv[2], std::ref(queue), std::ref(prom_d), std::ref(prom_c));
    vector<tbb::concurrent_queue<Command>> sub_queue(n);

    vector<thread> thr_exec;
    for(size_t i = 0; i < n; i++){

        thr_exec.emplace_back(thr_exec_command,std::ref(sub_queue[i]),"127.0.0.1",stoi(argv[i+4]));
    }
    thread thr_d_c(distribute_command, std::ref(queue), d, c, std::ref(sub_queue));
    // 必须等待所有其他线程结束后才能结束本线程，因为其他线程共享了本线程的栈内存，没用智能指针是因为懒
    thr_load.join();
    thr_d_c.join();
    for(auto & thr : thr_exec){
        thr.join();
    }
    cout << "Recovery Finish!" << endl;
    return 0;
}