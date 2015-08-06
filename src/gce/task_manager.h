#ifndef TASK_MANAGER_H
#define TASK_MANAGER_H

#include <string>
#include "proto/galaxy.pb.h"
#include "proto/initd.pb.h"
#include "mutex.h"
#include "thread_pool.h"
#include "rpc/rpc_client.h"

namespace baidu {
namespace galaxy {

class TaskManager {
public:
    TaskManager();

    ~TaskManager();

    int Init();

    int CreateTasks(const std::string& podid, const PodDescriptor& pod);

    int DeleteTasks();

    int QueryTasks(std::vector<TaskStatus>* tasks);

    int UpdateCpuLimit(const std::string& task_id, const uint32_t millicores);

private:
    enum TaskStage {
        kStagePENDING = 0,
        kStageDEPLOYING = 1, 
        kStageRUNNING = 2,
        kStageSTOPPING = 3,
        kStageENDING = 4
    };
    struct TaskInfo {
        // meta infomation
        std::string task_id;
        std::string pod_id;
        TaskDescriptor desc;
        std::string initd_endpoint;
        TaskStage stage;
        // check stage state use TaskStatus and stage exit_code
        // dynamic resource usage
        ProcessInfo main_process;
        ProcessInfo deploy_process;
        ProcessInfo stop_process;
        TaskStatus status;
        std::string cgroup_path;
        uint32_t millicores;
        std::string task_workspace;
        TaskInfo() : 
            task_id(),
            pod_id(),
            desc(), 
            initd_endpoint(),
            stage(kStagePENDING),
            main_process(), 
            deploy_process(),
            stop_process(),
            status(),
            cgroup_path(),
            millicores(0),
            task_workspace() {
        }
    };

    int Update(const std::string& task_id, 
               const uint32_t millicores);

    int QueryProcessInfo(const std::string& initd_endpoint, 
                         ProcessInfo* process_info);

    void LoopCheckTaskStatus();
    
    int PrepareWorkspace(TaskInfo* task);
    int PrepareCgroupEnv(const TaskInfo* task);
    int PrepareResourceCollector(const TaskInfo* task);
    int PrepareVolumeEnv(const TaskInfo* task);

    int DeployTask(TaskInfo* task_info);
    int RunTask(TaskInfo* task_info);
    int TerminateTask(TaskInfo* task_info);

    int CleanWorkspace(const TaskInfo* task);
    int CleanCgroupEnv(const TaskInfo* task);
    int CleanResourceCollector(const TaskInfo* task);
    int CleanVolumeEnv(const TaskInfo* task);

    // kStagePENDING -> kStageDEPLOYING
    // kStageDEPLOYING -> kStageRUNNING
    void DelayCheckTaskStageChange(const std::string& key);

    std::string GenerateTaskId(const std::string& podid);

private:
    // key task id
    Mutex tasks_mutex_;
    std::map<std::string, TaskInfo*> tasks_;

    ThreadPool background_thread_;
    std::string cgroup_root_;
    std::vector<std::string> hierarchies_;
    RpcClient* rpc_client_;
};

} // ending namespace galaxy
} // ending namespace baidu

#endif
