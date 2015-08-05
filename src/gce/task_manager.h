#ifndef TASK_MANAGER_H
#define TASK_MANAGER_H

#include <string>
#include "proto/galaxy.pb.h"
#include "proto/initd.pb.h"
#include "mutex.h"
#include "thread_pool.h"

namespace baidu {
namespace galaxy {

class TaskManager {
public:
    TaskManager();

    ~TaskManager();

    int Init();

    int CreatePodTasks(const std::string& podid, const PodDescriptor& pod);

    int DeletePodTasks();

    int QueryPodTasks(std::vector<TaskStatus>* tasks);

    int UpdateTasksCpuLimit(const uint32_t millicores);

private:
    struct TaskInfo {
        // meta infomation
        std::string task_id;
        std::string pod_id;
        TaskDescriptor desc;

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
            main_process(), 
            deploy_process(),
            stop_process(),
            status(),
            cgroup_path(),
            millicores(0),
            task_workspace() {
        }
    };

    int DeployTask(const TaskInfo* task_info);
    int RunTask(const TaskInfo* task_info);
    int TerminateTask(const TaskInfo* task_info);

    //int Execute(const std::string& desc);

    //int Kill(const std::string& task_id);

    int Update(const std::string& task_id, 
               const uint32_t millicores);

    //int Show(TaskInfo* info);

    bool AttachCgroup(const std::string& cgroup_path, pid_t pid);

    void LoopCheckTaskStatus();
    
    //int PrepareMountNamespace(const PodDescriptor& pod);
    int PrepareWorkspace(TaskInfo* task);
    int PrepareCgroupEnv(const TaskInfo* task);
    int PrepareVolumeEnv(const TaskInfo* task);

    int CleanWorkspace(const TaskInfo* task);
    int CleanCgroupEnv(const TaskInfo* task);
    int CleanVolumeEnv(const TaskInfo* task);

    //int PrepareInitd(const std::string& pod_id);
    //int ReapInitd(const std::string& pod_id);
    //int AllocInitdPort(int* port_num) {
    //    return -1; 
    //}

    std::string GenerateTaskId(const std::string& podid);
private:
    //struct InitdConfig {
    //    int stdout_fd;
    //    int stderr_fd;
    //    int port;
    //    int pid;
    //    std::string initd_bin_path;
    //    std::string initd_run_path;
    //    std::vector<int> fds;
    //    InitdConfig() : 
    //        stdout_fd(0), 
    //        stderr_fd(0), 
    //        port(0),
    //        pid(0),
    //        initd_bin_path(),
    //        initd_run_path(),
    //        fds() {}
    //};
    // key task id
    Mutex tasks_mutex_;
    std::map<std::string, TaskInfo*> tasks_;

    ThreadPool background_thread_;
    std::string cgroup_root_;
    //std::vector<std::string> support_subsystems_;
    std::vector<std::string> hierarchies_;
    // TODO initd port only use between [initd_port_begin_, initd_port_end_)
    std::vector<int> initd_port_used_;
    int initd_port_begin_;
    int initd_port_end_;
    int initd_next_port_;
};

} // ending namespace galaxy
} // ending namespace baidu

#endif
