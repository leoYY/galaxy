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

    int RunTask(const TaskInfo* task_info);
    int TerminateTask(const TaskInfo* task_info);

    int Execute(const std::string& desc);

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

    std::string GenerateTaskId(const std::string& podid);
private:
    // key task id
    Mutex tasks_mutex_;
    std::map<std::string, TaskInfo*> tasks_;

    ThreadPool background_thread_;
    std::string cgroup_root_;
    std::vector<std::string> support_subsystems_;
};

} // ending namespace galaxy
} // ending namespace baidu

#endif
