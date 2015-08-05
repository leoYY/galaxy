#include "task_manager.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/bind.hpp>

#include "gflags/gflags.h"
#include "gce/utils.h"
#include "logging.h"

DECLARE_string(gce_cgroup_root);
DECLARE_string(gce_support_subsystems);
DECLARE_int64(gce_initd_zombie_check_interval);
DECLARE_string(gce_work_dir);

namespace baidu {
namespace galaxy {

TaskManager::TaskManager() : 
    tasks_mutex_(),
    tasks_(),
    background_thread_(1), 
    cgroup_root_(FLAGS_gce_cgroup_root),
    support_subsystems_() {
    // init resource collector engine
}

TaskManager::~TaskManager() {
}

int TaskManager::Init() {
    // TODO create susystem deal
    return 0;
}

int TaskManager::CreatePodTasks(const std::string& podid, const PodDescriptor& pod) {
    MutexLock scope_lock(&tasks_mutex_);
    for (int i = 0; i < pod.tasks_size(); i++) {
        const TaskDescriptor& task = pod.tasks(i);  
        TaskInfo* task_info = new TaskInfo();
        task_info->task_id = GenerateTaskId(podid);
        task_info->pod_id = podid;
        task_info->desc.CopyFrom(task);
        task_info->status.set_state(kPodPending);
        task_info->millicores = task.requirement().millicores();
        tasks_[task_info->task_id] = task_info;
        // 1. prepare workspace 
        if (PrepareWorkspace(task_info) != 0) {
            LOG(WARNING, "prepare task %s workspace failed", 
                    task_info->task_id.c_str()); 
            return -1;
        }
        // 2. prepare cgroup 
        if (PrepareCgroupEnv(task_info) != 0) {
            LOG(WARNING, "prepare task %s cgroup faield", 
                    task_info->task_id.c_str()); 
            return -1;
        }
        // 3. prepare mount path
        if (PrepareVolumeEnv(task_info) != 0) {
            LOG(WARNING, "prepare task %s volume env failed",
                    task_info->task_id.c_str()); 
            return -1;
        }
        LOG(INFO, "prepare task %s success", 
                        task_info->task_id.c_str());
    }
    return 0;
}

int TaskManager::DeletePodTasks() {
    MutexLock scope_lock(&tasks_mutex_);
    std::map<std::string, TaskInfo*>::iterator it = 
        tasks_.begin();
    for (; it != tasks_.end(); ++it) {
        // 4. Terminate Task 
        if (TerminateTask(it->second) != 0) {
            LOG(WARNING, "terminate task %s failed", 
                    it->first.c_str()); 
            return -1;
        }
        // 3. clean mount path
        if (CleanVolumeEnv(it->second) != 0) {
            LOG(WARNING, "clean task %s volume failed",
                    it->first.c_str()); 
            return -1;
        }
        // 2. clean cgroup
        if (CleanCgroupEnv(it->second) != 0) {
            LOG(WARNING, "clean task %s cgroup failed", 
                    it->first.c_str()); 
            return -1;
        }
        // 1. clean workspace
        if (CleanWorkspace(it->second) != 0) {
            LOG(WARNING, "clean task %s workspace failed",
                    it->first.c_str()); 
            return -1;
        }
        delete it->second;
        it->second = NULL;
    }
    tasks_.clear();  
    return 0;
}

int TaskManager::UpdateTasksCpuLimit(const uint32_t millicores) {
    return 0;
}

void TaskManager::LoopCheckTaskStatus() {
}


int TaskManager::Execute(const std::string& command) {
    return 0;
}

int TaskManager::RunTask(const TaskInfo* task_info) {
    return 0;
}

int TaskManager::TerminateTask(const TaskInfo* task_info) {
    return 0;
}

int TaskManager::PrepareWorkspace(TaskInfo* task) {
    std::string workspace_root = FLAGS_gce_work_dir;
    workspace_root.append("/");
    workspace_root.append(task->pod_id);
    if (!file::Mkdir(workspace_root)) {
        LOG(WARNING, "mkdir workspace root failed"); 
        return -1;
    }

    std::string task_workspace = workspace_root;
    task_workspace.append("/");
    task_workspace.append(task->task_id);
    if (!file::Mkdir(task_workspace)) {
        LOG(WARNING, "mkdir task workspace failed");
        return -1;
    }
    task->task_workspace = task_workspace;
    return 0;
}

int TaskManager::PrepareCgroupEnv(const TaskInfo* task) {
    return 0;
}

int TaskManager::PrepareVolumeEnv(const TaskInfo* task) {
    return 0;
}

int TaskManager::CleanWorkspace(const TaskInfo* task) {
    if (file::IsExists(task->task_workspace)
            && !file::Remove(task->task_workspace)) {
        LOG(WARNING, "Remove task %s workspace failed",
                task->task_id.c_str());
        return -1;
    }
    return 0;
}

int TaskManager::CleanCgroupEnv(const TaskInfo* task) {
    return 0;
}

int TaskManager::CleanVolumeEnv(const TaskInfo* task) {
    return 0;
}

std::string TaskManager::GenerateTaskId(const std::string& podid) {
    return "";
}

} // ending namespace galaxy
} // ending namespace baidu


