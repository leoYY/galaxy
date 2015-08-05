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
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/bind.hpp>

#include "gflags/gflags.h"
#include "gce/utils.h"
#include "logging.h"
#include "gce/cgroups.h"

DECLARE_string(gce_cgroup_root);
DECLARE_string(gce_support_subsystems);
DECLARE_int64(gce_initd_zombie_check_interval);
DECLARE_string(gce_work_dir);
DECLARE_string(gce_initd_bin);
DECLARE_int32(gce_initd_port_begin);
DECLARE_int32(gce_initd_port_end);

namespace baidu {
namespace galaxy {

static int LaunchInitd(void *) {

}

TaskManager::TaskManager() : 
    tasks_mutex_(),
    tasks_(),
    background_thread_(1), 
    cgroup_root_(FLAGS_gce_cgroup_root),
    hierarchies_(),
    initd_port_used_(), 
    initd_port_begin_(FLAGS_gce_initd_port_begin),
    initd_port_end_(FLAGS_gce_initd_port_end),
    initd_next_port_(initd_port_begin_) {
    // init resource collector engine
    // TODO initd_port_end > initd_port_begin
    initd_port_used_.resize(initd_port_end_ - initd_port_begin_);
}

TaskManager::~TaskManager() {
}

int TaskManager::Init() {
    // TODO create susystem deal
    return 0;
}

int TaskManager::CreatePodTasks(const std::string& podid, const PodDescriptor& pod) {
    MutexLock scope_lock(&tasks_mutex_);
    if (PrepareInitd(podid) != 0) {
        LOG(WARNING, "initd prepare failed for %s",
                podid.c_str()); 
        return -1;
    }
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
    std::string pod_id;
    for (; it != tasks_.end(); ++it) {
        pod_id = it->second->pod_id;
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
    if (0 != ReapInitd(pod_id)) {
        LOG(WARNING, "reap initd for pod %s failed",
                pod_id.c_str());
        return -1;
    }
    return 0;
}

int TaskManager::UpdateTasksCpuLimit(const uint32_t millicores) {
    return 0;
}

void TaskManager::LoopCheckTaskStatus() {
}


//int TaskManager::Execute(const std::string& command) {
//    return 0;
//}

int TaskManager::DeployTask(const TaskInfo* task_info) {
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
    if (task == NULL) {
        return -1; 
    }
    std::vector<std::string>::iterator hier_it = 
        hierarchies_.begin();
    std::string cgroup_name = task->task_id;
    for (; hier_it != hierarchies_.end(); ++ hier_it) {
        std::string cgroup_dir = *hier_it;
        cgroup_dir.append("/");
        cgroup_dir.append(cgroup_name);
        if (!file::Mkdir(cgroup_dir)) {
            LOG(WARNING, "create dir %s failed for %s",
                    cgroup_dir.c_str(), task->task_id.c_str()); 
            return -1;
        }              
    }
    // TODO set cgroup contro_file
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
    std::string workspace_root = FLAGS_gce_work_dir;
    workspace_root.append("/");
    workspace_root.append(task->pod_id);
    if (file::IsExists(workspace_root)
            && !file::Remove(workspace_root)) {
        return -1;     
    }
    return 0;
}

int TaskManager::CleanCgroupEnv(const TaskInfo* task) {
    if (task == NULL) {
        return -1; 
    } 
    // TODO do set control_file  frozen ?
    std::vector<std::string>::iterator hier_it = 
        hierarchies_.begin();
    std::string cgroup = task->task_id;
    for (; hier_it != hierarchies_.end(); ++hier_it) {
        std::string cgroup_dir = *hier_it;
        cgroup_dir.append("/");
        cgroup_dir.append(cgroup);
        if (!file::IsExists(cgroup_dir)) {
            LOG(INFO, "%s %s not exists",
                    (*hier_it).c_str(), cgroup.c_str()); 
            continue;
        }
        // TODO kill pids
        if (::rmdir(cgroup_dir.c_str()) != 0
                && errno != ENOENT) {
            LOG(WARNING, "rmdir %s failed err[%d: %s]",
                    cgroup_dir.c_str(), errno, strerror(errno));
            return -1;
        }
    }
    return 0;
}

int TaskManager::CleanVolumeEnv(const TaskInfo* task) {
    return 0;
}

std::string TaskManager::GenerateTaskId(const std::string& podid) {
    boost::uuids::uuid uuid = boost::uuids::random_generator()();    
    std::stringstream sm_uuid;
    sm_uuid << uuid;
    std::string str_uuid = podid;
    str_uuid.append("_");
    str_uuid.append(sm_uuid.str());
    return str_uuid;
}

//static int STACK_SIZE = 1024 * 1024;
//static char INITD_STACK_BUFFER[STACK_SIZE];
//
//int TaskManager::PrepareInitd(const std::string& pod_id) {
//    std::string work_space = FLAGS_gce_work_dir;
//    work_space.append("/");
//    work_space.append(pod_id);
//    if (!file::Mkdir(work_space)) {
//        LOG(WARNING, "workspace %s mkdir failed for pod id %s",
//                work_space.c_str(), pod_id.c_str());    
//        return -1;
//    }
//
//    InitdConfig* initd_config = new InitdConfig();
//    initd_config->initd_run_path = work_space;
//    initd_config->initd_bin_path = FLAGS_gce_initd_bin;
//    // alloc initd port 
//    if (!AllocInitdPort(&(initd_config->port))) {
//        LOG(WARNING, "no enough initd port for pod id %s",
//                pod_id.c_str()); 
//        delete initd_config;
//        return -1;
//    }
//
//    // prepare stdout stderr fds for initd
//    if (!process::PrepareStdFds(
//                initd_config->initd_run_path, 
//                &(initd_config->stdout_fd), 
//                &(initd_config->stderr_fd))) {
//        LOG(WARNING, "prepare initd std fds failed"); 
//        delete initd_config;
//        return -1;
//    }  
//
//    // collect process fds 
//    // NOTE only deal with pbrpc fds, fds created 
//    // by agent should use CLOSE ON EXEC flag
//    pid_t curpid = ::getpid();
//    process::GetProcessOpenFds(curpid, &(initd_config->fds));
//
//// for internal centos4 not define CLONE_NEWPID
//#ifndef CLONE_NEWPID
//#define CLONE_NEWPID 0x20000000
//#endif 
//#ifndef CLONE_NEWUTS
//#define CLONE_NEWUTS 0x04000000
//#endif
//    const int CLONE_FLAGS = CLONE_NEWNS | CLONE_NEWPID 
//        | CLONE_NEWUTS;
//    // do clone for initd in new PID namespace and other namespace
//    int  
//    delete initd_config;
//    return 0;
//}
//
//int TaskManager::ReapInitd(const std::string& pod_id) {
//    return 0;
//}

} // ending namespace galaxy
} // ending namespace baidu


