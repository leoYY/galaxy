// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "agent/pods_manager.h"
#include <sched.h>
#include <signal.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include <string>
#include <vector>

#include <gflags/gflags.h>

#include "boost/lexical_cast.hpp"
#include "logging.h"
#include "agent/utils.h"
#include "agent/task_manager.h"

// for kernel 2.6.32 and glibc not define some clone flag
#ifndef CLONE_NEWPID        
#define CLONE_NEWPID 0x20000000
#endif

#ifndef CLONE_NEWUTS
#define CLONE_NEWUTS 0x04000000
#endif 

DECLARE_string(gce_initd_bin);
DECLARE_string(gce_work_dir);

namespace baidu {
namespace galaxy {


struct LanuchInitdContext {
    int stdout_fd;
    int stderr_fd;
    std::string start_command;
    std::string path;
    std::vector<int> fds;
};

static int LanuchInitdMain(void *arg) {
    LanuchInitdContext* context = 
        reinterpret_cast<LanuchInitdContext*>(arg);
    if (context == NULL) {
        return -1; 
    }

    process::PrepareChildProcessEnvStep1(::getpid(), 
                                         context->path.c_str());  
    process::PrepareChildProcessEnvStep2(context->stdout_fd, 
                                         context->stderr_fd, 
                                         context->fds);
    char* argv[] = {
        const_cast<char*>("sh"),
        const_cast<char*>("-c"),
        const_cast<char*>(context->start_command.c_str()),
        NULL};
    ::execv("/bin/sh", argv);
    assert(0);
    return 0;
}

PodsManager::PodsManager() : 
    pods_(), 
    task_manager_(NULL) {
}

PodsManager::~PodsManager() {
    if (task_manager_ != NULL) {
        delete task_manager_;
        task_manager_ = NULL;
    }
}

int PodsManager::Init() {
    task_manager_ = new TaskManager();
    return task_manager_->Init();
}

int PodsManager::LanuchInitd(PodInfo* info) {
    if (info == NULL) {
        return -1; 
    }
    const int CLONE_FLAG = CLONE_NEWNS | CLONE_NEWPID 
                            | CLONE_NEWUTS;
    const int CLONE_STACK_SIZE = 1024 * 1024;
    static char CLONE_STACK[CLONE_STACK_SIZE];
    
    LanuchInitdContext context;
    context.stdout_fd = 0; 
    context.stderr_fd = 0;
    context.start_command = FLAGS_gce_initd_bin;
    context.start_command.append(" --gce_initd_port=");
    context.start_command.append(boost::lexical_cast<std::string>(info->initd_port));
    context.path = FLAGS_gce_work_dir + "/" + info->pod_id;
    if (!file::Mkdir(context.path)) {
        LOG(WARNING, "mkdir %s failed", context.path.c_str()); 
        return -1;
    }

    if (!process::PrepareStdFds(context.path,
                                &context.stdout_fd,
                                &context.stderr_fd)) {
        LOG(WARNING, "prepare %s std file failed", 
                context.path.c_str()); 
        return -1;
    }
    
    process::GetProcessOpenFds(::getpid(), &context.fds);
    int child_pid = ::clone(&LanuchInitdMain, 
                            CLONE_STACK + CLONE_STACK_SIZE, 
                            CLONE_FLAG | SIGCHLD, 
                            &context);
    if (child_pid == -1) {
        LOG(WARNING, "clone initd for %s failed err[%d: %s]",
                    info->pod_id.c_str(), errno, strerror(errno));      
        return -1;
    }
    info->initd_pid = child_pid;
    return 0;
}

void PodsManager::CheckPod(const std::string& pod_id) {
    std::map<std::string, PodInfo>::iterator pod_it = 
        pods_.find(pod_id);
    if (pod_it == pods_.end()) {
        return; 
    }

    PodInfo& pod_info = pod_it->second;
    // all task delete by taskmanager, no need check
    if (pod_info.tasks.size() == 0) {
        // TODO check initd exits
        ::kill(pod_info.initd_pid, SIGTERM);
        pods_.erase(pod_it);
        return;
    }

    std::map<std::string, TaskInfo>::iterator task_it = 
        pod_info.tasks.begin();
    for (; task_it != pod_info.tasks.end(); ++task_it) {
        if (task_manager_->QueryTasks(&(task_it->second)) != 0) {
            pod_info.tasks.erase(task_it);             
        }
    }
    return;
}

int PodsManager::ShowPods(std::vector<PodInfo>* pods) {
    if (pods == NULL) {
        return -1; 
    }
    std::map<std::string, PodInfo>::iterator pod_it = 
        pods_.begin();
    for (; pod_it != pods_.end(); ++pod_it) {
        pods->push_back(pod_it->second); 
    }
    return 0;
}

int PodsManager::DeletePod(const std::string& pod_id) {
    // async delete, only do delete to task_manager
    // pods_ erase by show pods
    std::map<std::string, PodInfo>::iterator pods_it = 
        pods_.find(pod_id);
    if (pods_it == pods_.end()) {
        LOG(WARNING, "pod %s already delete",
                pod_id.c_str()); 
        return 0;
    }
    PodInfo& pod_info = pods_it->second;
    std::map<std::string, TaskInfo>::iterator task_it = 
        pod_info.tasks.begin();
    for (; task_it != pod_info.tasks.end(); ++task_it) {
        int ret = task_manager_->DeleteTask(
                task_it->first);
        if (ret != 0) {
            LOG(WARNING, "delete task %s for pod %s failed",
                    task_it->first.c_str(),
                    pod_info.pod_id.c_str()); 
            return -1;
        }
    }
    return 0;
}

int PodsManager::UpdatePod(const std::string& /*pod_id*/, const PodInfo& /*info*/) {
    // TODO  not support yet
    return -1;
}

int PodsManager::AddPod(const PodInfo& info) {
    // NOTE locked by agent
    std::map<std::string, PodInfo>::iterator pods_it = 
        pods_.find(info.pod_id);
    // NOTE pods_manager should be do same 
    // when add same pod multi times
    if (pods_it != pods_.end()) {
        LOG(WARNING, "pod %s already added", info.pod_id.c_str());
        return 0; 
    }
    pods_[info.pod_id] = info;
    if (LanuchInitd(&pods_[info.pod_id]) != 0) {
        LOG(WARNING, "lanuch initd for %s failed",
                info.pod_id.c_str()); 
        return -1;
    }                    
    std::map<std::string, TaskInfo>::iterator task_it = 
        pods_[info.pod_id].tasks.begin();
    for (; task_it != info.tasks.end(); ++task_it) {
        int ret = task_manager_->CreateTask(task_it->second);
        if (ret != 0) {
            LOG(WARNING, "create task ind %s for pods %s failed",
                    task_it->first.c_str(), info.pod_id.c_str()); 
            return -1;
        }
    }
    return 0; 
}

}   // ending namespace galaxy
}   // ending namespace baidu

/* vim: set ts=4 sw=4 sts=4 tw=100 */
