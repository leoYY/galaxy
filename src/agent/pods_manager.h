// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef _PODS_MANAGER_H
#define _PODS_MANAGER_H

#include "agent/agent_internal_infos.h"

#include <string>
#include <vector>

#include "proto/galaxy.pb.h"
#include "mutex.h"

namespace baidu {
namespace galaxy {

class TaskManager;

class PodsManager {
public:
    PodsManager();
    virtual ~PodsManager();

    int Init();

    int AddPod(const PodInfo& info);
    int DeletePod(const std::string& pod_id);
    int UpdatePod(const std::string& pod_id, const PodInfo& info);
    int ShowPods(std::vector<PodInfo>* pods);
    void CheckPod(const std::string& pod_id);
protected:
    int LanuchInitd(PodInfo* info); 
    //Mutex pods_lock_;
    std::map<std::string, PodInfo> pods_;
    TaskManager* task_manager_;
};

}   // ending namespace galaxy
}   // ending namespace baidu

#endif  //_PODS_MANAGER_H

/* vim: set ts=4 sw=4 sts=4 tw=100 */
