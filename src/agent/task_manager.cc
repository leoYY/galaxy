#include "task_manager.h"

namespace baidu {
namespace galaxy {

TaskManager::TaskManager() {
}

TaskManager::~TaskManager() {
}

int TaskManager::CreateTasks(const std::vector<TaskDesc>& tasks) {
    return 0;
}


int TaskManager::DeleteTasks(const std::vector<std::string>& taskid) {
    return 0;
}

int TaskManager::UpdateTaskCpuLimit(const std::string& taskid, 
                                    const uint32_t millicores) {
    return 0;
}

int QueryTasks(const std::vector<std::string>& taskid, 
               std::vector<TaskInfo>* tasks) {
    return 0;
}

int TaskManager::Execute(const std::string& command) {
    return 0;
}

int Update(const std::string& taskid, const uint32_t millicores) {
    return 0;
}

void TaskManager::LoopCheckTaskStatus() {
    
}

}
}
