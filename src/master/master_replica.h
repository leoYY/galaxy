// Copyright (c) 2015, Galaxy Authors. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yuanyi03@baidu.com

#ifndef _MASTER_REPLICA_H
#define _MASTER_REPLICA_H

#include "proto/master_replica.pb.h"
#include "common/mutex.h"
#include "rpc/rpc_client.h"

namespace galaxy {

class MasterReplicaImpl : public MasterReplica {
public:
    explicit MasterReplicaImpl(const std::vector<std::string>& server_addrs);
    ~MasterReplicaImpl();

    void AppendEntries(::google::protobuf::RpcController* controller,
            const ::galaxy::AppendEntriesRequest* request,
            ::galaxy::AppendEntriesResponse* response,
            ::google::protobuf::Closure* done);

    void RequestVote(::google::protobuf::RpcController* controller,
            const ::galaxy::RequestVoteRequest* request,
            ::galaxy::RequestVoteResponse* response,
            ::google::protobuf::Closure* done);
private:
    void ApplyLogEntry();
    void VoteForSelfCallback(std::string member, 
                             const ::galaxy::RequestVoteRequest*,
                             ::galaxy::RequestVoteResponse*,
                             bool, int);
    // election
    void ElectionTimeoutCheck();
    void ElectionTimeoutValue(int64_t* timeout_val);

    void ReplicateLog(const std::string& follower_addr);
    // heartbeat check
    void SetRoleState(const RoleState& state);

    void VoteForSelf();
    void IncrementTerm() {
        ++current_term_;
    }

    void UpdateNextElectionTime() {
        ElectionTimeoutValue(&next_election_time_);
    }

    void UpdateNextHeartBeatTime() {
        // TODO 
        next_heartbeat_time_ = timer::now_time() + max_election_timeout_ /2;     
    }

    bool IsLogEntityMoreUpToDate(int64_t term, int64_t log_index) {
        int64_t last_log_index = logs_[logs_.size() - 1].log_index();
        int64_t last_log_term = logs_[logs_.size() - 1].term();
        if (last_log_term > term) {
            return false; 
        } else if (last_log_term < term) {
            return true;
        }

        if (last_log_index < log_index) {
            return true; 
        }
        return false;
    }
private:
    int64_t current_term_;    
    std::map<int64_t, std::string> vote_for_;
    std::vector<uint32_t> vote_grant_;

    std::vector<Entry> logs_;
    int64_t current_log_index_;

    int64_t commited_index_;        // last index of log be commited replica
    int64_t last_applied_index_;    // last index of log be persistence

    std::map<std::string, int64_t> next_index_;
    std::map<std::string, int64_t> match_index_;

    RoleState current_role_state_;
    std::vector<std::string> members_;

    int64_t next_election_time_;
    int64_t next_heartbeat_time_;
    int64_t max_election_timeout_;      // unit ms

    common::ThreadPool backgroud_threads_;
    common::Mutex lock_;
    common::CondVar replicate_cond_;
    common::CondVar applied_cond_;
    RpcClient* rpc_client_;
    bool stop_;
};

}   // ending namespace galaxy

#endif  //_MASTER_REPLICA_H

/* vim: set ts=4 sw=4 sts=4 tw=100 */
