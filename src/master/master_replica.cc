// Copyright (c) 2015, Galaxy Authors. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yuanyi03@baidu.com

#include "master/master_replica.h"
#include "proto/master_replica.pb.h"
#include "common/logging.h"
#include "common/timer.h"

extern int64_t FLAGS_max_election_timeout;
extern int64_t FLAGS_max_heartbeat_timeout;
extern std::string FLAGS_localhost_addr;
extern int32_t FLAGS_max_replicate_length;

namespace galaxy {

MasterReplicaImpl::MasterReplicaImpl(const std::vector<std::string>& server_addrs) 
        : current_term_(0),
          vote_for_(), 
          vote_grant_(),
          logs_(),
          current_log_index_(0),
          commited_index_(0),
          last_applied_index_(0),
          next_index_(),
          match_index_(),
          current_role_state_(kRoleStateFollower),
          members_(), 
          next_election_time_(0),
          max_election_timeout_(0),
          max_heartbeat_timeout_(0),
          backgroud_threads_(2 + server_addrs.size()),
          lock_(),
          replicate_cond_(&lock_),
          applied_cond_(&lock_),
          rpc_client_(NULL),
          stop_(false) {
    rpc_client_ = new RpcClient();
    for (int server_ind = 0; 
            server_ind < server_addrs_.size(); 
                ++server_ind) {
        members_[server_ind] = server_addrs[server_ind];
        std::string& server_addr = server_addrs[server_ind];
        next_index_[server_addr] = 0;
        match_index_[server_addr] = 0;
    }

    max_election_timeout_ = FLAGS_max_election_timeout;
    next_election_time_ = max_election_timeout_ + timer::now_time();
    next_heartbeat_time_ = timer::now_time() + max_election_timeout_ / 2; 
    backgroud_threads_.DelayTask(max_election_timeout_, 
            boost::bind(&MasterReplicaImpl::ElectionTimeoutCheck, this));
    backgroud_threads_.AddTask(boost::bind(
                &MasterReplicaImpl::ApplyLogEntry, this));
}

MasterReplicaImpl::~MasterReplicaImpl() {
    stop_ = true;
}

void MasterReplicaImpl::ApplyLogEntry() {
    while (!stop_) {
        MutexLock scope_lock(&lock_); 
        while (last_applied_index_ == commited_index_
                && !stop_) {
            applied_cond_.TimeWait(500);
        }

        if (stop_) {
            break; 
        }

        assert(last_applied_index_ < commited_index_);
        while (last_applied_index_ <= commited_index_) {
            // do Apply 

            last_applied_index_ ++;
        }    
    }
}

void MasterReplicaImpl::ElectionTimeoutCheck() {
    int64_t now_time = timer::now_time();
    if (next_election_time_ > now_time) {
        backgroud_threads_.DelayTask(
                next_election_time_ - now_time, 
                boost::bind(&MasterReplicaImpl::ElectionTimeoutCheck, this));
        return;
    }

    MutexLock scope_lock(&lock_);
    int64_t new_election_timeout = max_election_timeout_;
    if (current_role_state_ != kRoleStateLeader) {
        IncrementTerm();
        SetRoleState(kRoleStateCandidate);
        VoteForSelf();
        ElectionTimeoutValue(&new_election_timeout);
    }
    backgroud_threads_.DelayTask(
            new_election_timeout, 
            boost::bind(&MasterReplicaImpl::ElectionTimeoutCheck, this));
    return;
}

void MasterReplicaImpl::VoteForSelfCallback(std::string member, 
                                            const RequestVoteRequest* request,
                                            RequestVoteResponse* response,
                                            bool failed,
                                            int err_code) {
    if (failed) {
        LOG(WARNING, "VoteForSelf call %s back failed err %d", 
                member.c_str(), err_code); 
    } else {
        MutexLock scope_lock(&lock_); 
        int64_t resp_term = response->term();
        if (current_role_state_ == kRoleStateCandidate) {
            if (current_term_ >= resp_term 
                    && response->vote_granted()) {
                LOG(DEBUG, "VoteForSelf %s vote Me", member.c_str());
                vote_grant_[current_term_] ++;
                if (vote_grant_[current_term_] > members_.size() / 2) {
                    LOG(INFO, "VoteForSelf vote for %s win on term %ld", 
                            FLAGS_localhost_addr.c_str(), current_term_); 
                    SetRoleState(kRoleStateLeader);
                }
            } else {
                LOG(WARNING, "VoteForSelf %s don't vote Me. "
                        "[My Term: %ld, His Term: %ld, vote grant: %s]", 
                        member.c_str(), current_term_, resp_term, 
                        response->vote_granted() ? "true" : "false"); 
                if (current_term_ < resp_term) {
                    LOG(WARNING, "VoteForSelf switch to Follower for %s", 
                            member.c_str())
                    current_term_ = resp_term;
                    vote_for_[current_term_] = member;
                    SetRoleState(kRoleStateFollower);
                }
            }       
        }    
    }

    if (request != NULL) {
        delete request; 
    }

    if (response != NULL) {
        delete response; 
    }
}

void MasterReplicaImpl::VoteForSelf() {
    if (current_role_state_ != kRoleStateCandidate) {
        return; 
    }
    // init for vote for self
    vote_for_ = FLAGS_localhost_addr;
    vote_grant_[current_term_] = 0;
    for (int mem_ind = 0; mem_ind < members_.size(); ++mem_ind) {
        MasterReplica_Stub* member_stub = NULL; 
        bool ret = rpc_client_->GetStub(members_[mem_ind], &member_stub);
        if (!ret) {
            LOG(WARNING, "members[%s] connect failed", 
                    members_[mem_ind].c_str()); 
            continue;
        }

        RequestVoteRequest* request = new RequestVoteRequest();
        request->set_term(current_term_);
        request->set_candidate_addr(FLAGS_localhost_addr);
        // NOTE
        request->last_log_index = current_log_index_;
        request->last_log_term = logs_[current_log_index_].term();
        RequestVoteResponse* response = new RequestVoteResponse();
        boost::function<void(
                const RequestVoteRequest*, 
                RequestVoteResponse*, 
                bool, int)> callback = boost::bind(
                    &MasterReplicaImpl::VoteForSelfCallback, 
                    this, members_[mem_ind], _1, _2, _3, _4);
        // do async rpc 
        rpc_client->AsyncRequest(member_stub, 
                                 &MasterReplica_Stub::RequestVote, 
                                 request, response, callback, 5, 1);
    }
}

void MasterReplicaImpl::SetRoleState(const RoleState& state) {
    RoleState last_state = current_role_state_;
    current_role_state_ = state;
    LOG(DEBUG, "convert server role state from [%s] to [%s]",
            RoleState_Name(last_state).c_str(),
            RoleState_Name(current_role_state_).c_str());
}

void MasterReplicaImpl::ElectionTimeoutValue(int64_t* timeout_val) {
    if (timeout_val == NULL) {
        return; 
    }
    
    switch (current_role_state_) {
    case kRoleStateCandidate: 
        // TODO random [max_election_timeout_/2, max_election_timeout_)
        *timeout_val = max_election_timeout_ / 2;
        break;
    default:
        *timeout_val = max_election_timeout_;
    }

    return;
}

void MasterReplicaImpl::AppendEntries(
        ::google::protobuf::RpcController* controller,
        const ::galaxy::AppendEntriesRequest* request,
        ::galaxy::AppendEntriesResponse* response,
        ::google::protobuf::Closure* done) {
    response->set_seq_id(request->seq_id()); 
    int64_t request_term = request->term();
    response->set_term(current_term_);     

    if (request_term < current_term_) {
        response->set_success(false);
        done->Run();
    }

    MutexLock scope_lock(&lock_); 
    UpdateNextElectionTime();
    if (request_term > current_term_) {
        current_term_ = request_term; 
        SetRoleState(kRoleStateFollower);
        vote_for_ = request->leader_addr(); 
        response->set_term(current_term_);
        response->set_success(false);
        done->Run();
    }

    if (current_log_index_ < request->prev_log_index()) {
        response->set_success(false);
        done->Run();
        return;
    }

    Entry& log_entry = logs_[request->prev_log_index()]; 
    if (log_entry.term() != request->prev_log_term()) {
        // delete logs from prev_log_index
        current_log_index_ = request->prev_log_index() - 1;
        response->set_success(false);
    } else {
        int64_t new_entries_size = request->entries_size();
        for (int64_t new_entries_index = 0;
                new_entries_index < new_entries_size;
                    ++new_entries_index) {
            // TODO to persistence  
            logs_[++current_log_index_] = 
                request->entiries(new_entries_index); 
        }
        response->set_success(true);
        if (request->leader_commit() > commited_index_) {
            commited_index_ = 
                current_log_index_ > request->leader_commit() ? 
                    request->leader_commit : current_log_index_;
        }
    }

    done->Run(); 
    return;
}

void MasterReplicaImpl::RequestVote(
        ::google::protobuf::RpcController* controller,
        const ::galaxy::RequestVoteRequest* request,
        ::galaxy::RequestVoteResponse* response,
        ::google::protobuf::Closure* done) {
    response->set_seq_id(request->seq_id()); 
    int64_t voter_term = request->term();
    response->set_term(current_term_);
    if (voter_term < current_term_) {
        LOG(WARNING, "voter %s term[%ld] is out-of"
                " date than current[%ld]",
                request->candidate_addr().c_str(),
                voter_term,
                current_term_);
        response->set_vote_granted(false);
        done->Run();
        return;
    }

    if (voter_term > current_term_) {
        LOG(INFO, "current term[%ld] is smaller than "
                "voter term[%ld]",
                current_term_, voter_term);
        {
            MutexLock scope_lock(&lock_);
            current_term_ = voter_term; 
            SetRoleState(kRoleStateFollower);
            vote_for_ = request->candidate_addr();
        }
        response->set_term(current_term_);
        response->set_vote_granted(true);
        done->Run();
        return;
    }
    if (vote_for_.empty() 
            || vote_for_ == request->candidate_addr()) {
        if (IsLogEntityMoreUpToDate(request->last_log_term(), 
                    request->last_log_index())) {
            LOG(DEBUG, "voter %s can be voted", 
                    request->candidate_addr().c_str());
            {
                MutexLock scope_lock(&lock_);
                vote_for_ = request->candidate_addr();
            }
            response->set_vote_granted(true);
            done->Run();
            return;
        }
    }
    response->set_vote_granted(false);
    done->Run();
    return;
}

void MasterReplicaImpl::ReplicateLog(const std::string& follower_addr) {
    
    while (!stop_) {
        MutexLock scope_lock(&lock_);
        if (current_role_state_ != kRoleStateLeader) {
            break; 
        }
        replicate_cond_.TimeWait(100); 

        if (current_role_state_ != kRoleStateLeader
                    || stop_) {
            break; 
        }

        int64_t prev_log_index = next_index_[follower_addr] - 1;
        assert(prev_log_index <= current_log_index_);
        int64_t prev_log_term = logs_[prev_log_index].term(); 
        int64_t term = current_term_;
        int64_t commited_index = commited_index_;

        // TODO unlock rpc 
        MasterReplica_Stub* member_stub = NULL;
        bool ret = rpc_client->GetStub(follower_addr, 
                                       &member_stub);
        if (!ret) {
            LOG(WARNING, "ReplicateLog for %s connect failed", 
                    follower_addr.c_str()); 
            continue;
        }

        AppendEntriesRequest request;
        AppendEntriesResponse response;
        request->set_term(term);
        request->set_leader_addr(FLAGS_localhost_addr);
        request->set_prev_log_index(prev_log_index);
        request->set_prev_log_term(prev_log_term);
        request->set_leader_commit(commited_index);
         
        int64_t log_index_end = current_log_index_;
        if (log_index_end - prev_log_index + 1 
                > FLAGS_max_replicate_length) {
            log_index_end = prev_log_index + 1 
                + FLAGS_max_replicate_length; 
        }
        for (int64_t send_log_index = prev_log_index + 1;
                send_log_index <= log_index_end;
                    ++send_log_index) {
            Entry* entry = request->add_entries();
            entry->CopyFrom(logs_[send_log_index]);
        }
        
        ret = rpc_client_->SendRequest(member_stub, 
                &MasterReplica_Stub::AppendEntries, 
                &request, &response, 5, 1); 
        if (!ret) {
            LOG(WARNING, "ReplicateLog for %s send failed", 
                    follower_addr.c_str()); 
            continue;
        }

        if (response.term() > current_term_) {
            LOG(INFO, "ReplicateLog for %s term[%ld] "
                    "more than current[%ld], follow him",
                    follower_addr.c_str(),
                    response.term(),
                    current_term_);
            current_term_ = response.term();
            SetRoleState(kRoleStateFollower); 
            break;
        }

        if (response.success()) {
            next_index_[follower_addr] = log_index_end + 1; 
            match_index_[follower_addr] = 
                next_index_[follower_addr] - 1;
        } else {
            next_index_[follower_addr] -= 1; 
        }
    }    
}


}   // ending namespace galaxy

/* vim: set ts=4 sw=4 sts=4 tw=100 */

