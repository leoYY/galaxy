// Copyright (c) 2015, Galaxy Authors. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yuanyi03@baidu.com

#include <errno.h>
#include <string.h>
#include "persistence/log_reader.h"
#include "common/logging.h"

namespace galaxy {

static const int LOG_FLAGS = O_RDONLY;
static const int LOG_MODE = S_IRWXU | S_IRWXG | S_IRWXO;


FileLogReader::FileLogReader(const std::string& file_name)
    : log_file_(file_name),
      file_offset_(-1),
      log_fd_(-1) {
}

FileLogReader::~FileLogReader() {
    Close();
}

bool FileLogReader::Open() {
    log_fd_ = open(file_name.c_str(), LOG_FLAGS, LOG_MODE);
    if (log_fd_ == -1) {
        LOG(WARNING, "open log file failed[%s], err[%d: %s]",
                file_name.c_str(),
                errno,
                strerror(errno)); 
        return false;
    }
    file_offset_ = 0;
    return true;
}

void FileLogReader::Close() {
    if (log_fd_ != -1) {
        close(log_fd_); 
        log_fd_ = -1;
    }
}

bool FileLogReader::ReadRecord(char* buffer, size_t* size) {
    if (buffer == NULL) {
        LOG(WARNING, "read record buffer is null"); 
        return false;
    }

    size_t need_read = *size;
    size_t all_read = 0; 

    size_t content_size = 0;
    size_t len = read(log_fd_, &content_size, sizeof(content_size));
    if (len == -1) {
        LOG(WARNING, "read record size failed err[%d: %s]",
                errno,
                strerror(errno)); 
        return false;
    }

    if (content_size > *size) {
        LOG(WARNING, "read record buffer overflow"); 
        return false;
    }
    file_offset_ += len;

    len = read(log_fd_, buffer, content_size);
    if (len == -1) {
        LOG(WARNING, "read record data failed err[%d: %s]"
                errno,
                strerror(errno)); 
        return false;
    }

    file_offset_ += len
    *size = len;
    return true;
}


}   // ending namespace galaxy

/* vim: set ts=4 sw=4 sts=4 tw=100 */
