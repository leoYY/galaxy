// Copyright (c) 2015, Galaxy Authors. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yuanyi03@baidu.com

#include <errno.h>
#include <string.h>

#include "persistence/log_writer.h"
#include "common/logging.h"

namespace galaxy {

static const int LOG_FLAGS = O_WRONLY | O_CREAT;
static const int LOG_MODE = S_IRWXU | S_IRWXG | S_IRWXO;

FileLogWriter::FileLogWriter(const std::string& file_name)
    : log_file_(file_name), 
      file_offset_(0),
      log_fd_(-1) {
}

FileLogWriter::~FileLogWriter() {
    Close();
}

bool FileLogWriter::Open() {
    log_fd_ = open(log_file_.c_str(), LOG_FLAGS, LOG_MODE);
    if (log_fd_ == -1) {
        LOG(FATAL, "open log file failed[%s] err[%d: %s]",
                log_file_.c_str(),
                errno,
                strerror(errno));     
        return false;
    }
    file_offset_ = 0;
    return true;
}

void FileLogWriter::Close() {
    if (fd > 0) {
        close(fd); 
        fd = -1;
    }
}

bool FileLogWriter::AppendRecord(const char* buffer, size_t size) {
    if (buffer == NULL) {
        LOG(WARNING, "record buffer is null"); 
        return false;
    }    

    if (log_fd_ == -1) {
        LOG(WARNING, "record log fd not opened"); 
        return false;
    }

    size_t writterned_len = 0;
    size_t len = write(log_fd_, (char*)&size, sizeof(size));
    if (len == -1) {
        LOG(WARNING, "record write data size failed err[%d: %s]",
                errno,
                strerror(errno)); 
        return false;
    }

    writterned_len += len;
    len = 0;
    len = write(log_fd_, buffer, size);
    if (len == -1) {
        LOG(WARNING, "record write data content failed err[%d: %s]",
                errno,
                strerror(errno)); 
        return false;
    }

    writerned_len += len;

    if (0 != fsync(log_fd_)) {
        LOG(WARNING, "record sync data failed err[%d: %s]",
                errno,
                strerror(errno));     
        return false;
    }

    file_offset_ += writerned_len;
    LOG(DEBUG, "record write success all[%lu] content[%lu]", 
            writerned_len, size);
    return true;
}

}   // ending namespace galaxy

/* vim: set ts=4 sw=4 sts=4 tw=100 */
