// Copyright (c) 2015, Galaxy Authors. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yuanyi03@baidu.com

#include "utils/message_writer.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

#include "common/logging.h"

namespace galaxy {

static int OPEN_FLAGS = O_CREAT | O_WRONLY;
static int OPEN_MODE = S_IRUSR | S_IWUSR | S_IRGRP | S_IRWXO;

MessageWriter::MessageWriter(const std::string& message_file) 
    : output_stream_(NULL),
      message_file_(message_file),
      message_file_fd_(-1),
      err_code_(MSG_WRITER_OK) {
    message_file_fd_ = open(message_file_.c_str(), OPEN_FLAGS, OPEN_MODE);
    if (message_file_fd_ == -1) {
        err_code_ = MSG_WRITER_OPEN_ERROR;      
        LOG(WARNING, "open file %s failed err[%d: %s]",
                message_file_.c_str(),
                errno,
                strerror(errno));
    } else {
        output_stream_ = 
            new google::protobuf::io::ZeroCopyOutputStream(message_file_fd_); 
    }
}

MessageWriter::~MessageWriter() {
    if (output_stream_ != NULL) {
        delete output_stream_; 
        output_stream_ = NULL;
    }

    if (message_file_fd_ > 0) {
        close(message_file_fd_); 
        message_file_fd_ = -1;
    }
}

bool MessageWriter::Write(const google::Message& msg,
        MessageWriterErrorCodes* err_codes) {
    
}


}   // ending namespace galaxy

/* vim: set ts=4 sw=4 sts=4 tw=100 */
