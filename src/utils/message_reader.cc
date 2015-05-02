// Copyright (c) 2015, Galaxy Authors. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yuanyi03@baidu.com

#include "utils/message_reader.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

#include "google/protobuf/io/coded_stream.h"
#include "common/logging.h"

namespace galaxy {

MessageReader::MessageReader(const std::string& message_file)
    : input_stream_(NULL),
      message_file_(mesage_file),
      message_file_fd_(-1),
      err_code_(MSG_READER_OK) {
    message_file_fd_ = open(message_file_.c_str(), O_RDONLY);        
    if (message_file_fd_ == -1) {
        LOG(WARNING, "open message file %s failed err[%d: %s]",
                message_file.c_str(),
                errno,
                strerror(errno));
        err_code_ = MSG_READER_OPEN_ERROR; 
    }
    else {
        input_stream_ = 
            new google::protobuf::io::ZeroCopyInputStream(message_file_fd_);     
    }
}

MessageReader::~MessageReader() {
    if (input_stream_ != NULL) {
        delete input_stream_; 
        input_stream_ = NULL;
    }
    if (message_file_fd_ > 0) {
        close(message_file_fd_); 
        message_file_fd_ = -1;
    }
}

bool MessageReader::Read(google::Message* msg,
        MessageReaderErrorCodes* err_codes) {
    if (err_codes == NULL) {
        return false;
    }
    if (err_code_ != MSG_READER_OK) {
        *err_codes = err_code_; 
        return false;
    }
    if (msg == NULL) {
        *err_codes = MSG_READER_NULL_MESSAGE; 
        return false;
    }

    if (message_file_fd_ == -1 || input_stream_ == NULL) {
        *err_codes = MSG_READER_OTHERS; 
        return false;
    }

    uint32_t length;
    bool ret = false;
    google::protobuf::io::CodedInputStream coded_stream(input_stream_);
    if (coded_stream.ReadVarint32(&length)) {
        google::protobuf::io::CodedInputStream::Limit limit =  
            coded_stream.PushLimit(length);
        if (msg->ParseFromCodedStream(&coded_stream)) {
            *err_codes = MSG_READER_OK;
            ret = true;
        } else {
            LOG(WARNING, "message parse failed");
            *err_codes = MSG_READER_PARSE_MESSAGE_ERROR; 
            ret = false;
        }
        coded_stream.PopLimit(limit);
    } else {
        LOG(WARNING, "read length failed");  
        err_codes = MSG_RAEDER_EOF;
        ret = false;
    }

    return ret;
}

}   // ending namespace galaxy

/* vim: set ts=4 sw=4 sts=4 tw=100 */
