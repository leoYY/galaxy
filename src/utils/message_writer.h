// Copyright (c) 2015, Galaxy Authors. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yuanyi03@baidu.com

#ifndef _MESSAGE_WRITER_H
#define _MESSAGE_WRITER_H

#include <string>
#include "google/protobuf/io/zero_copy_stream.h"

namespace galaxy {

enum MessageWriterErrorCodes {
    MSG_WRITER_OK = 0,
    MSG_WRITER_OPEN_ERROR = -1,
    MSG_WRITER_SERIALIZ_ERROR = -2,
    MSG_WRITER_OTHERS = -3
};

class MessageWriter {
public:
    explicit MessageWriter(const std::string& message_file);

    bool Write(const google::Message& msg, 
            MessageWriterErrorCodes* err_codes);

    ~MessageWriter();
private:
    google::protobuf::io::ZeroCopyOutputStream* output_stream_;
    std::string message_file_;
    int message_file_fd_;
    // keep err occur in contructure
    MessageWriterErrorCodes err_code_;
};

}   // ending namespace galaxy

#endif  //_MESSAGE_WRITER_H

/* vim: set ts=4 sw=4 sts=4 tw=100 */
