// Copyright (c) 2015, Galaxy Authors. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yuanyi03@baidu.com

#ifndef _MESSAGE_READER_H
#define _MESSAGE_READER_H

#include <string>

#include "google/protobuf/io/zero_copy_stream.h"

namespace galaxy {

enum MessageReaderErrorCodes {
    MSG_READER_OK = 0,
    MSG_READER_EOF = -1,
    MSG_READER_NULL_MESSAGE = -2,
    MSG_READER_PARSE_MESSAGE_ERROR = -3,
    MSG_READER_OPEN_ERROR = -4,
    MSG_READER_OTHERS = -5
};

class MessageReader {
public:
    explicit MessageReader(const std::string& message_file);

    bool Read(google::Message* msg, 
            MessageReaderErrorCodes* err_codes);

    ~MessageReader();
private:
    google::protobuf::io::ZeroCopyInputStream* input_stream_;
    std::string mesage_file_;
    int message_file_fd_;
    MessageReaderErrorCodes err_code_;
};

}   // ending namespace galaxy

#endif  //_MESSAGE_READER_H

/* vim: set ts=4 sw=4 sts=4 tw=100 */
