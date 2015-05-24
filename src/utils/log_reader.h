// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yuanyi03@baidu.com

#ifndef _LOG_READER_H
#define _LOG_READER_H

#include <string>

#include "proto/log.pb.h"

namespace galaxy {

class LogReader {
public:
    explicit LogReader(const std::string& log_file, uint64_t offset);
    ~LogReader();

    bool ReadLog(LogRecord* record, LogRecordReadState* state);

    uint64_t LastRecordOffset();
};

}   // ending namespace galaxy

#endif  //_LOG_READER_H

/* vim: set ts=4 sw=4 sts=4 tw=100 */
