// Copyright (c) 2014, Baidu.com, Inc. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yuanyi03@baidu.com

#ifndef _LOG_WRITER_H
#define _LOG_WRITER_H

namespace galaxy {

class LogWriter {
public:
    explicit LogWriter(const std::string& log_file, uint64_t offset);
    ~LogWriter();

    bool AppendRecord(const LogRecord& record, LogRecordWriteState* state);

    uint64_t LastRecordOffset();
};

}   // ending namespace galaxy

#endif  //_LOG_WRITER_H

/* vim: set ts=4 sw=4 sts=4 tw=100 */
