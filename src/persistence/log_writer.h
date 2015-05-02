// Copyright (c) 2015, Galaxy Authors. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yuanyi03@baidu.com

#ifndef _LOG_WRITER_H
#define _LOG_WRITER_H

namespace galaxy {

class LogWriter {
public:
    virtual ~LogWriter() {}
    virtual bool Open() = 0;
    virtual void Close() = 0;
    virtual bool AppendRecord(const char* buffer, size_t size) = 0;
};

class FileLogWriter : public LogWriter {
public:
    FileLogWriter(const std::string& file_name);
    virtual ~FileLogWriter();
    virtual bool Open();
    virtual void Close();
    virtual bool AppendRecord(const char* buffer, size_t size);

private:
    std::string log_file_;
    int64_t file_offset_;
    int log_fd_;
};

}   // ending namespace galaxy

#endif  //PS_SPI_LOG_WRITER_H

/* vim: set ts=4 sw=4 sts=4 tw=100 */
