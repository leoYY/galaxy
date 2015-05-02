// Copyright (c) 2015, Galaxy Authors. All Rights Reserved
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.
//
// Author: yuanyi03@baidu.com

#ifndef _LOG_READER_H
#define _LOG_READER_H

#include <string>

namespace galaxy {

class LogReader {
public:
    virtual ~LogReader() {}
    virtual bool Open() = 0;
    virtual void Close() = 0;
    virtual bool ReadRecord(char* buffer, size_t* size) = 0; 
};

class FileLogReader : public LogReader {
public:
    FileLogReader(const std::string& file_name);    
    virtual ~FileLogReader();
    virtual bool Open();
    virtual void Close();
    // @TODO read has buffer
    virtual bool ReadRecord(char* buffer, size_t* size);
private:
    std::string log_file_;
    int64_t file_offset_;
    int log_fd_;
};


}   // ending namespace galaxy

#endif  //_LOG_READER_H

/* vim: set ts=4 sw=4 sts=4 tw=100 */
