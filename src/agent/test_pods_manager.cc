// Copyright (c) 2015, Baidu.com, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#include "agent/pods_manager.h"
#include <gflags/gflags.h>

static baidu::galaxy::PodsManager pods_manager;

void TestLanuchInitd() {
    int ret = pods_manager.LanuchInitd(8989, "hehe");
    if (ret != 0) {
        fprintf(stderr, "LanuchInitd failed\n");
        return;
    } 
    fprintf(stdout, "LanuchInitd success");
}

int main(int argc, char* argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    TestLanuchInitd();
}


/* vim: set ts=4 sw=4 sts=4 tw=100 */
