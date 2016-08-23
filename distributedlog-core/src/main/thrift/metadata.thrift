/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
namespace java com.twitter.distributedlog.thrift

struct BKDLConfigFormat {
    1: optional string bkZkServers
    2: optional string bkLedgersPath
    3: optional bool sanityCheckTxnID
    4: optional bool encodeRegionID
    5: optional string bkZkServersForReader
    6: optional string dlZkServersForWriter
    7: optional string dlZkServersForReader
    8: optional string aclRootPath
    9: optional i64 firstLogSegmentSeqNo
    10: optional bool federatedNamespace
}

struct AccessControlEntry {
    1: optional bool denyWrite
    2: optional bool denyTruncate
    3: optional bool denyDelete
    4: optional bool denyAcquire
    5: optional bool denyRelease
}
