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
package com.twitter.distributedlog.service.balancer;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

class CountBasedStreamChooser implements StreamChooser, Serializable,
        Comparator<Pair<SocketAddress, LinkedList<String>>> {

    private static final long serialVersionUID = 4664153397369979203L;

    final List<Pair<SocketAddress, LinkedList<String>>> streamsDistribution;

    // pivot index in the list of hosts. the chooser will remove streams from the hosts before
    // pivot, which will reduce their stream counts to make them equal to the stream count of the pivot.
    int pivot;
    int pivotCount;

    // next index in the list of hosts to choose stream from.
    int next;

    CountBasedStreamChooser(Map<SocketAddress, Set<String>> streams) {
        Preconditions.checkArgument(streams.size() > 0, "Only support no-empty streams distribution");
        streamsDistribution = new ArrayList<Pair<SocketAddress, LinkedList<String>>>(streams.size());
        for (Map.Entry<SocketAddress, Set<String>> entry : streams.entrySet()) {
            LinkedList<String> randomizedStreams = new LinkedList<String>(entry.getValue());
            Collections.shuffle(randomizedStreams);
            streamsDistribution.add(Pair.of(entry.getKey(), randomizedStreams));
        }
        // sort the hosts by the number of streams in descending order
        Collections.sort(streamsDistribution, this);
        pivot = 0;
        pivotCount = streamsDistribution.get(0).getValue().size();
        findNextPivot();
        next = 0;
    }

    private void findNextPivot() {
        int prevPivotCount = pivotCount;
        while (++pivot < streamsDistribution.size()) {
            pivotCount = streamsDistribution.get(pivot).getValue().size();
            if (pivotCount < prevPivotCount) {
                return;
            }
        }
        pivot = streamsDistribution.size();
        pivotCount = 0;
    }

    @Override
    public synchronized String choose() {
        // reach the pivot
        if (next == pivot) {
            if (streamsDistribution.get(next - 1).getRight().size() > pivotCount) {
                next = 0;
            } else if (pivotCount == 0) { // the streams are empty now
                return null;
            } else {
                findNextPivot();
                next = 0;
            }
        }

        // get stream count that next host to choose from
        LinkedList<String> nextStreams = streamsDistribution.get(next).getRight();
        if (nextStreams.size() == 0) {
            return null;
        }

        String chosenStream = nextStreams.remove();
        ++next;
        return chosenStream;
    }

    @Override
    public int compare(Pair<SocketAddress, LinkedList<String>> o1,
                       Pair<SocketAddress, LinkedList<String>> o2) {
        return o2.getValue().size() - o1.getValue().size();
    }
}
