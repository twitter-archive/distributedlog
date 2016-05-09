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
package com.twitter.distributedlog.util;

import com.google.common.base.Optional;
import org.apache.commons.cli.CommandLine;

/**
 * Utils to commandline
 */
public class CommandLineUtils {

    public static Optional<String> getOptionalStringArg(CommandLine cmdline, String arg) {
        if (cmdline.hasOption(arg)) {
            return Optional.of(cmdline.getOptionValue(arg));
        } else {
            return Optional.absent();
        }
    }

    public static Optional<Boolean> getOptionalBooleanArg(CommandLine cmdline, String arg) {
        if (cmdline.hasOption(arg)) {
            return Optional.of(true);
        } else {
            return Optional.absent();
        }
    }

    public static Optional<Integer> getOptionalIntegerArg(CommandLine cmdline, String arg) throws IllegalArgumentException {
        try {
            if (cmdline.hasOption(arg)) {
                return Optional.of(Integer.parseInt(cmdline.getOptionValue(arg)));
            } else {
                return Optional.absent();
            }
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(arg + " is not a number");
        }
    }

}
