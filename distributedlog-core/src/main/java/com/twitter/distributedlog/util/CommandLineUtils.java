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
