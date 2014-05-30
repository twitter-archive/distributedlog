package com.twitter.distributedlog.tools;

import org.apache.bookkeeper.util.ReflectionUtils;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.util.Map;
import java.util.TreeMap;

/**
 * A Tool Framework
 */
public abstract class Tool {

    /**
     * Interface of a command to run in a tool.
     */
    protected interface Command {
        String getName();
        String getDescription();
        int runCmd(String[] args) throws Exception;
        void printUsage();
    }

    /**
     * {@link org.apache.commons.cli.Options} based command.
     */
    protected abstract class OptsCommand implements Command {

        /**
         * @return options used by this command.
         */
        protected abstract Options getOptions();

        /**
         * @return usage of this command.
         */
        protected abstract String getUsage();

        /**
         * Run given command line <i>commandLine</i>.
         *
         * @param commandLine
         *          command line to run.
         * @return return code of this command.
         * @throws Exception
         */
        protected abstract int runCmd(CommandLine commandLine) throws Exception;

        protected String cmdName;
        protected String description;

        protected OptsCommand(String name, String description) {
            this.cmdName = name;
            this.description = description;
        }

        @Override
        public String getName() {
            return cmdName;
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public int runCmd(String[] args) throws Exception {
            try {
                BasicParser parser = new BasicParser();
                CommandLine cmdline = parser.parse(getOptions(), args);
                return runCmd(cmdline);
            } catch (ParseException e) {
                printUsage();
                return -1;
            }
        }

        @Override
        public void printUsage() {
            HelpFormatter helpFormatter = new HelpFormatter();
            println(cmdName + ": " + getDescription());
            helpFormatter.printHelp(getUsage(), getOptions());
        }
    }

    class HelpCommand implements Command {

        @Override
        public String getName() {
            return "help";
        }

        @Override
        public String getDescription() {
            return "describe the usage of this tool or its sub-commands.";
        }

        @Override
        public int runCmd(String[] args) throws Exception {
            if (args.length == 0) {
                printToolUsage();
                return -1;
            }
            String cmdName = args[0];
            Command command = commands.get(cmdName);
            if (null == command) {
                println("Unknown command " + cmdName);
                printToolUsage();
                return -1;
            }
            command.printUsage();
            println("");
            return 0;
        }

        @Override
        public void printUsage() {
            println(getName() + ": " + getDescription());
            println("");
            println("usage: " + getName() + " <command>");
        }
    }

    // Commands managed by a tool
    protected final Map<String, Command> commands =
            new TreeMap<String, Command>();

    protected Tool() {
        addCommand(new HelpCommand());
    }

    /**
     * @return tool name.
     */
    protected abstract String getName();

    /**
     * Add a command in this tool.
     *
     * @param command
     *          command to run in this tool.
     */
    protected void addCommand(Command command) {
        commands.put(command.getName(), command);
    }

    /**
     * Print a message in this tool.
     *
     * @param msg
     *          message to print
     */
    protected void println(String msg) {
        System.err.println(msg);
    }

    /**
     * print tool usage.
     */
    protected void printToolUsage() {
        println("Usage: " + getName() + " <command>");
        println("");
        int maxKeyLength = 0;
        for (String key : commands.keySet()) {
            if (key.length() > maxKeyLength) {
                maxKeyLength = key.length();
            }
        }
        maxKeyLength += 2;
        for (Map.Entry<String, Command> entry : commands.entrySet()) {
            String spaces = "";
            int numSpaces = maxKeyLength - entry.getKey().length();
            for (int i = 0; i < numSpaces; i++) {
                spaces += " ";
            }
            println("\t"  + entry.getKey() + spaces + ": " + entry.getValue().getDescription());
        }
        println("");
    }

    public int run(String[] args) throws Exception {
        if (args.length <= 0) {
            printToolUsage();
            return -1;
        }
        String cmdName = args[0];
        Command cmd = commands.get(cmdName);
        if (null == cmd) {
            println("ERROR: Unknown command " + cmdName);
            printToolUsage();
            return -1;
        }
        // prepare new args
        String[] newArgs = new String[args.length - 1];
        System.arraycopy(args, 1, newArgs, 0, newArgs.length);
        return cmd.runCmd(newArgs);
    }

    public static void main(String args[]) {
        if (args.length <= 0) {
            System.err.println("No tool to run.");
            System.err.println("");
            System.err.println("Usage : Tool <tool_class_name> <options>");
            System.exit(-1);
        }
        String toolClass = args[0];
        try {
            Tool tool = ReflectionUtils.newInstance(toolClass, Tool.class);
            String[] newArgs = new String[args.length - 1];
            System.arraycopy(args, 1, newArgs, 0, newArgs.length);
            tool.run(newArgs);
        } catch (Throwable t) {
            System.err.println("Fail to run tool " + toolClass + " : ");
            t.printStackTrace();
            System.exit(-1);
        }
    }
}
