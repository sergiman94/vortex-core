package com.vortex.distr.cmd;

import com.vortex.common.config.VortexConfig;
import com.vortex.common.config.OptionSpace;
import com.vortex.common.config.TypedOption;
import com.vortex.distr.RegisterUtil;
import com.vortex.common.util.E;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.TreeSet;

public class ConfDumper {

    public static final String EOL = System.getProperty("line.separator");

    public static void main(String[] args)
                       throws ConfigurationException, IOException {
        E.checkArgument(args.length == 1,
                        "ConfDumper need a config file.");

        String input = args[0];
        File output = new File(input + ".default");
        System.out.println("Input config: " + input);
        System.out.println("Output config: " + output.getPath());

        RegisterUtil.registerBackends();
        RegisterUtil.registerServer();

        VortexConfig config = new VortexConfig(input);

        for (String name : new TreeSet<>(OptionSpace.keys())) {
            TypedOption<?, ?> option = OptionSpace.get(name);
            writeOption(output, option, config.get(option));
        }
    }

    private static void writeOption(File output, TypedOption<?, ?> option,
                                    Object value) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("# ").append(option.desc()).append(EOL);
        sb.append(option.name()).append("=").append(value).append(EOL);
        sb.append(EOL);
        // Write to output file
        FileUtils.write(output, sb.toString(), true);
    }
}
