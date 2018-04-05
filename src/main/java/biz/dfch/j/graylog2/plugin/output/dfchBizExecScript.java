package biz.dfch.j.graylog2.plugin.output;

import java.io.*;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.inject.Inject;

import com.google.inject.assistedinject.Assisted;
import org.graylog2.plugin.configuration.Configuration;
import org.graylog2.plugin.configuration.ConfigurationRequest;
import org.graylog2.plugin.configuration.fields.BooleanField;
import org.graylog2.plugin.configuration.fields.ConfigurationField;
import org.graylog2.plugin.configuration.fields.TextField;
import org.graylog2.plugin.Message;
import org.graylog2.plugin.outputs.*;

import org.graylog2.plugin.streams.Stream;
import org.msgpack.annotation.NotNullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.util.Map;

/**
 * This is the plugin. Your class should implement one of the existing plugin
 * interfaces. (i.e. AlarmCallback, MessageInput, MessageOutput)
 */
public class dfchBizExecScript implements MessageOutput
{
    private static final String DF_PLUGIN_NAME = "d-fens SCRIPT Output";
    private static final String DF_DISPLAY_SCRIPT_OUTPUT = "DF_DISPLAY_SCRIPT_OUTPUT";
    private static final String DF_SCRIPT = "DF_SCRIPT";
    private static String findAndReplaceAll( String pattern, String replaceWith, String inputString)	{
        Pattern p = Pattern.compile(pattern);
        Matcher matcher = p.matcher(inputString);
        return matcher.replaceAll(replaceWith);
    }
    
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private Configuration configuration;
    private String streamTitle;
    private String _script;


    private static final Logger LOG = LoggerFactory.getLogger(dfchBizExecScript.class);

    @Inject
    public dfchBizExecScript
            (
                    @NotNullable @Assisted Stream stream,
                    @NotNullable @Assisted Configuration configuration
            )
            throws MessageOutputConfigurationException
    {
        try
        {
            LOG.debug("Verifying configuration ...");

            this.configuration = configuration;

            streamTitle = stream.getTitle();
            if(null == streamTitle || streamTitle.isEmpty())
            {
                throw new MessageOutputConfigurationException(String.format("streamTitle: Parameter validation FAILED. Value cannot be null or empty."));
            }
            
            LOG.trace("DF_SCRIPT                : %s\r\n", configuration.getString("DF_SCRIPT"));
            LOG.trace("DF_DISPLAY_SCRIPT_OUTPUT : %b\r\n", configuration.getBoolean("DF_DISPLAY_SCRIPT_OUTPUT"));
            
            _script = configuration.getString("DF_SCRIPT");

            isRunning.set(true);

            LOG.info("Verifying configuration SUCCEEDED.");
        }
        catch(Exception ex)
        {
            isRunning.set(false);

            LOG.error("*** " + DF_PLUGIN_NAME + "::write() - Exception\r\n" + ex.getMessage() + "\r\n");
            ex.printStackTrace();
        }
    }

    @Override
    public void stop()
    {
        isRunning.set(false);
    }

    @Override
    public boolean isRunning()
    {
        return isRunning.get();
    }

    @Override
    public void write(Message msg) throws Exception
    {
        String _run = _script;
        if(!isRunning.get())
        {
            return;
        }

        try
        {
			for (Map.Entry<String, Object> entry : msg.getFields().entrySet()) {
                /// replace keys with values in command
				_run = findAndReplaceAll("\\$\\{" + entry.getKey() + "\\}", entry.getValue().toString(), _run);
            }
            /// run command
            Runtime.getRuntime().exec(new String[]{"bash","-c",_run});

            if(configuration.getBoolean("DF_DISPLAY_SCRIPT_OUTPUT")) {
                LOG.info(String.format("%s\r\n", _run.toString()));
                LOG.trace("%s\r\n", _run.toString());
            }

        }
        catch(Exception ex)
        {
            LOG.error("*** " + DF_PLUGIN_NAME + "::write() - Exception");
            ex.printStackTrace();
        }
    }

    @Override
    public void write(List<Message> messages) throws Exception
    {
        if(!isRunning.get()) return;

        for(Message msg : messages)
        {
            write(msg);
        }
    }

    public static class Config extends MessageOutput.Config
    {
        @Override
        public ConfigurationRequest getRequestedConfiguration()
        {
            final ConfigurationRequest configurationRequest = new ConfigurationRequest();

            configurationRequest.addField(new TextField
                            (
                                    DF_SCRIPT
                                    ,
                                    "Script"
                                    ,
                                    "echo example"
                                    ,
                                    "Specify bash script to execute."
                                    ,
                                    ConfigurationField.Optional.NOT_OPTIONAL
                            )
            );
            configurationRequest.addField(new BooleanField
                            (
                                    DF_DISPLAY_SCRIPT_OUTPUT
                                    ,
                                    "Show script output"
                                    ,
                                    false
                                    ,
                                    "Show the script output on the console."
                            )
            );
            return configurationRequest;
        }
    }

    public interface Factory extends MessageOutput.Factory<dfchBizExecScript>
    {
        @Override
        dfchBizExecScript create(Stream stream, Configuration configuration);

        @Override
        Config getConfig();

        @Override
        Descriptor getDescriptor();
    }
    public static class Descriptor extends MessageOutput.Descriptor
    {
        public Descriptor()
        {
            super((new dfchBizExecScriptMetadata()).getName(), false, "", (new dfchBizExecScriptMetadata()).getDescription());
        }
    }
}

/**
 *
 *
 * Copyright 2015 Ronald Rink, d-fens GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
