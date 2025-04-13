package org.example;

import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Serializable;

@Plugin(name = "KafkaAppender", category = "Core", elementType = "appender", printObject = true)
public class KafkaAppender extends AbstractAppender {
    private final KafkaManager manager;

    protected KafkaAppender(String name, Filter filter, Layout<? extends Serializable> layout,
                            boolean ignoreExceptions, KafkaManager manager) {
        super(name, filter, layout, ignoreExceptions);
        this.manager = manager;
    }

    @PluginFactory
    public static KafkaAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginAttribute("topic") String topic,
            @PluginAttribute("bootstrapServers") String bootstrapServers,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") Filter filter,
            @PluginAttribute("ignoreExceptions") boolean ignoreExceptions
    ) {
        if (name == null) {
            LOGGER.error("No name provided for KafkaAppender");
            return null;
        }
        if (layout == null) {
            layout = PatternLayout.createDefaultLayout();
        }

        KafkaManager manager = new KafkaManager(bootstrapServers, topic);
        return new KafkaAppender(name, filter, layout, ignoreExceptions, manager);
    }

    @Override
    public void append(LogEvent event) {
        byte[] data = getLayout().toByteArray(event);
        manager.send(data);
    }
}
