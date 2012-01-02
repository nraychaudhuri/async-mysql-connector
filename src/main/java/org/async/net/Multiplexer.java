package org.async.net;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.SelectorProvider;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.async.mysql.MysqlConnection;

public class Multiplexer {
    protected Selector selector;

    private Map<SelectionKey, MysqlConnection> processors = new HashMap<SelectionKey, MysqlConnection>();

    public Multiplexer() throws IOException {
        super();
        this.selector = SelectorProvider.provider().openSelector();
    }

    public void select() throws IOException {
        if ((selector.select()) > 0) {
            process();
        }

    }

    public void select(int timeout) throws IOException {
        if ((selector.select(timeout)) > 0) {
            process();
        }

    }

    public void registerProcessor(SelectionKey key, MysqlConnection processor) {
        processors.put(key, processor);
    }

    public void unregisterProcessor(SelectionKey key) {
        processors.remove(key);
    }

    public void close() throws SQLException {
       for (MysqlConnection value : processors.values()) {
          value.close();
       }
       processors.clear();
    }

    private void process() {
        Set<SelectionKey> keys = selector.selectedKeys();
        Iterator<SelectionKey> i = keys.iterator();
        while (i.hasNext()) {
            SelectionKey key = i.next();
            i.remove();
            if (!key.isValid()) {
                continue;
            }
            ChannelProcessor channelProcessor = processors.get(key);
            if(channelProcessor == null) {
                continue;
            }
            if (key.isAcceptable()) {
                channelProcessor.accept(key);
            } else if (key.isReadable()) {
                channelProcessor.read(key);
            } else if (key.isWritable()) {
                channelProcessor.write(key);
            } else if (key.isConnectable()) {
                channelProcessor.connect(key);
            }

        }
    }

    public Selector getSelector() {
        return selector;
    }
}
