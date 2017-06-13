package org.jlab.mya.jmyapi;

import java.io.IOException;
import java.nio.channels.Channel;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;

/**
 *
 * @author ryans
 * @param <T>
 */
public class PvRecordStream<T> implements Channel {

    private final Statement stmt;
    private final ResultSet rs;
    private final Class<T> type;
    private final int recordSize;

    protected PvRecordStream(Statement stmt, ResultSet rs, Class<T> type, int recordSize) {
        this.stmt = stmt;
        this.rs = rs;
        this.type = type;
        this.recordSize = recordSize;
    }

    @Override
    public boolean isOpen() {
        try {
            return !rs.isClosed();
        } catch (SQLException e) {
            return false;
        }
    }

    public PvRecord<T> read() throws IOException {
        try {
            if (rs.next()) {
                return rowToEntity();
            } else {
                return null;
            }
        } catch (SQLException e) {
            throw new IOException("Unable to read", e);
        }
    }

    @SuppressWarnings("unchecked")
    private PvRecord<T> rowToEntity() throws SQLException {
        Instant time = MyaUtil.fromMyaTimestamp(rs.getLong(1));
        int codeOrdinal = rs.getInt(2);
        PvEventType code = PvEventType.values()[codeOrdinal];
        T value;

        if (!type.isArray() && "Float".equals(type.getSimpleName())) {
            value = (T)Float.valueOf(rs.getFloat(3));
        } else {
            Class componentType = type.getComponentType();
            if (componentType == null) {
                componentType = type;
            }
            ResultExtractor extractor = getExtractor(componentType);
            Object[] values = new Object[recordSize];

            int offset = 3;

            for (int i = 0; i < recordSize; i++) {
                values[i] = extractor.get(i + offset);
            }
            value = (T)values;
        }

        return new PvRecord<>(time, code, value);
    }

    @Override
    public void close() throws IOException {
        try {
            //rs.close();
            stmt.close();
            /*Closing Statement closes the ResultSet too*/
        } catch (SQLException e) {
            throw new IOException("Unable to close SpanOutputChannel", e);
        }
    }

    /*public Iterator<PvRecord> iterator() {
        // Don't mix iterator and read calls or bad things happen...
        return new Iterator<PvRecord>() {
            private boolean didNext = false;
            private boolean hasNext = false;

            @Override
            public boolean hasNext() {
                if (!didNext) {
                    try {
                        hasNext = rs.next();
                    } catch (SQLException e) {
                        throw new RuntimeException("Unable to iterate", e);
                    }
                    didNext = true;
                }
                return hasNext;
            }

            @Override
            public PvRecord next() {
                try {
                    if (!didNext) {
                        rs.next();
                    }
                    didNext = true;
                    return rowToEntity();
                } catch (SQLException e) {
                    throw new RuntimeException("Unable to iterate", e);
                }
            }
        };
    }*/
    private ResultExtractor getExtractor(Class componentType) throws SQLException {
        ResultExtractor extractor;

        switch (componentType.getSimpleName()) {
            case "String":
                extractor = new ResultExtractor(rs) {
                    @Override
                    public Object get(int column) throws SQLException {
                        return rs.getString(column);
                    }
                };
                break;
            case "Short":
                extractor = new ResultExtractor(rs) {
                    @Override
                    public Object get(int column) throws SQLException {
                        return rs.getShort(column);
                    }
                };
                break;
            case "Float":
                extractor = new ResultExtractor(rs) {
                    @Override
                    public Object get(int column) throws SQLException {
                        return rs.getFloat(column);
                    }
                };
                break;
            case "Integer":
                extractor = new ResultExtractor(rs) {
                    @Override
                    public Object get(int column) throws SQLException {
                        return rs.getInt(column);
                    }
                };
                break;
            case "Long":
                extractor = new ResultExtractor(rs) {
                    @Override
                    public Object get(int column) throws SQLException {
                        return rs.getLong(column);
                    }
                };
                break;
            default:
                throw new SQLException("Unknown type: " + type);
        }

        return extractor;
    }

    private abstract class ResultExtractor {

        protected ResultSet rs;

        public ResultExtractor(ResultSet rs) {
            this.rs = rs;
        }

        public abstract Object get(int column) throws SQLException;
    }
}
