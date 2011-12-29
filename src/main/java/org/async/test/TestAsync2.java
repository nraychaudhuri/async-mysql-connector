package org.async.test;

import java.sql.SQLException;

import org.async.jdbc.AsyncConnection;
import org.async.jdbc.Connection;
import org.async.jdbc.PreparedQuery;
import org.async.jdbc.PreparedStatement;
import org.async.jdbc.ResultSet;
import org.async.jdbc.ResultSetCallback;
import org.async.jdbc.SuccessCallback;
import org.async.mysql.MysqlConnection;
import org.async.mysql.protocol.packets.OK;
import org.async.net.Multiplexer;
import org.junit.Test;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

public class TestAsync2 {

    private final int TEST_DURATION = 120000;

    @Test
    public void singleThreadTest() throws IOException {

        final Random random = new Random();
        Multiplexer mpx = new Multiplexer();
        final List<MysqlConnection> cons = new ArrayList<MysqlConnection>();
        final AtomicBoolean stop = new AtomicBoolean(false);
        for (int i = 0; i < 32; i++) {
            final int idx = i;
            cons.add(new MysqlConnection("localhost", 3306, "root", "",
                    "async_mysql_test", mpx.getSelector(), new SuccessCallback() {

                        @Override
                        public void onError(SQLException e) {
                            e.printStackTrace();

                        }

                        @Override
                        public void onSuccess(OK ok) {
                            try {
                                final PreparedStatement ps = cons
                                        .get(idx)
                                        .prepareStatement(
                                                "select SQL_NO_CACHE * from test where id=?");

                                final PreparedQuery query = new PreparedQuery() {

                                    @Override
                                    public void query(PreparedStatement pstmt)
                                            throws SQLException {
                                        pstmt.setInteger(1,
                                                random.nextInt(1000000));

                                    }
                                };
                                final ResultSetCallback rsc = new ResultSetCallback() {
                                    int a = 0;

                                    @Override
                                    public void onError(SQLException e) {
                                        e.printStackTrace();

                                    }

                                    @Override
                                    public void onResultSet(ResultSet rs) {
                                        while (rs.hasNext()) {
                                            rs.next();
//                                            int id = rs.getInteger(1);
//                                            java.sql.Timestamp date = rs
//                                                    .getTimestamp(2);
//                                            int status = rs.getInteger(3);
//                                            String text = rs.getString(4);
                                        }

                                        try {
                                            if (!stop.get()) {
                                                ps.executeQuery(query, this);
                                            } else {
                                                cons.get(idx).close();
                                            }
                                        } catch (SQLException e) {

                                            e.printStackTrace();
                                        }
                                        a++;

                                    }
                                };
                                ps.executeQuery(query, rsc);
                            } catch (SQLException e1) {
                                e1.printStackTrace();
                            }
                        }
                    }));
        }

        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < TEST_DURATION) {
            mpx.select();

        }

        stop.set(true);
        for (int i = 0; i < 10; i++) {
            mpx.select(100);

        }
    }

    @Test
    public void multiThreadTest() throws IOException, InterruptedException {
        final ThreadSafeWrapper wrapper = new ThreadSafeWrapper(8);

        final Random random = new Random();

        for (int i = 0; i < 32; i++) {
            new Thread(new Runnable() {

                @Override
                public void run() {
                    try {
                        wrapper.q(new ThreadSafeWrapper.Query() {
                            PreparedStatement ps;
                            final PreparedQuery query = new PreparedQuery() {

                                @Override
                                public void query(PreparedStatement pstmt)
                                        throws SQLException {
                                    pstmt.setInteger(1, random.nextInt(1000000));

                                }
                            };
                            final ResultSetCallback rsc = new ResultSetCallback() {
                                int a = 0;

                                @Override
                                public void onError(SQLException e) {
                                    e.printStackTrace();

                                }

                                @Override
                                public void onResultSet(ResultSet rs) {
                                    while (rs.hasNext()) {
                                        rs.next();
//                                        int id = rs.getInteger(1);
//                                        java.sql.Timestamp date = rs
//                                                .getTimestamp(2);
//                                        int status = rs.getInteger(3);
//                                        String text = rs.getString(4);
                                    }

                                    try {
                                        ps.executeQuery(query, this);
                                    } catch (SQLException e) {

                                        e.printStackTrace();
                                    }
                                    a++;

                                }
                            };

                            @Override
                            public void doInConnection(MysqlConnection con) {
                                try {
                                    if (ps == null)
                                        ps = con.prepareStatement("select SQL_NO_CACHE * from test where id=?");
                                    ps.executeQuery(query, rsc);
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }

                            }

                        });
                    } catch (InterruptedException e) {
                        return;

                    }

                }

            }).start();

        }

        Thread.sleep(TEST_DURATION);

        wrapper.stop();

    }

}