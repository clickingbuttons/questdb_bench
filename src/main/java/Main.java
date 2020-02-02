import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static java.lang.Thread.*;

class WatchedSymbol {
    String symbol;
    long volume;
    double avgPrice;
    float money;
    public WatchedSymbol(String symbol, long volume, double avgPrice, float money) {
        this.symbol = symbol;
        this.volume = volume;
        this.avgPrice = avgPrice;
        this.money = money;
    }
}

class NumberedThread extends Thread {
    int num;
    public NumberedThread(Runnable r, int num) {
        super(r);
        this.num = num;
    }
    public int getNum() {
        return num;
    }
}

class MyThreadFactory implements ThreadFactory {
    static int num = 0;
    public Thread newThread(Runnable r) {
        return new NumberedThread(r, num++);
    }
}

class QuestDBTask implements Callable<Long> {
    File partition;
    List<String> dbPaths;
    String dbPath;
    String table;
    List<WatchedSymbol> watchList = new ArrayList<>();

    public QuestDBTask(File partition, List<String> dbPaths, String table) {
        this.partition = partition;
        this.dbPaths = dbPaths;
        this.table = table;
    }

    long readPartition(File partition) {
        this.dbPath = dbPaths.get(((NumberedThread)currentThread()).getNum());
        CairoConfiguration configuration = new DefaultCairoConfiguration(dbPath);
        CairoEngine engine = new CairoEngine(configuration);
        SqlCompiler compiler = new SqlCompiler(engine);
        String query = String.format(
                "(select sym, sum(size) as vol, avg(price) as avgPrice, sum(size*price) as money" +
                        " from %1$s" +
                        " where ts>'%2$sT00:00:00.000Z' and ts<'%2$sT09:30:00.000Z')" +
                        " where avgPrice < 49.2 order by money desc",
                table,
                partition.getName());
        try (RecordCursor cursor = compiler.compile(query).getRecordCursorFactory().getCursor()) {
            Record record = cursor.getRecord();

            while (cursor.hasNext()) {
                String sym = record.getSym(0).toString();
                int size = record.getInt(1);
                double avgPrice = record.getDouble(2);
                float money = record.getFloat(3);
                watchList.add(new WatchedSymbol(sym, size, avgPrice, money));
            }
            if (watchList.isEmpty()) {
                return watchList.size();
            }
            String query2 = String.format("select *" +
                            " from %1$s" +
                            " where ts>'%2$sT00:00:00.000Z' and ts<'%2$sT10:30:00.000Z'" +
                            " and (" + String.join(" or ", watchList.stream()
                                .map(watchedSymbol -> "sym='" + watchedSymbol.symbol + "'")
                                .collect(Collectors.toList())) +
                            ")" +
                            " order by sym",
                    table,
                    partition.getName());
            try (RecordCursor cursor2 = compiler.compile(query2).getRecordCursorFactory().getCursor()) {
                Record record2 = cursor2.getRecord();
                long d = 0;
                long count = 0;
                while (cursor2.hasNext()) {
                    d += record2.getSym(0).toString().hashCode();
                    d += record2.getFloat(1);
                    d += record2.getInt(2);
                    d += record2.getInt(3);
                    d += record2.getByte(4);
                    d += record2.getTimestamp(5);
                    count++;
                }
                return count;
            }
        } catch (SqlException e) {
            e.printStackTrace();
        }
        compiler.close();
        engine.close();

        return 0;
    }

    public Long call() {
        List<Long> rawTimes = new ArrayList();
        long startTime = System.currentTimeMillis();
        long tradeCount = readPartition(partition);
        rawTimes.add(System.currentTimeMillis() - startTime);
        System.out.printf("%s %s %dms %d symbols %d trades\n", dbPath, partition.getName(), rawTimes.get(rawTimes.size() - 1), watchList.size(), tradeCount);
        return tradeCount;
    }
}

class PopulateTestTableTask implements Runnable {
    String table;
    String dbPath;

    public PopulateTestTableTask(String table, String dbPath) {
        super();
        this.table = table;
        this.dbPath = dbPath;
    }

    public void run() {
        String rowCount = String.valueOf(20000000 * 5);
        CairoConfiguration configuration = new DefaultCairoConfiguration(dbPath);
        CairoEngine engine = new CairoEngine(configuration);
        SqlCompiler compiler = new SqlCompiler(engine);
        try {
            compiler.compile(String.format("select * from %s limit 1", table));
            System.out.printf("Table %s already exists, skipping\n", table);
        } catch (SqlException e) {
            if (e.getMessage().contains("does not exist")) {
                System.out.printf("Populating 1 week (5 partitions) in %s at %s\n", table, dbPath);
                String query = String.format(
                        "create table %s as " +
                                "(" +
                                "select" +
                                " rnd_symbol(6000,1,4,0) sym," +
                                " rnd_float(0)*100 price," +
                                " abs(rnd_int()) size," +
                                " abs(rnd_int()) conditions," +
                                " rnd_byte(2, 50) exchange," +
                                " timestamp_sequence(to_timestamp(1420171200011000), (1420596000000000 - 1420171200011000) / %s) ts" +
                                " from" +
                                " long_sequence(%s)" +
                                ") timestamp(ts) partition by DAY",
                        table, rowCount, rowCount);
                try {
                    compiler.compile(query);
                } catch (SqlException e2) {
                    e2.printStackTrace();
                    System.exit(1);
                }
            }
        }
    }
}

public class Main {
    static List<String> dbPaths = new ArrayList<>();
    static String table = "trades_test";

    public static void timeTasks(List<QuestDBTask> tasks) throws InterruptedException, ExecutionException {
        ExecutorService pool = Executors.newFixedThreadPool(dbPaths.size(), new MyThreadFactory());
        long startTime = System.currentTimeMillis();
        List<Future<Long>> results = pool.invokeAll(tasks);
        pool.shutdown();
        long sum = 0;
        for (Future<Long> result : results) {
            sum += result.get();
        }
        System.out.printf("\nCompleted ms: %d, total symbols %d\n", System.currentTimeMillis() - startTime, sum);
    }

    public static void testMultiThread(List<String> dbPaths) throws InterruptedException, ExecutionException {
        List<QuestDBTask> tasks = new ArrayList<>();

        File tradeDir = new File(dbPaths.get(0) + "/" + table);
        List<File> partitions = Arrays.asList(tradeDir.listFiles()).stream()
                .filter(file -> file.toString().contains("2015"))
                .sorted()
                .collect(Collectors.toList());

        for (File partition: partitions) {
            tasks.add(new QuestDBTask(partition, dbPaths, table));
        }

        timeTasks(tasks);
    }

    public static void main(String args[]) {
        dbPaths.add("./questdb-4.0.4/db");
//        dbPaths.add("./questdb-4.0.4/db2"); // Point to unique disks
//        dbPaths.add("./questdb-4.0.4/db3"); // Point to unique disks
        ExecutorService pool = Executors.newFixedThreadPool(dbPaths.size());
        List<Runnable> tasks = new ArrayList<>();
        for (String dbPath : dbPaths) {
            File directory = new File(dbPath);
            if (!directory.exists()) {
                directory.mkdir();
            }
            tasks.add(new PopulateTestTableTask(table, dbPath));
        }
        for (Runnable task : tasks) {
            pool.execute(task);
        }
        pool.shutdown();
        try {
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.printf("Testing %d %s tables\n", dbPaths.size(), table);
        try {
            testMultiThread(dbPaths);
        } catch (InterruptedException|ExecutionException e) {
            e.printStackTrace();
        }
        System.out.printf("Done with %d threads\n", dbPaths.size());

        System.exit(0);
    }
}
