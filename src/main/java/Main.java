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

class WatchedSymbol {
    String symbol;
    long volume;
    double avgPrice;
    double money;
    public WatchedSymbol(String symbol, long volume, double avgPrice, double money) {
        this.symbol = symbol;
        this.volume = volume;
        this.avgPrice = avgPrice;
        this.money = money;
    }
}

class QuestDBTask implements Callable<Long> {
    File partition;
    String dbPath;
    String table;
    List<WatchedSymbol> watchList = new ArrayList<>();

    public QuestDBTask(File partition, String dbPath, String table) {
        this.partition = partition;
        this.dbPath = dbPath;
        this.table = table;
    }

    long readPartition(File partition) {
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
                double money = record.getDouble(3);
                watchList.add(new WatchedSymbol(sym, size, avgPrice, money));
            }
            if (watchList.isEmpty()) {
                return watchList.size();
            }
            String query2 = String.format("select *" +
                            " from %1$s" +
                            " where ts>'%2$sT00:00:00.000Z' and ts<'%2$sT10:30:00.000Z'" +
                            " and sym in (" + String.join(", ", watchList.stream()
                                .map(watchedSymbol -> "'" + watchedSymbol.symbol + "'")
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
        try {

            System.out.printf("%s %dms %d symbols %d trades\n", partition.toPath().toRealPath(), rawTimes.get(rawTimes.size() - 1), watchList.size(), tradeCount);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return tradeCount;
    }
}

public class Main {
    static String dbPath = "/mnt/ssd1/db";
    static int threadCount = 8;
    static String table = "trades_test";

    static void timeTasks(List<QuestDBTask> tasks) throws InterruptedException, ExecutionException {
        ExecutorService pool = Executors.newFixedThreadPool(threadCount);
        long startTime = System.currentTimeMillis();
        List<Future<Long>> results = pool.invokeAll(tasks);
        pool.shutdown();
        long sum = 0;
        for (Future<Long> result : results) {
            sum += result.get();
        }
        System.out.printf("\nCompleted ms: %d, total trades %d\n", System.currentTimeMillis() - startTime, sum);
    }

    static void testMultiThread() throws InterruptedException, ExecutionException {
        List<QuestDBTask> tasks = new ArrayList<>();

        File tradeDir = new File(dbPath + "/" + table);
        List<File> partitions = Arrays.asList(tradeDir.listFiles()).stream()
                .filter(file -> file.toString().contains("2015"))
                .sorted()
                .collect(Collectors.toList());

        for (File partition: partitions) {
            tasks.add(new QuestDBTask(partition, dbPath, table));
        }

        timeTasks(tasks);
    }

    static void populateTestTable(String tableName) {
        String rowCount = String.valueOf(20000000 * 10);
        CairoConfiguration configuration = new DefaultCairoConfiguration(dbPath);
        CairoEngine engine = new CairoEngine(configuration);
        SqlCompiler compiler = new SqlCompiler(engine);
        try {
            compiler.compile(String.format("select * from %s limit 1", tableName));
            System.out.printf("Table %s already exists, skipping\n", tableName);
        } catch (SqlException e) {
            if (e.getMessage().contains("does not exist")) {
                System.out.printf("Populating 1 week (5 partitions) in %s at %s\n", tableName, dbPath);
                String createQuery = String.format(
                        "CREATE TABLE %s (" +
                                "    sym SYMBOL CACHE INDEX," +
                                "    price DOUBLE," +
                                "    size INT," +
                                "    conditions INT," +
                                "    exchange BYTE," +
                                "    ts TIMESTAMP" +
                                ") TIMESTAMP(ts) PARTITION BY DAY", tableName);
                // [2015-01-01, 2015-01-09]
                String populateQuery = String.format(
                        "INSERT INTO %s SELECT * FROM " +
                                "(" +
                                "SELECT" +
                                " rnd_symbol(6000,1,4,0) sym," +
                                " rnd_float(0)*100 price," +
                                " abs(rnd_int()) size," +
                                " abs(rnd_int()) conditions," +
                                " rnd_byte(2, 50) exchange," +
                                " timestamp_sequence(1420070400000000, (1420761600000000 - 1420070400000000) / %s) ts" +
                                " FROM" +
                                " long_sequence(%s)" +
                                ") TIMESTAMP(ts)",
                        tableName, rowCount, rowCount);
                try {
                    compiler.compile(createQuery);
                    compiler.compile(populateQuery);
                } catch (SqlException e2) {
                    e2.printStackTrace();
                    System.exit(1);
                }
            }
        }
    }

    public static void main(String args[]) {
        populateTestTable("trades_test");

        System.out.printf("Testing %d %s tables\n", threadCount, table);
        try {
            testMultiThread();
        } catch (InterruptedException|ExecutionException e) {
            e.printStackTrace();
        }
        System.out.printf("Done with %d threads\n", threadCount);

        System.exit(0);
    }
}
