import io.questdb.cairo.*;
import io.questdb.cairo.sql.Record;
import io.questdb.cairo.sql.RecordCursor;
import io.questdb.griffin.SqlCompiler;
import io.questdb.griffin.SqlException;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

abstract class ReporterThread extends Thread {
    private List<File> partitions;

    public ReporterThread(List<File> partitions) {
        this.partitions = partitions;
    }

    public void reportTimes(List<Long> times) {
        long sum = times.stream().mapToLong(Long::longValue).sum();
        System.out.printf("Partitions: %d\nTotal ms: %d\nAverage ms: %d\n", times.size(), sum, sum / times.size());
    }

    abstract void readPartition(File partition);

    public void run() {
        List<Long> rawTimes = new ArrayList();
        for (File partition : partitions) {
            long startTime = System.currentTimeMillis();
            readPartition(partition);
            rawTimes.add(System.currentTimeMillis() - startTime);
            System.out.printf("%s %d\n", partition, rawTimes.get(rawTimes.size() - 1));
        }
        reportTimes(rawTimes);
    }
}

class RawThread extends ReporterThread {
    public RawThread(List<File> partitions) {
        super(partitions);
    }

    void readPartition(File partition) {
        File[] dataFiles = partition.listFiles();
        for (int i = 0; i < dataFiles.length; i++) {
            try {
                FileChannel fileChannel = FileChannel.open(dataFiles[i].toPath(), StandardOpenOption.READ);
                MappedByteBuffer buffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, 0, fileChannel.size());
                long b = 0;
                for (int j = 0; j < buffer.limit(); j++) {
                    b += buffer.get();
                }
                fileChannel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}

class QuestDBThread extends ReporterThread {
    String dbPath;
    String table;
    public QuestDBThread(List<File> partitions, String dbPath, String table) {
        super(partitions);
        this.dbPath = dbPath;
        this.table = table;
    }

    void readPartition(File partition) {
        CairoConfiguration configuration = new DefaultCairoConfiguration(dbPath);
        CairoEngine engine = new CairoEngine(configuration);
        SqlCompiler compiler = new SqlCompiler(engine);
        try (RecordCursor cursor = compiler.compile(String.format("select * from %s where ts = '%s' order by sym",
                table,
                partition.getName())).getRecordCursorFactory().getCursor()) {
            Record record = cursor.getRecord();
            long b = 0;
            while (cursor.hasNext()) {
                b += record.getFloat(1);
                b += record.getInt(2);
                b += record.getInt(3);
                b += record.getByte(4);
                b += record.getTimestamp(5);
                b += record.getRowId();
                b += record.getSym(0).length();
            }
        } catch (SqlException e) {
            e.printStackTrace();
        }
        compiler.close();
        engine.close();
    }
}

public class Main {
    static List<String> dbPaths = new ArrayList<>();
    static String table = "trades";

    public static void timeThreads(List<Thread> threads) throws InterruptedException {
        long startTime = System.currentTimeMillis();
        for (Thread thread: threads) {
            thread.start();
        }
        for (Thread thread: threads) {
            thread.join();
        }
        System.out.printf("\nCompleted ms: %d\n", System.currentTimeMillis() - startTime);
    }

    public static void testMultiThread(List<String> dbPaths, int threadCount) throws InterruptedException {
        List<Thread> rawThreads = new ArrayList<>();
        List<Thread> questDBThreads = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            File tradeDir = new File(dbPaths.get(i) + "/" + table);
            List<File> partitions = Arrays.asList(tradeDir.listFiles()).stream()
                    .filter(file -> file.toString().contains("2015"))
                    .sorted()
                    .collect(Collectors.toList());

            int interval = partitions.size() / threadCount;
            List<File> shardedPartitions = partitions.subList(i * interval, (i + 1) * interval);

            rawThreads.add(new RawThread(shardedPartitions));
            questDBThreads.add(new QuestDBThread(shardedPartitions, dbPaths.get(i), table));
        }

        timeThreads(rawThreads);
        timeThreads(questDBThreads);
    }

    public static void main(String args[]) {
        for(int i = 0; i < args.length; i++) {
            dbPaths.add(args[i]);
        }

        try {
            testMultiThread(dbPaths, dbPaths.size());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.printf("Done with %d threads\n", dbPaths.size());

        System.exit(0);
    }
}
